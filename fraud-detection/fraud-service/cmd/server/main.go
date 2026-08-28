package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"

	"github.com/bank/fraud-detection/fraud-service/internal/config"
	fgrpc "github.com/bank/fraud-detection/fraud-service/internal/grpc"
	"github.com/bank/fraud-detection/fraud-service/internal/resilience"
	"github.com/bank/fraud-detection/fraud-service/internal/rules"
	"github.com/bank/fraud-detection/fraud-service/internal/scoring"
	fraudpb "github.com/bank/fraud-detection/fraud-service/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

func main() {
	// ── Load Configuration ────────────────────────────────────
	cfgPath := os.Getenv("CONFIG_PATH")
	cfg, err := config.Load(cfgPath)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to load configuration")
	}

	log.Info().
		Str("model_path", cfg.Model.Path).
		Str("model_version", cfg.Model.Version).
		Float64("approve_threshold", cfg.Scoring.ApproveThreshold).
		Float64("review_threshold", cfg.Scoring.ReviewThreshold).
		Msg("Configuration loaded")

	// ── Load Model + Scaler ────────────────────────────────────
	var model *scoring.Model
	if _, err := os.Stat(cfg.Model.Path); err == nil {
		model, err = scoring.LoadModel(cfg.Model.Path)
		if err != nil {
			log.Warn().Err(err).Msg("Failed to load model, using default")
			model = scoring.NewModel()
		}
	} else {
		log.Info().Msg("No model file found, using default model")
		model = scoring.NewModel()
	}
	model.Version = cfg.Model.Version

	// Load the StandardScaler from Python training pipeline.
	// This is critical: the model was trained on scaled features,
	// so we must apply the same normalization at serving time.
	if _, err := os.Stat(cfg.Model.ScalerPath); err == nil {
		scaler, err := scoring.LoadScaler(cfg.Model.ScalerPath)
		if err != nil {
			log.Warn().Err(err).Msg("Failed to load scaler — serving with raw features (training-serving skew risk!)")
		} else {
			model.SetScaler(scaler)
			log.Info().Str("path", cfg.Model.ScalerPath).Msg("Scaler loaded — features will be normalized before inference")
		}
	} else {
		log.Warn().Str("path", cfg.Model.ScalerPath).Msg("No scaler file found — serving with raw features (training-serving skew risk!)")
	}

	// ── Initialize Rules Engine ───────────────────────────────
	rulesEngine := rules.NewRulesEngine(rules.RulesConfig{
		Enabled:            cfg.Rules.Enabled,
		MaxAmountPerDay:    cfg.Rules.MaxAmountPerDay,
		MaxTxPerHour:       cfg.Rules.MaxTxPerHour,
		MaxCountriesPerDay: cfg.Rules.MaxCountriesPerDay,
		BlockedCountries:   cfg.Rules.BlockedCountries,
	})

	// ── Initialize Scorer ─────────────────────────────────────
	// Wrap rules engine to satisfy scoring.RulesEvaluator interface
	rulesAdapter := &rulesEngineAdapter{engine: rulesEngine}
	scorer := scoring.NewScorer(model, rulesAdapter)

	// ── Connect to Redis (with resilience) ─────────────────────
	rawRedisClient := redis.NewClient(&redis.Options{
		Addr:         cfg.Redis.Addr,
		Password:     cfg.Redis.Password,
		DB:           cfg.Redis.DB,
		PoolSize:     cfg.Redis.PoolSize,
		ReadTimeout:  cfg.Redis.ReadTimeout,
		WriteTimeout: cfg.Redis.WriteTimeout,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rawRedisClient.Ping(ctx).Err(); err != nil {
		log.Warn().Err(err).Msg("Failed to connect to Redis — running in degraded mode")
	} else {
		log.Info().Str("addr", cfg.Redis.Addr).Msg("Connected to Redis")
	}

	// Wrap Redis with circuit breaker + retry + fallback
	redisCB := resilience.NewCircuitBreaker(resilience.CircuitBreakerConfig{
		Name:             "redis",
		FailureThreshold: 5,    // 5 consecutive failures → OPEN
		SuccessThreshold: 3,    // 3 consecutive successes → CLOSED
		Timeout:          30 * time.Second, // wait 30s before probing
	})
	resilientRedis := resilience.NewResilientRedis(rawRedisClient, redisCB)

	// ── Start gRPC Server ────────────────────────────────────
	grpcAddr := fmt.Sprintf(":%d", cfg.Server.GRPCPort)
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to listen on gRPC port")
	}

	grpcServer := grpc.NewServer(
		grpc.MaxConcurrentStreams(1000),
		grpc.ChainUnaryInterceptor(
			loggingInterceptor,
			recoveryInterceptor,
		),
	)

	// Register services
	fraudServer := fgrpc.NewFraudServer(scorer, resilientRedis)
	fraudpb.RegisterFraudScoringServiceServer(grpcServer, fraudServer)

	// Health check
	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)
	healthServer.SetServingStatus("fraud.v1.FraudScoringService", grpc_health_v1.HealthCheckResponse_SERVING)

	// Reflection for debugging
	reflection.Register(grpcServer)

	// ── Start HTTP Metrics Server ─────────────────────────────
	metricsAddr := fmt.Sprintf(":%d", cfg.Metrics.Port)
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"status":"ok","model":"%s"}`, cfg.Model.Version)
	})

	metricsServer := &http.Server{
		Addr:    metricsAddr,
		Handler: mux,
	}

	// ── Start Servers ─────────────────────────────────────────
	go func() {
		log.Info().Str("addr", grpcAddr).Msg("gRPC server starting")
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatal().Err(err).Msg("gRPC server failed")
		}
	}()

	go func() {
		log.Info().Str("addr", metricsAddr).Msg("Metrics server starting")
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error().Err(err).Msg("Metrics server failed")
		}
	}()

	// ── Graceful Shutdown ─────────────────────────────────────
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Info().Msg("Shutting down gracefully...")

	healthServer.SetServingStatus("fraud.v1.FraudScoringService", grpc_health_v1.HealthCheckResponse_NOT_SERVING)

	grpcServer.GracefulStop()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	metricsServer.Shutdown(shutdownCtx)

	rawRedisClient.Close()

	log.Info().Msg("Fraud service stopped")
}

// loggingInterceptor logs all gRPC requests.
func loggingInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	start := time.Now()
	resp, err := handler(ctx, req)
	duration := time.Since(start)

	log.Info().
		Str("method", info.FullMethod).
		Dur("duration", duration).
		Err(err).
		Msg("gRPC request")

	return resp, err
}

// recoveryInterceptor catches panics in gRPC handlers.
func recoveryInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (resp interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Error().Interface("panic", r).Str("method", info.FullMethod).Msg("Panic recovered")
			err = fmt.Errorf("internal server error: %v", r)
		}
	}()
	return handler(ctx, req)
}

// rulesEngineAdapter wraps rules.RulesEngine to satisfy scoring.RulesEvaluator.
// Converts rules.Decision → scoring.Decision.
type rulesEngineAdapter struct {
	engine *rules.RulesEngine
}

func (a *rulesEngineAdapter) Evaluate(features map[string]string) (scoring.Decision, string) {
	decision, reason := a.engine.Evaluate(features)
	// rules.Decision and scoring.Decision are both int enums with the same values
	return scoring.Decision(decision), reason
}
