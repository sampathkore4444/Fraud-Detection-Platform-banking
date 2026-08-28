package grpc

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/bank/fraud-detection/fraud-service/internal/resilience"
	"github.com/bank/fraud-detection/fraud-service/internal/scoring"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/bank/fraud-detection/fraud-service/proto"
)

// FraudServer implements the FraudScoringService gRPC service.
type FraudServer struct {
	pb.UnimplementedFraudScoringServiceServer
	scorer    *scoring.Scorer
	redis     ResilientRedisClient // ResilientRedis wrapper
	startTime time.Time
}

// ResilientRedisClient is the interface for Redis operations with resilience.
type ResilientRedisClient interface {
	GetFeatureVector(ctx context.Context, key string) (map[string]string, error)
	Ping(ctx context.Context) error
}

// NewFraudServer creates a new FraudServer with resilience.
func NewFraudServer(scorer *scoring.Scorer, redisClient ResilientRedisClient) *FraudServer {
	return &FraudServer{
		scorer:    scorer,
		redis:     redisClient,
		startTime: time.Now(),
	}
}

// ScoreTransaction evaluates a single transaction for fraud.
//
// Resilience flow:
//  1. Validate input
//  2. Load feature vector from Redis (with circuit breaker + retry + fallback)
//  3. Score via XGBoost model
//  4. Apply rules engine override
//  5. Return decision
func (s *FraudServer) ScoreTransaction(ctx context.Context, req *pb.ScoreRequest) (*pb.ScoreResponse, error) {
	if req.TransactionId == "" {
		return nil, status.Error(codes.InvalidArgument, "transaction_id is required")
	}

	start := time.Now()

	// Try to load feature vector from Redis (with resilience)
	features := req.Features
	if len(features) == 0 {
		loaded, err := s.redis.GetFeatureVector(ctx, "feature_vector:"+req.TransactionId)
		if err != nil {
			// This shouldn't happen now (fallback returns defaults), but handle anyway
			log.Warn().
				Str("tx_id", req.TransactionId).
				Err(err).
				Msg("Failed to load feature vector, using defaults")
			features = resilience.DefaultFeatureVector()
		} else {
			features = loaded
		}
	}

	// Determine model version
	modelVersion := req.ModelVersion
	if modelVersion == "" {
		modelVersion = s.scorer.ModelVersion()
	}

	// Score the transaction
	result := s.scorer.Score(req.TransactionId, features, modelVersion, req.TimestampMs)

	latencyMs := time.Since(start).Milliseconds()

	return &pb.ScoreResponse{
		TransactionId:    result.TransactionID,
		Decision:         decisionToProto(result.Decision),
		FraudProbability: result.FraudProbability,
		ModelVersion:     result.ModelVersion,
		LatencyMs:        latencyMs,
		TopFeatures:      result.TopFeatures,
		ReasonCode:       result.ReasonCode,
	}, nil
}

// GetDecision retrieves a previously computed decision.
func (s *FraudServer) GetDecision(ctx context.Context, req *pb.StringRequest) (*pb.DecisionResponse, error) {
	if req.Value == "" {
		return nil, status.Error(codes.InvalidArgument, "transaction_id is required")
	}

	// Look up decision from Redis (with resilience)
	key := "decision:" + req.Value
	decisionMap, err := s.redis.GetFeatureVector(ctx, key)
	if err != nil || len(decisionMap) == 0 {
		return nil, status.Errorf(codes.NotFound, "decision not found for transaction %s", req.Value)
	}

	// Parse decision from the map
	decisionStr := decisionMap["decision"]
	var decision pb.Decision
	switch decisionStr {
	case "APPROVE":
		decision = pb.Decision_APPROVE
	case "REVIEW":
		decision = pb.Decision_REVIEW
	case "DECLINE":
		decision = pb.Decision_DECLINE
	default:
		decision = pb.Decision_APPROVE
	}

	return &pb.DecisionResponse{
		TransactionId: req.Value,
		Decision:      decision,
		TimestampMs:   time.Now().UnixMilli(),
	}, nil
}

// HealthCheck returns service health and model version.
func (s *FraudServer) HealthCheck(ctx context.Context, req *pb.Empty) (*pb.HealthResponse, error) {
	// Check Redis connectivity (with short timeout)
	redisHealthy := s.redis.Ping(ctx) == nil

	return &pb.HealthResponse{
		Healthy:         redisHealthy,
		ModelVersion:    s.scorer.ModelVersion(),
		ModelLoadedAtMs: s.scorer.ModelLoadedAt().UnixMilli(),
		UptimeSeconds:   int64(time.Since(s.startTime).Seconds()),
	}, nil
}

// ScoreBatch scores a stream of transactions (SPEC §3.4 batch scoring).
func (s *FraudServer) ScoreBatch(stream pb.FraudScoringService_ScoreBatchServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}

		features := req.Features
		if len(features) == 0 {
			loaded, loadErr := s.redis.GetFeatureVector(stream.Context(), "feature_vector:"+req.TransactionId)
			if loadErr != nil {
				features = resilience.DefaultFeatureVector()
			} else {
				features = loaded
			}
		}

		modelVersion := req.ModelVersion
		if modelVersion == "" {
			modelVersion = s.scorer.ModelVersion()
		}

		result := s.scorer.Score(req.TransactionId, features, modelVersion, req.TimestampMs)

		resp := &pb.ScoreResponse{
			TransactionId:    result.TransactionID,
			Decision:         decisionToProto(result.Decision),
			FraudProbability: result.FraudProbability,
			ModelVersion:     result.ModelVersion,
			LatencyMs:        result.LatencyMs,
			TopFeatures:      result.TopFeatures,
			ReasonCode:       result.ReasonCode,
		}

		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}

func decisionToProto(d scoring.Decision) pb.Decision {
	switch d {
	case scoring.DecisionApprove:
		return pb.Decision_APPROVE
	case scoring.DecisionReview:
		return pb.Decision_REVIEW
	case scoring.DecisionDecline:
		return pb.Decision_DECLINE
	default:
		return pb.Decision_APPROVE
	}
}

func jsonUnmarshal(data []byte, v *map[string]string) error {
	return json.Unmarshal(data, v)
}

// Ensure we use fmt to avoid import cycle issues.
var _ = fmt.Sprintf
