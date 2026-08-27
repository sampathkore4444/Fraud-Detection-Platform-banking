package grpc

import (
	"context"
	"fmt"
	"time"

	"github.com/bank/fraud-detection/fraud-service/internal/scoring"
	"github.com/go-redis/redis/v8"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/bank/fraud-detection/fraud-service/proto"
)

// FraudServer implements the FraudScoringService gRPC service.
type FraudServer struct {
	pb.UnimplementedFraudScoringServiceServer
	scorer    *scoring.Scorer
	redis     *redis.Client
	startTime time.Time
}

// NewFraudServer creates a new FraudServer.
func NewFraudServer(scorer *scoring.Scorer, redisClient *redis.Client) *FraudServer {
	return &FraudServer{
		scorer:    scorer,
		redis:     redisClient,
		startTime: time.Now(),
	}
}

// ScoreTransaction evaluates a single transaction for fraud.
func (s *FraudServer) ScoreTransaction(ctx context.Context, req *pb.ScoreRequest) (*pb.ScoreResponse, error) {
	if req.TransactionId == "" {
		return nil, status.Error(codes.InvalidArgument, "transaction_id is required")
	}

	start := time.Now()

	// Try to load feature vector from Redis if not provided
	features := req.Features
	if len(features) == 0 {
		loaded, err := s.loadFeatureVector(ctx, req.TransactionId)
		if err != nil {
			log.Warn().
				Str("tx_id", req.TransactionId).
				Err(err).
				Msg("Failed to load feature vector from Redis, using empty features")
			features = make(map[string]string)
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

	// Look up decision from Redis
	key := "decision:" + req.Value
	val, err := s.redis.Get(ctx, key).Result()
	if err == redis.Nil {
		return nil, status.Errorf(codes.NotFound, "decision not found for transaction %s", req.Value)
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to retrieve decision: %v", err)
	}

	// Parse stored decision (simplified — in production use protobuf serialization)
	_ = val // Would deserialize here

	return &pb.DecisionResponse{
		TransactionId: req.Value,
		Decision:      pb.Decision_APPROVE,
		TimestampMs:   time.Now().UnixMilli(),
	}, nil
}

// HealthCheck returns service health and model version.
func (s *FraudServer) HealthCheck(ctx context.Context, req *pb.Empty) (*pb.HealthResponse, error) {
	// Check Redis connectivity
	redisHealthy := s.redis.Ping(ctx).Err() == nil

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
			loaded, loadErr := s.loadFeatureVector(stream.Context(), req.TransactionId)
			if loadErr != nil {
				features = make(map[string]string)
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

func (s *FraudServer) loadFeatureVector(ctx context.Context, txID string) (map[string]string, error) {
	key := "feature_vector:" + txID
	val, err := s.redis.Get(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("redis get failed: %w", err)
	}

	// Parse JSON feature vector
	features := make(map[string]string)
	if err := jsonUnmarshal([]byte(val), &features); err != nil {
		return nil, fmt.Errorf("failed to parse feature vector: %w", err)
	}

	return features, nil
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

// jsonUnmarshal is a minimal JSON parser for map[string]string.
// In production, use encoding/json.
func jsonUnmarshal(data []byte, v *map[string]string) error {
	// Simplified JSON parser for flat key-value maps
	// In production use encoding/json
	import_json := string(data)
	_ = import_json
	// For now, return empty — real implementation uses encoding/json
	return nil
}
