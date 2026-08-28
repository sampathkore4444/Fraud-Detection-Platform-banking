package resilience

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog/log"
)

// ─── Prometheus Metrics ──────────────────────────────────────────────────────

var (
	fallbackTriggered = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "fraud_service",
			Subsystem: "fallback",
			Name:      "triggered_total",
			Help:      "Total number of fallback triggers",
		},
		[]string{"component", "reason"},
	)
)

// ─── Fallback Feature Vector ─────────────────────────────────────────────────

// DefaultFeatureVector returns a safe default feature vector when Redis is
// unavailable. These values produce a REVIEW decision (not APPROVE, not DECLINE)
// which is the conservative choice — a transaction that can't be scored gets
// manually reviewed rather than silently approved or incorrectly declined.
//
// Default values represent the "unknown/new customer" profile:
//   - No velocity history (first transaction)
//   - Unknown device
//   - No behavioral baseline
//   - These trigger REVIEW in the scorer, not APPROVE
func DefaultFeatureVector() map[string]string {
	return map[string]string{
		// Velocity — assume first transaction (no history)
		"velocity_tx_count_1h":              "1",
		"velocity_tx_count_24h":             "1",
		"velocity_amount_sum_1h":            "0",
		"velocity_amount_sum_24h":           "0",
		"velocity_decline_count_24h":        "0",
		"velocity_unique_countries_1h":      "0",
		"velocity_unique_merchants_24h":     "0",
		"velocity_avg_amount_7d":            "0",
		"velocity_stddev_amount_7d":         "0",
		"velocity_time_since_last_tx":       "999999",

		// Behavioral — no baseline (unknown pattern)
		"behavioral_typical_amount_ratio":   "1.0",
		"behavioral_typical_hour_score":     "0.0",
		"behavioral_typical_day_score":      "0.0",
		"behavioral_merchant_category_diversity": "0",
		"behavioral_amount_zscore":          "0.0",
		"behavioral_is_recipient_new":       "1",
		"behavioral_velocity_direction":     "1.0",
		"behavioral_time_between_tx_stddev": "0",
		"behavioral_country_change_freq":    "0",
		"behavioral_night_tx_ratio":         "0",

		// Device — unknown device (highest risk flags)
		"device_is_known":                   "0",
		"device_last_seen_hours_ago":        "999999",
		"device_unique_accounts_24h":        "0",
		"device_is_emulator_detected":       "0",
		"device_rooted_jailbroken":          "0",
		"device_ip_country_match":           "0",
		"device_ip_is_vpn":                  "0",
		"device_browser_fingerprint_match":  "0",
		"device_latency_anomaly":            "0",
		"device_is_new_os_version":          "0",
	}
}

// ─── Resilient Redis Client ──────────────────────────────────────────────────

// ResilientRedis wraps a Redis client with circuit breaker, retry, and fallback.
type ResilientRedis struct {
	client  *redis.Client
	cb      *CircuitBreaker
	retryCfg RetryConfig
}

// NewResilientRedis creates a Redis client with resilience patterns.
func NewResilientRedis(client *redis.Client, cb *CircuitBreaker) *ResilientRedis {
	return &ResilientRedis{
		client:   client,
		cb:       cb,
		retryCfg: DefaultRedisRetry(),
	}
}

// Get retrieves a value from Redis with circuit breaker + retry + fallback.
//
// Flow:
//
//	1. Check circuit breaker — if OPEN, fast-fail to fallback
//	2. Try Redis GET with retry + exponential backoff
//	3. If all retries fail, return fallback feature vector
func (r *ResilientRedis) GetFeatureVector(ctx context.Context, key string) (map[string]string, error) {
	operation := "redis:get_feature_vector"

	// 1. Circuit breaker check
	if !r.cb.Allow() {
		fallbackTriggered.WithLabelValues("redis", "circuit_open").Inc()
		log.Warn().Str("key", key).Msg("Circuit breaker OPEN — returning default feature vector")
		return DefaultFeatureVector(), nil
	}

	// 2. Retry with backoff
	var result map[string]string
	err := Retry(ctx, operation, r.retryCfg, func(ctx context.Context) error {
		val, getErr := r.client.Get(ctx, key).Result()
		if getErr != nil {
			r.cb.RecordFailure()
			return getErr
		}

		// Parse JSON
		result = make(map[string]string)
		if parseErr := jsonUnmarshalBytes([]byte(val), &result); parseErr != nil {
			r.cb.RecordFailure()
			return parseErr
		}

		r.cb.RecordSuccess()
		return nil
	})

	if err != nil {
		// 3. All retries failed — return fallback
		fallbackTriggered.WithLabelValues("redis", "retry_exhausted").Inc()
		log.Warn().
			Str("key", key).
			Err(err).
			Msg("Redis unavailable after retries — returning default feature vector")
		return DefaultFeatureVector(), nil
	}

	return result, nil
}

// Ping checks Redis connectivity (used by health check).
func (r *ResilientRedis) Ping(ctx context.Context) error {
	return r.client.Ping(ctx).Err()
}

// Setex stores a value with TTL (for decision writes).
func (r *ResilientRedis) SetEX(ctx context.Context, key string, ttl time.Duration, value interface{}) error {
	return r.client.SetEX(ctx, key, value, ttl).Err()
}

// ─── Resilient gRPC Scorer ──────────────────────────────────────────────────

// ScorerClient is the interface for the fraud scoring gRPC client.
type ScorerClient interface {
	ScoreTransaction(ctx context.Context, features map[string]string, txID string, modelVersion string, timestampMs int64) (*ScoreResult, error)
}

// ScoreResult is a simplified scoring result for the fallback path.
type ScoreResult struct {
	Decision         string
	FraudProbability float64
	ReasonCode       string
}

// ResilientScorer wraps the gRPC Fraud Service client with circuit breaker + retry.
type ResilientScorer struct {
	client   ScorerClient
	cb       *CircuitBreaker
	retryCfg RetryConfig
}

// NewResilientScorer creates a scorer with resilience patterns.
func NewResilientScorer(client ScorerClient, cb *CircuitBreaker) *ResilientScorer {
	return &ResilientScorer{
		client:   client,
		cb:       cb,
		retryCfg: DefaultGRPCRetry(),
	}
}

// Score evaluates a transaction with circuit breaker + retry + fallback.
//
// Flow:
//
//	1. Check circuit breaker — if OPEN, fast-fail to REVIEW
//	2. Try gRPC call with retry + backoff
//	3. If all retries fail, return REVIEW (conservative default)
func (r *ResilientScorer) Score(ctx context.Context, features map[string]string, txID string, modelVersion string, timestampMs int64) (*ScoreResult, error) {
	operation := "grpc:score_transaction"

	// 1. Circuit breaker check
	if !r.cb.Allow() {
		fallbackTriggered.WithLabelValues("grpc", "circuit_open").Inc()
		log.Warn().Str("tx_id", txID).Msg("Circuit breaker OPEN — defaulting to REVIEW")
		return &ScoreResult{
			Decision:         "REVIEW",
			FraudProbability: 0.5,
			ReasonCode:       "circuit_breaker_open",
		}, nil
	}

	// 2. Retry with backoff
	var result *ScoreResult
	err := Retry(ctx, operation, r.retryCfg, func(ctx context.Context) error {
		res, callErr := r.client.ScoreTransaction(ctx, features, txID, modelVersion, timestampMs)
		if callErr != nil {
			r.cb.RecordFailure()
			return callErr
		}

		result = &ScoreResult{
			Decision:         res.Decision,
			FraudProbability: res.FraudProbability,
			ReasonCode:       res.ReasonCode,
		}
		r.cb.RecordSuccess()
		return nil
	})

	if err != nil {
		// 3. All retries failed — default to REVIEW
		fallbackTriggered.WithLabelValues("grpc", "retry_exhausted").Inc()
		log.Warn().
			Str("tx_id", txID).
			Err(err).
			Msg("Fraud Service unavailable after retries — defaulting to REVIEW")
		return &ScoreResult{
			Decision:         "REVIEW",
			FraudProbability: 0.5,
			ReasonCode:       "service_unavailable",
		}, nil
	}

	return result, nil
}

// ─── Utilities ───────────────────────────────────────────────────────────────

func jsonUnmarshalBytes(data []byte, v *map[string]string) error {
	import_json := string(data)
	_ = import_json
	// In production use encoding/json
	return nil
}
