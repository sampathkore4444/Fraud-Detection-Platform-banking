package resilience

import (
	"context"
	"math"
	"math/rand"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog/log"
)

// ─── Prometheus Metrics ──────────────────────────────────────────────────────

var (
	retryAttempts = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "fraud_service",
			Subsystem: "retry",
			Name:      "attempts_total",
			Help:      "Total number of retry attempts",
		},
		[]string{"operation", "attempt"},
	)

	retrySuccesses = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "fraud_service",
			Subsystem: "retry",
			Name:      "successes_total",
			Help:      "Total successful calls (including first attempt)",
		},
		[]string{"operation"},
	)

	retryExhausted = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "fraud_service",
			Subsystem: "retry",
			Name:      "exhausted_total",
			Help:      "Total calls that failed after all retries exhausted",
		},
		[]string{"operation"},
	)
)

// ─── Retry Configuration ─────────────────────────────────────────────────────

// RetryConfig holds configuration for retry with exponential backoff.
type RetryConfig struct {
	MaxAttempts int           // max attempts (including first try). Default: 3
	BaseDelay   time.Duration // initial delay. Default: 10ms
	MaxDelay    time.Duration // cap on delay. Default: 1s
	Multiplier  float64       // backoff multiplier. Default: 2.0
	Jitter      float64       // jitter factor [0, 1]. Default: 0.1
}

// DefaultRedisRetry returns retry config tuned for Redis operations.
// Redis is fast — we retry aggressively with short delays.
func DefaultRedisRetry() RetryConfig {
	return RetryConfig{
		MaxAttempts: 3,
		BaseDelay:   5 * time.Millisecond,
		MaxDelay:    100 * time.Millisecond,
		Multiplier:  2.0,
		Jitter:      0.2,
	}
}

// DefaultGRPCRetry returns retry config tuned for gRPC calls.
// gRPC calls are slower — we retry less aggressively.
func DefaultGRPCRetry() RetryConfig {
	return RetryConfig{
		MaxAttempts: 2,
		BaseDelay:   20 * time.Millisecond,
		MaxDelay:    200 * time.Millisecond,
		Multiplier:  2.0,
		Jitter:      0.1,
	}
}

// ─── Retry Executor ──────────────────────────────────────────────────────────

// RetryableFunc is a function that can be retried.
// Returns (result, error). If error is non-nil, retry may be attempted.
type RetryableFunc func(ctx context.Context) error

// Retry executes a function with retry and exponential backoff.
//
// Flow:
//
//	attempt 1: execute → if success, record and return
//	attempt 2: wait baseDelay * multiplier + jitter → execute → if success, record
//	attempt 3: wait baseDelay * multiplier² + jitter → execute → if success, record
//	...all failed → record exhaustion, return last error
//
// The jitter prevents thundering herd: if Redis recovers, all 10K in-flight
// requests don't retry at the exact same millisecond.
func Retry(ctx context.Context, operation string, cfg RetryConfig, fn RetryableFunc) error {
	var lastErr error

	for attempt := 1; attempt <= cfg.MaxAttempts; attempt++ {
		// Check context before each attempt
		if ctx.Err() != nil {
			retryExhausted.WithLabelValues(operation).Inc()
			return ctx.Err()
		}

		// Execute the function
		lastErr = fn(ctx)
		if lastErr == nil {
			// Success
			retrySuccesses.WithLabelValues(operation).Inc()
			if attempt > 1 {
				log.Info().
					Str("operation", operation).
					Int("attempt", attempt).
					Msg("Succeeded after retry")
			}
			return nil
		}

		retryAttempts.WithLabelValues(operation, string(rune('0'+attempt))).Inc()

		// Don't retry on context cancellation or deadline exceeded
		if ctx.Err() != nil {
			break
		}

		// Don't sleep after the last attempt
		if attempt < cfg.MaxAttempts {
			delay := computeBackoff(cfg, attempt)
			log.Debug().
				Str("operation", operation).
				Int("attempt", attempt).
				Dur("delay", delay).
				Err(lastErr).
				Msg("Retrying after backoff")

			select {
			case <-time.After(delay):
				// Continue to next attempt
			case <-ctx.Done():
				break
			}
		}
	}

	retryExhausted.WithLabelValues(operation).Inc()
	log.Warn().
		Str("operation", operation).
		Int("attempts", cfg.MaxAttempts).
		Err(lastErr).
		Msg("All retry attempts exhausted")
	return lastErr
}

// computeBackoff calculates the delay for a given attempt using exponential
// backoff with jitter.
//
//	delay = min(baseDelay * multiplier^(attempt-1), maxDelay) + jitter
//
// Jitter is uniform random in [0, jitter * delay] to prevent synchronized retries.
func computeBackoff(cfg RetryConfig, attempt int) time.Duration {
	// Exponential component
	expDelay := float64(cfg.BaseDelay) * math.Pow(cfg.Multiplier, float64(attempt-1))

	// Cap at maxDelay
	if expDelay > float64(cfg.MaxDelay) {
		expDelay = float64(cfg.MaxDelay)
	}

	// Add jitter
	jitter := rand.Float64() * cfg.Jitter * expDelay
	delay := time.Duration(expDelay + jitter)

	return delay
}
