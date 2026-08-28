package resilience

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// ─── Circuit Breaker Tests ───────────────────────────────────────────────────

func TestCircuitBreaker_NormalOperation(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		Name:             "test",
		FailureThreshold: 3,
		SuccessThreshold: 2,
		Timeout:          1 * time.Second,
	})

	// Should start CLOSED
	if cb.GetState() != StateClosed {
		t.Fatal("Expected CLOSED state")
	}

	// Allow should return true in CLOSED state
	if !cb.Allow() {
		t.Fatal("Expected Allow() = true in CLOSED state")
	}

	// Record successes — should stay CLOSED
	cb.RecordSuccess()
	cb.RecordSuccess()
	if cb.GetState() != StateClosed {
		t.Fatal("Expected CLOSED state after successes")
	}
}

func TestCircuitBreaker_TripsOnFailures(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		Name:             "test",
		FailureThreshold: 3,
		SuccessThreshold: 2,
		Timeout:          1 * time.Second,
	})

	// Record 3 failures — should trip to OPEN
	cb.RecordFailure()
	cb.RecordFailure()
	cb.RecordFailure()

	if cb.GetState() != StateOpen {
		t.Fatalf("Expected OPEN state, got %v", cb.GetState())
	}

	// Allow should return false in OPEN state
	if cb.Allow() {
		t.Fatal("Expected Allow() = false in OPEN state")
	}
}

func TestCircuitBreaker_RecoveryViaHalfOpen(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		Name:             "test",
		FailureThreshold: 2,
		SuccessThreshold: 2,
		Timeout:          100 * time.Millisecond, // Short timeout for testing
	})

	// Trip the breaker
	cb.RecordFailure()
	cb.RecordFailure()
	if cb.GetState() != StateOpen {
		t.Fatal("Expected OPEN state")
	}

	// Wait for timeout
	time.Sleep(150 * time.Millisecond)

	// Allow should return true (HALF_OPEN)
	if !cb.Allow() {
		t.Fatal("Expected Allow() = true in HALF_OPEN state")
	}
	if cb.GetState() != StateHalfOpen {
		t.Fatalf("Expected HALF_OPEN state, got %v", cb.GetState())
	}

	// Record successes — should recover to CLOSED
	cb.RecordSuccess()
	if cb.GetState() != StateHalfOpen {
		t.Fatal("Expected HALF_OPEN state (need more successes)")
	}
	cb.RecordSuccess()
	if cb.GetState() != StateClosed {
		t.Fatalf("Expected CLOSED state after recovery, got %v", cb.GetState())
	}
}

func TestCircuitBreaker_ProbeFailureReturnsToOpen(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		Name:             "test",
		FailureThreshold: 2,
		SuccessThreshold: 2,
		Timeout:          100 * time.Millisecond,
	})

	// Trip the breaker
	cb.RecordFailure()
	cb.RecordFailure()

	// Wait for timeout → HALF_OPEN
	time.Sleep(150 * time.Millisecond)
	cb.Allow() // Transition to HALF_OPEN

	// Probe fails → back to OPEN
	cb.RecordFailure()
	if cb.GetState() != StateOpen {
		t.Fatalf("Expected OPEN state after failed probe, got %v", cb.GetState())
	}
}

func TestCircuitBreaker_FailureResetsOnSuccess(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		Name:             "test",
		FailureThreshold: 3,
		SuccessThreshold: 2,
		Timeout:          1 * time.Second,
	})

	// 2 failures (below threshold)
	cb.RecordFailure()
	cb.RecordFailure()

	// Success resets failure count
	cb.RecordSuccess()

	// 2 more failures — should NOT trip (counter was reset)
	cb.RecordFailure()
	cb.RecordFailure()

	if cb.GetState() != StateClosed {
		t.Fatal("Expected CLOSED state — success should have reset failure count")
	}
}

// ─── Retry Tests ─────────────────────────────────────────────────────────────

func TestRetry_SucceedsImmediately(t *testing.T) {
	cfg := RetryConfig{
		MaxAttempts: 3,
		BaseDelay:   10 * time.Millisecond,
		MaxDelay:    100 * time.Millisecond,
		Multiplier:  2.0,
		Jitter:      0,
	}

	var attempts int32
	err := Retry(context.Background(), "test", cfg, func(ctx context.Context) error {
		atomic.AddInt32(&attempts, 1)
		return nil // Succeed immediately
	})

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if atomic.LoadInt32(&attempts) != 1 {
		t.Fatalf("Expected 1 attempt, got %d", attempts)
	}
}

func TestRetry_SucceedsAfterRetries(t *testing.T) {
	cfg := RetryConfig{
		MaxAttempts: 3,
		BaseDelay:   10 * time.Millisecond,
		MaxDelay:    100 * time.Millisecond,
		Multiplier:  2.0,
		Jitter:      0,
	}

	var attempts int32
	err := Retry(context.Background(), "test", cfg, func(ctx context.Context) error {
		count := atomic.AddInt32(&attempts, 1)
		if count < 3 {
			return errors.New("not yet")
		}
		return nil // Succeed on 3rd attempt
	})

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if atomic.LoadInt32(&attempts) != 3 {
		t.Fatalf("Expected 3 attempts, got %d", attempts)
	}
}

func TestRetry_ExhaustsAllAttempts(t *testing.T) {
	cfg := RetryConfig{
		MaxAttempts: 3,
		BaseDelay:   10 * time.Millisecond,
		MaxDelay:    100 * time.Millisecond,
		Multiplier:  2.0,
		Jitter:      0,
	}

	var attempts int32
	err := Retry(context.Background(), "test", cfg, func(ctx context.Context) error {
		atomic.AddInt32(&attempts, 1)
		return errors.New("always fail")
	})

	if err == nil {
		t.Fatal("Expected error after exhaustion")
	}
	if atomic.LoadInt32(&attempts) != 3 {
		t.Fatalf("Expected 3 attempts, got %d", attempts)
	}
}

func TestRetry_RespectsContextCancellation(t *testing.T) {
	cfg := RetryConfig{
		MaxAttempts: 5,
		BaseDelay:   50 * time.Millisecond,
		MaxDelay:    100 * time.Millisecond,
		Multiplier:  2.0,
		Jitter:      0,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	var attempts int32
	err := Retry(ctx, "test", cfg, func(ctx context.Context) error {
		atomic.AddInt32(&attempts, 1)
		return errors.New("fail")
	})

	if err == nil {
		t.Fatal("Expected error from context cancellation")
	}
	// Should have fewer than 5 attempts due to context timeout
	if atomic.LoadInt32(&attempts) >= 5 {
		t.Fatal("Expected fewer attempts due to context cancellation")
	}
}

func TestComputeBackoff_Exponential(t *testing.T) {
	cfg := RetryConfig{
		BaseDelay:  10 * time.Millisecond,
		MaxDelay:   1 * time.Second,
		Multiplier: 2.0,
		Jitter:     0, // No jitter for deterministic test
	}

	// Attempt 1: 10ms * 2^0 = 10ms
	d1 := computeBackoff(cfg, 1)
	if d1 != 10*time.Millisecond {
		t.Fatalf("Expected 10ms, got %v", d1)
	}

	// Attempt 2: 10ms * 2^1 = 20ms
	d2 := computeBackoff(cfg, 2)
	if d2 != 20*time.Millisecond {
		t.Fatalf("Expected 20ms, got %v", d2)
	}

	// Attempt 3: 10ms * 2^2 = 40ms
	d3 := computeBackoff(cfg, 3)
	if d3 != 40*time.Millisecond {
		t.Fatalf("Expected 40ms, got %v", d3)
	}
}

func TestComputeBackoff_CapsAtMaxDelay(t *testing.T) {
	cfg := RetryConfig{
		BaseDelay:  100 * time.Millisecond,
		MaxDelay:   200 * time.Millisecond,
		Multiplier: 2.0,
		Jitter:     0,
	}

	// Attempt 5: 100ms * 2^4 = 1600ms, but capped at 200ms
	d5 := computeBackoff(cfg, 5)
	if d5 != 200*time.Millisecond {
		t.Fatalf("Expected 200ms (capped), got %v", d5)
	}
}

// ─── Fallback Tests ──────────────────────────────────────────────────────────

func TestDefaultFeatureVector_Completeness(t *testing.T) {
	fv := DefaultFeatureVector()

	// Should have all 30 features
	expectedFeatures := []string{
		"velocity_tx_count_1h",
		"velocity_tx_count_24h",
		"velocity_amount_sum_1h",
		"velocity_amount_sum_24h",
		"velocity_decline_count_24h",
		"velocity_unique_countries_1h",
		"velocity_unique_merchants_24h",
		"velocity_avg_amount_7d",
		"velocity_stddev_amount_7d",
		"velocity_time_since_last_tx",
		"behavioral_typical_amount_ratio",
		"behavioral_typical_hour_score",
		"behavioral_typical_day_score",
		"behavioral_merchant_category_diversity",
		"behavioral_amount_zscore",
		"behavioral_is_recipient_new",
		"behavioral_velocity_direction",
		"behavioral_time_between_tx_stddev",
		"behavioral_country_change_freq",
		"behavioral_night_tx_ratio",
		"device_is_known",
		"device_last_seen_hours_ago",
		"device_unique_accounts_24h",
		"device_is_emulator_detected",
		"device_rooted_jailbroken",
		"device_ip_country_match",
		"device_ip_is_vpn",
		"device_browser_fingerprint_match",
		"device_latency_anomaly",
		"device_is_new_os_version",
	}

	if len(fv) != len(expectedFeatures) {
		t.Fatalf("Expected %d features, got %d", len(expectedFeatures), len(fv))
	}

	for _, feat := range expectedFeatures {
		if _, ok := fv[feat]; !ok {
			t.Errorf("Missing feature: %s", feat)
		}
	}
}

func TestDefaultFeatureVector_UnknownCustomerProfile(t *testing.T) {
	fv := DefaultFeatureVector()

	// Unknown device (highest risk)
	if fv["device_is_known"] != "0" {
		t.Error("Expected device_is_known=0 for unknown customer")
	}

	// First transaction
	if fv["velocity_tx_count_1h"] != "1" {
		t.Error("Expected velocity_tx_count_1h=1 for first transaction")
	}

	// No behavioral baseline
	if fv["behavioral_amount_zscore"] != "0.0" {
		t.Error("Expected behavioral_amount_zscore=0.0 for no baseline")
	}
}
