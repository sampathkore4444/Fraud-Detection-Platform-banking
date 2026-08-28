package resilience

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog/log"
)

// ─── Circuit Breaker States ──────────────────────────────────────────────────

type State int

const (
	StateClosed   State = iota // Normal — requests flow through
	StateOpen                  // Tripped — requests blocked, fast-fail
	StateHalfOpen              // Testing — one probe request allowed
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "CLOSED"
	case StateOpen:
		return "OPEN"
	case StateHalfOpen:
		return "HALF_OPEN"
	default:
		return "UNKNOWN"
	}
}

// ─── Prometheus Metrics ──────────────────────────────────────────────────────

var (
	circuitBreakerState = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "fraud_service",
			Subsystem: "circuit_breaker",
			Name:      "state",
			Help:      "Current circuit breaker state (0=closed, 1=open, 2=half_open)",
		},
		[]string{"name"},
	)

	circuitBreakerTripped = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "fraud_service",
			Subsystem: "circuit_breaker",
			Name:      "tripped_total",
			Help:      "Total number of times the circuit breaker tripped to OPEN",
		},
		[]string{"name"},
	)

	circuitBreakerRecovered = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "fraud_service",
			Subsystem: "circuit_breaker",
			Name:      "recovered_total",
			Help:      "Total number of times the circuit breaker recovered to CLOSED",
		},
		[]string{"name"},
	)

	circuitBreakerRequests = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "fraud_service",
			Subsystem: "circuit_breaker",
			Name:      "requests_total",
			Help:      "Total requests through the circuit breaker",
		},
		[]string{"name", "result"},
	)
)

// ─── Circuit Breaker ─────────────────────────────────────────────────────────

// CircuitBreaker implements the circuit breaker pattern.
//
// States:
//   - CLOSED:   Normal operation. Requests flow through. Failures are counted.
//   - OPEN:     Tripped after consecutive failures. All requests fast-fail.
//   - HALF_OPEN: After timeout, one probe request is allowed through.
//     If it succeeds → CLOSED. If it fails → OPEN again.
//
// This prevents cascading failures: if Redis is down, we don't hammer it
// with 10K TPS of requests — we fast-fail to REVIEW after the first few
// failures, then periodically probe to see if it recovered.
type CircuitBreaker struct {
	name            string
	state           State
	failureCount    int
	successCount    int
	failureThreshold int           // failures before tripping
	successThreshold int           // successes in HALF_OPEN before recovery
	timeout         time.Duration  // how long to stay OPEN before HALF_OPEN
	lastFailureTime time.Time
	mu              sync.RWMutex
}

// CircuitBreakerConfig holds configuration for a circuit breaker.
type CircuitBreakerConfig struct {
	Name             string
	FailureThreshold int           // default: 5
	SuccessThreshold int           // default: 3
	Timeout          time.Duration // default: 30s
}

// NewCircuitBreaker creates a new circuit breaker.
func NewCircuitBreaker(cfg CircuitBreakerConfig) *CircuitBreaker {
	if cfg.FailureThreshold == 0 {
		cfg.FailureThreshold = 5
	}
	if cfg.SuccessThreshold == 0 {
		cfg.SuccessThreshold = 3
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 30 * time.Second
	}

	cb := &CircuitBreaker{
		name:             cfg.Name,
		state:            StateClosed,
		failureThreshold: cfg.FailureThreshold,
		successThreshold: cfg.SuccessThreshold,
		timeout:          cfg.Timeout,
	}

	circuitBreakerState.WithLabelValues(cfg.Name).Set(0)

	return cb
}

// Allow checks if a request is allowed through the circuit breaker.
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case StateClosed:
		return true

	case StateOpen:
		// Check if timeout has elapsed — transition to HALF_OPEN
		if time.Since(cb.lastFailureTime) >= cb.timeout {
			cb.state = StateHalfOpen
			cb.successCount = 0
			circuitBreakerState.WithLabelValues(cb.name).Set(2)
			log.Info().Str("name", cb.name).Msg("Circuit breaker → HALF_OPEN (probing)")
			return true
		}
		return false

	case StateHalfOpen:
		// Allow one probe request at a time
		return true
	}

	return false
}

// RecordSuccess records a successful call.
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	circuitBreakerRequests.WithLabelValues(cb.name, "success").Inc()

	switch cb.state {
	case StateClosed:
		// Reset failure count on success
		cb.failureCount = 0

	case StateHalfOpen:
		cb.successCount++
		if cb.successCount >= cb.successThreshold {
			cb.state = StateClosed
			cb.failureCount = 0
			cb.successCount = 0
			circuitBreakerState.WithLabelValues(cb.name).Set(0)
			circuitBreakerRecovered.WithLabelValues(cb.name).Inc()
			log.Info().Str("name", cb.name).Msg("Circuit breaker → CLOSED (recovered)")
		}
	}
}

// RecordFailure records a failed call.
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	circuitBreakerRequests.WithLabelValues(cb.name, "failure").Inc()

	switch cb.state {
	case StateClosed:
		cb.failureCount++
		if cb.failureCount >= cb.failureThreshold {
			cb.state = StateOpen
			cb.lastFailureTime = time.Now()
			circuitBreakerState.WithLabelValues(cb.name).Set(1)
			circuitBreakerTripped.WithLabelValues(cb.name).Inc()
			log.Warn().
				Str("name", cb.name).
				Int("failure_count", cb.failureCount).
				Dur("timeout", cb.timeout).
				Msg("Circuit breaker → OPEN (tripped)")
		}

	case StateHalfOpen:
		// Probe failed — back to OPEN
		cb.state = StateOpen
		cb.lastFailureTime = time.Now()
		cb.successCount = 0
		circuitBreakerState.WithLabelValues(cb.name).Set(1)
		circuitBreakerTripped.WithLabelValues(cb.name).Inc()
		log.Warn().Str("name", cb.name).Msg("Circuit breaker → OPEN (probe failed)")
	}
}

// GetState returns the current state (thread-safe read).
func (cb *CircuitBreaker) GetState() State {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

// IsAvailable returns true if the circuit is CLOSED or HALF_OPEN.
func (cb *CircuitBreaker) IsAvailable() bool {
	state := cb.GetState()
	return state == StateClosed || state == StateHalfOpen
}
