package backend

import (
	"sync"
	"time"
)

// CircuitState represents the operational state of a backend circuit breaker.
type CircuitState string

const (
	StateClosed   CircuitState = "closed"    // Normal traffic flow
	StateHalfOpen CircuitState = "half_open" // Trial probe request allowed
	StateOpen     CircuitState = "open"      // Quarantined, traffic bypassed
)

// CircuitBreaker implements the Martin Fowler Circuit Breaker stability pattern.
// It isolates backends that repeatedly fail requests, protecting the cluster
// from cascading failures and allowing degraded services to recover.
type CircuitBreaker struct {
	maxFailures int64
	cooldown    time.Duration

	state               CircuitState
	consecutiveFailures int64
	lastStateChange     time.Time
	halfOpenProbing     bool
	mux                 sync.Mutex
}

// NewCircuitBreaker creates a new thread-safe CircuitBreaker.
// maxFailures: number of consecutive 5xx/network errors before tripping (e.g. 5)
// cooldown: quarantine duration before entering half-open probe state (e.g. 10s)
func NewCircuitBreaker(maxFailures int64, cooldown time.Duration) *CircuitBreaker {
	if maxFailures <= 0 {
		maxFailures = 5
	}
	if cooldown <= 0 {
		cooldown = 10 * time.Second
	}
	return &CircuitBreaker{
		maxFailures:     maxFailures,
		cooldown:        cooldown,
		state:           StateClosed,
		lastStateChange: time.Now(),
	}
}

// State returns current circuit state, checking for timeout transitions.
func (cb *CircuitBreaker) State() CircuitState {
	cb.mux.Lock()
	defer cb.mux.Unlock()
	cb.checkTransition()
	return cb.state
}

// Failures returns the current consecutive failure count.
func (cb *CircuitBreaker) Failures() int64 {
	cb.mux.Lock()
	defer cb.mux.Unlock()
	return cb.consecutiveFailures
}

// Allow determines if an incoming request is admitted through the breaker.
func (cb *CircuitBreaker) Allow() bool {
	cb.mux.Lock()
	defer cb.mux.Unlock()
	cb.checkTransition()

	switch cb.state {
	case StateClosed:
		return true
	case StateOpen:
		return false
	case StateHalfOpen:
		// In half-open, permit exactly one trial probe request
		if !cb.halfOpenProbing {
			cb.halfOpenProbing = true
			return true
		}
		return false
	default:
		return true
	}
}

// RecordSuccess registers a successful response, resetting failures or closing half-open breaker.
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mux.Lock()
	defer cb.mux.Unlock()
	cb.consecutiveFailures = 0
	if cb.state == StateHalfOpen {
		cb.state = StateClosed
		cb.halfOpenProbing = false
		cb.lastStateChange = time.Now()
	}
}

// RecordFailure registers a failed request (5xx or connection drop).
func (cb *CircuitBreaker) RecordFailure() {
	cb.mux.Lock()
	defer cb.mux.Unlock()
	cb.consecutiveFailures++
	if cb.state == StateHalfOpen || cb.consecutiveFailures >= cb.maxFailures {
		cb.state = StateOpen
		cb.halfOpenProbing = false
		cb.lastStateChange = time.Now()
	}
}

// checkTransition moves from Open to Half-Open after cooldown duration expires.
func (cb *CircuitBreaker) checkTransition() {
	if cb.state == StateOpen && time.Since(cb.lastStateChange) >= cb.cooldown {
		cb.state = StateHalfOpen
		cb.halfOpenProbing = false
		cb.lastStateChange = time.Now()
	}
}

// Reset resets the circuit breaker to closed state with zero failures.
func (cb *CircuitBreaker) Reset() {
	cb.mux.Lock()
	defer cb.mux.Unlock()
	cb.state = StateClosed
	cb.consecutiveFailures = 0
	cb.halfOpenProbing = false
	cb.lastStateChange = time.Now()
}

