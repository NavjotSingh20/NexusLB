package ratelimit

import (
	"sync"
	"sync/atomic"
	"time"
)

// bucket tracks token bucket state for a single client IP.
type bucket struct {
	tokens     float64
	lastRefill time.Time
	lastSeen   time.Time
	mux        sync.Mutex
}

// Limiter manages per-IP token bucket rate limiting.
type Limiter struct {
	rate         float64      // tokens per second
	burst        int          // maximum bucket capacity
	enabled      bool         // whether rate limiting is actively enforced
	buckets      sync.Map     // map[string]*bucket
	totalBlocked int64        // atomic counter for rejected requests (429)
	totalAllowed int64        // atomic counter for passed requests
	mux          sync.RWMutex // protects configuration updates
	stopCleanup  chan struct{}
}

// NewLimiter creates a new thread-safe per-IP token bucket rate limiter.
// rate: tokens refilled per second (e.g. 10.0)
// burst: maximum burst capacity (e.g. 10)
func NewLimiter(rate float64, burst int, enabled bool) *Limiter {
	l := &Limiter{
		rate:        rate,
		burst:       burst,
		enabled:     enabled,
		stopCleanup: make(chan struct{}),
	}

	// Start background cleanup routine to purge inactive IP buckets every 5 minutes
	go l.cleanupLoop()

	return l
}

// Allow evaluates whether an incoming request from clientIP is admitted.
// Returns:
// - allowed: true if request passes under rate limit, false if throttled
// - remaining: remaining tokens in the bucket
// - retryAfter: duration client must wait before next token arrives (if throttled)
func (l *Limiter) Allow(clientIP string) (bool, int, time.Duration) {
	l.mux.RLock()
	enabled := l.enabled
	rate := l.rate
	burst := l.burst
	l.mux.RUnlock()

	if !enabled {
		atomic.AddInt64(&l.totalAllowed, 1)
		return true, burst, 0
	}

	now := time.Now()

	val, _ := l.buckets.LoadOrStore(clientIP, &bucket{
		tokens:     float64(burst),
		lastRefill: now,
		lastSeen:   now,
	})
	b := val.(*bucket)

	b.mux.Lock()
	defer b.mux.Unlock()

	b.lastSeen = now

	// Refill tokens based on elapsed duration
	elapsed := now.Sub(b.lastRefill).Seconds()
	if elapsed > 0 {
		b.tokens += elapsed * rate
		if b.tokens > float64(burst) {
			b.tokens = float64(burst)
		}
		b.lastRefill = now
	}

	// Admission check
	if b.tokens >= 1.0 {
		b.tokens -= 1.0
		rem := int(b.tokens)
		atomic.AddInt64(&l.totalAllowed, 1)
		return true, rem, 0
	}

	// Throttled: calculate time until 1 token is available
	missing := 1.0 - b.tokens
	retrySeconds := missing / rate
	if retrySeconds < 0.1 {
		retrySeconds = 0.1
	}
	retryAfter := time.Duration(retrySeconds * float64(time.Second))

	atomic.AddInt64(&l.totalBlocked, 1)
	return false, 0, retryAfter
}

// IsEnabled returns current enforcement status.
func (l *Limiter) IsEnabled() bool {
	l.mux.RLock()
	defer l.mux.RUnlock()
	return l.enabled
}

// SetEnabled dynamically enables or disables rate limiting.
func (l *Limiter) SetEnabled(enabled bool) {
	l.mux.Lock()
	defer l.mux.Unlock()
	l.enabled = enabled
}

// SetConfig updates rate and burst capacity dynamically.
func (l *Limiter) SetConfig(rate float64, burst int) {
	l.mux.Lock()
	defer l.mux.Unlock()
	if rate > 0 {
		l.rate = rate
	}
	if burst > 0 {
		l.burst = burst
	}
}

// Rate returns the configured refill rate.
func (l *Limiter) Rate() float64 {
	l.mux.RLock()
	defer l.mux.RUnlock()
	return l.rate
}

// Burst returns the configured burst capacity.
func (l *Limiter) Burst() int {
	l.mux.RLock()
	defer l.mux.RUnlock()
	return l.burst
}

// TotalBlocked returns the total number of rejected 429 requests.
func (l *Limiter) TotalBlocked() int64 {
	return atomic.LoadInt64(&l.totalBlocked)
}

// TotalAllowed returns the total number of passed requests.
func (l *Limiter) TotalAllowed() int64 {
	return atomic.LoadInt64(&l.totalAllowed)
}

// ActiveIPCount returns the number of tracked IP buckets.
func (l *Limiter) ActiveIPCount() int {
	count := 0
	l.buckets.Range(func(_, _ interface{}) bool {
		count++
		return true
	})
	return count
}

// Reset clears tracked buckets and reset counters.
func (l *Limiter) Reset() {
	l.buckets = sync.Map{}
	atomic.StoreInt64(&l.totalBlocked, 0)
	atomic.StoreInt64(&l.totalAllowed, 0)
}

// Close stops background cleanup routines.
func (l *Limiter) Close() {
	select {
	case <-l.stopCleanup:
	default:
		close(l.stopCleanup)
	}
}

// cleanupLoop periodically evicts buckets that have been inactive for over 10 minutes.
func (l *Limiter) cleanupLoop() {
	ticker := time.NewTicker(3 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-l.stopCleanup:
			return
		case <-ticker.C:
			cutoff := time.Now().Add(-10 * time.Minute)
			l.buckets.Range(func(key, val interface{}) bool {
				b := val.(*bucket)
				b.mux.Lock()
				seen := b.lastSeen
				b.mux.Unlock()

				if seen.Before(cutoff) {
					l.buckets.Delete(key)
				}
				return true
			})
		}
	}
}
