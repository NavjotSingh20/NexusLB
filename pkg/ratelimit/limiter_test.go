package ratelimit

import (
	"sync"
	"testing"
	"time"
)

func TestLimiter_BasicAllow(t *testing.T) {
	// Rate: 10/s, Burst: 5
	lim := NewLimiter(10.0, 5, true)
	defer lim.Close()

	ip := "192.168.1.10"

	// First 5 requests must all be allowed
	for i := 0; i < 5; i++ {
		allowed, rem, _ := lim.Allow(ip)
		if !allowed {
			t.Fatalf("Request %d should be allowed, got blocked", i+1)
		}
		expectedRem := 5 - 1 - i
		if rem != expectedRem {
			t.Errorf("Request %d expected remaining %d, got %d", i+1, expectedRem, rem)
		}
	}

	// 6th request must be throttled
	allowed, rem, retryAfter := lim.Allow(ip)
	if allowed {
		t.Fatalf("6th request exceeded burst and must be throttled")
	}
	if rem != 0 {
		t.Errorf("Expected remaining 0, got %d", rem)
	}
	if retryAfter <= 0 {
		t.Errorf("Expected positive retryAfter duration, got %v", retryAfter)
	}
}

func TestLimiter_PerIPIsolation(t *testing.T) {
	lim := NewLimiter(5.0, 3, true)
	defer lim.Close()

	attackerIP := "203.0.113.99"
	normalIP := "198.51.100.1"

	// Attacker exhausts its burst of 3
	for i := 0; i < 3; i++ {
		if allowed, _, _ := lim.Allow(attackerIP); !allowed {
			t.Fatalf("Attacker request %d should have passed within burst", i+1)
		}
	}

	// Attacker is now throttled
	if allowed, _, _ := lim.Allow(attackerIP); allowed {
		t.Fatalf("Attacker must be throttled on 4th request")
	}

	// Legitimate user must STILL have full burst capacity and pass!
	for i := 0; i < 3; i++ {
		allowed, _, _ := lim.Allow(normalIP)
		if !allowed {
			t.Fatalf("Legitimate user %s request %d was blocked due to noisy neighbor", normalIP, i+1)
		}
	}
}

func TestLimiter_TokenRefill(t *testing.T) {
	// Rate: 10/s (1 token every 100ms), Burst: 2
	lim := NewLimiter(10.0, 2, true)
	defer lim.Close()

	ip := "10.0.0.1"

	lim.Allow(ip)
	lim.Allow(ip)

	// Bucket is empty
	if allowed, _, _ := lim.Allow(ip); allowed {
		t.Fatalf("Expected throttling when bucket empty")
	}

	// Sleep 120ms to allow ~1 token to refill
	time.Sleep(120 * time.Millisecond)

	allowed, _, _ := lim.Allow(ip)
	if !allowed {
		t.Fatalf("Expected request to be allowed after token refill")
	}
}

func TestLimiter_Disabled(t *testing.T) {
	lim := NewLimiter(1.0, 1, false)
	defer lim.Close()

	ip := "127.0.0.1"

	// All 10 requests must pass when disabled
	for i := 0; i < 10; i++ {
		allowed, _, _ := lim.Allow(ip)
		if !allowed {
			t.Fatalf("Request %d blocked while limiter disabled", i+1)
		}
	}
}

func TestLimiter_Concurrency(t *testing.T) {
	lim := NewLimiter(100.0, 50, true)
	defer lim.Close()

	var wg sync.WaitGroup
	ips := []string{"192.168.1.1", "192.168.1.2", "192.168.1.3", "192.168.1.4"}

	for _, ip := range ips {
		for i := 0; i < 25; i++ {
			wg.Add(1)
			go func(clientIP string) {
				defer wg.Done()
				lim.Allow(clientIP)
			}(ip)
		}
	}

	wg.Wait()

	total := lim.TotalAllowed() + lim.TotalBlocked()
	if total != 100 {
		t.Errorf("Expected 100 total requests processed, got %d", total)
	}
}
