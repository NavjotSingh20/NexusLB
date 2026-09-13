package balancer

import (
	"hash/fnv"
	"net"
	"net/http"
	"strings"
	"sync"

	"nexuslb/pkg/backend"
)

type IPHash struct {
	backends []*backend.Backend
	mux      sync.RWMutex
}

func NewIPHash(backends []*backend.Backend) *IPHash {
	return &IPHash{
		backends: backends,
	}
}

func (ih *IPHash) NextBackend(r *http.Request) (*backend.Backend, error) {
	ih.mux.RLock()
	defer ih.mux.RUnlock()

	n := len(ih.backends)
	if n == 0 {
		return nil, ErrNoHealthyBackends
	}

	clientIP := ExtractClientIP(r)
	hash := hashIP(clientIP)
	primaryIdx := int(hash % uint32(n))

	// 1. Check if the primary hashed target is healthy
	primary := ih.backends[primaryIdx]
	if primary.IsAlive() {
		return primary, nil
	}

	// 2. Deterministic sequential walk fallback for fault tolerance
	for i := 1; i < n; i++ {
		fallbackIdx := (primaryIdx + i) % n
		candidate := ih.backends[fallbackIdx]
		if candidate.IsAlive() {
			return candidate, nil
		}
	}

	return nil, ErrNoHealthyBackends
}

func (ih *IPHash) GetBackends() []*backend.Backend {
	ih.mux.RLock()
	defer ih.mux.RUnlock()
	copied := make([]*backend.Backend, len(ih.backends))
	copy(copied, ih.backends)
	return copied
}

func (ih *IPHash) Name() string {
	return "IP Hash"
}

// ExtractClientIP extracts the originating client IP address, checking forwarding headers
func ExtractClientIP(r *http.Request) string {
	if r == nil {
		return "127.0.0.1"
	}

	// 1. Check X-Forwarded-For header
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		parts := strings.Split(xff, ",")
		clientIP := strings.TrimSpace(parts[0])
		if clientIP != "" {
			return normalizeIP(clientIP)
		}
	}

	// 2. Check X-Real-IP header
	if xrip := r.Header.Get("X-Real-IP"); xrip != "" {
		clientIP := strings.TrimSpace(xrip)
		if clientIP != "" {
			return normalizeIP(clientIP)
		}
	}

	// 3. Fall back to RemoteAddr
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return normalizeIP(r.RemoteAddr)
	}
	return normalizeIP(host)
}

func normalizeIP(ipStr string) string {
	trimmed := strings.TrimSpace(ipStr)
	parsed := net.ParseIP(trimmed)
	if parsed == nil {
		return trimmed
	}
	if parsed.IsLoopback() {
		return "127.0.0.1"
	}
	return parsed.String()
}

func hashIP(ip string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(ip))
	return h.Sum32()
}
