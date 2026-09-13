# 02 : Sticky Session Invalidation Due to Ephemeral Port Skew in IP Hashing

# Loss of Client Affinity in Socket-Level Remote Address Hashing

## Error:

Client session affinity ("sticky sessions") failed during concurrent asset loading and page navigation benchmarks under IP Hash routing:

> [SESSION TEST] Client IP: 192.168.1.50 (Simulating Web Browser Multi-Connection Asset Fetch)
> Request 1 [GET /]         -> Backend 2 (Cookie Set: session_id=xyz)
> Request 2 [GET /style.css]-> Backend 1 (Error: 401 Unauthorized - Session Not Found)
> Request 3 [GET /app.js]   -> Backend 3 (Error: 401 Unauthorized - Session Not Found)
> [FAILURE] Session affinity broken: Same client distributed across 3 distinct nodes simultaneously.

## Relevant Context

The IP Hash balancing strategy in `pkg/balancer/ip_hash.go` was designed to map incoming client requests deterministically to a specific upstream backend. This ensures that stateful sessions, local in-memory caches, and long-lived state remain localized to a single backend without requiring a synchronized external Redis cache.

In the initial implementation, the client identifier was extracted directly from the HTTP request structure:

```go
func (ih *IPHash) NextBackend(r *http.Request) (*backend.Backend, error) {
    ih.mux.RLock()
    defer ih.mux.RUnlock()

    // Naive hash input: using RemoteAddr directly
    clientIdentifier := r.RemoteAddr

    h := fnv.New32a()
    h.Write([]byte(clientIdentifier))
    hashValue := h.Sum32()

    targetIdx := int(hashValue % uint32(len(ih.backends)))
    return ih.backends[targetIdx], nil
}
```

The system appeared to work in simple single-connection curl tests, but completely fragmented when real web browsers or multi-threaded HTTP clients connected.

## Key Observation

In Go's standard `net/http` package, `http.Request.RemoteAddr` is formatted by the HTTP server as `"<IP>:<Port>"` (e.g. `"192.168.1.50:54321"`).

When modern web browsers and HTTP client libraries make requests:
1. **Ephemeral Port Allocation**: Operating systems allocate a different ephemeral source port from the dynamic port range (`49152–65535`) for each concurrent TCP socket connection established to the proxy gateway.
2. **Hash Variance**: Because string hashing algorithms (such as FNV-1a) exhibit strong avalanche characteristics, changing even one character in the port suffix drastically alters the computed 32-bit integer:
   - `hash("192.168.1.50:54321") % 3` $\to$ **Backend 1**
   - `hash("192.168.1.50:54322") % 3` $\to$ **Backend 2**
   - `hash("192.168.1.50:54323") % 3` $\to$ **Backend 0**
3. **Loopback & Proxy Header Inconsistencies**: Furthermore, clients connecting via IPv6 loopback (`[::1]:port`) produced entirely different hashes compared to IPv4 loopback (`127.0.0.1:port`), and traffic routed through upstream enterprise proxies or CDNs contained multi-hop `X-Forwarded-For` headers (e.g. `"192.168.1.50, 10.0.0.1"`) that were ignored by `r.RemoteAddr`.

## Solution

Engineered a robust, canonical IP extraction and normalization pipeline in `ExtractClientIP` and `normalizeIP`:

1. **Proxy Header Hierarchy**: Inspects `X-Forwarded-For` (extracting the leftmost client hop) and `X-Real-IP` before falling back to `RemoteAddr`.
2. **Socket Port Stripping**: Leverages `net.SplitHostPort` to reliably isolate the host IP from dynamic ephemeral ports.
3. **Canonical Normalization**: Uses `net.ParseIP` to standardize IPv4/IPv6 loopbacks and IP representations:

```go
func ExtractClientIP(r *http.Request) string {
    if r == nil {
        return "127.0.0.1"
    }

    // 1. Check X-Forwarded-For header (leftmost entry is the originating client)
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

    // 3. Fall back to RemoteAddr, strictly stripping ephemeral client ports
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
    // Canonicalize loopback variants (::1 and 127.0.0.1)
    if parsed.IsLoopback() {
        return "127.0.0.1"
    }
    return parsed.String()
}
```

**Because**

Stripping the TCP port using `net.SplitHostPort` decouples the client's network layer identity from ephemeral transport-layer socket recycling. Normalizing parsed IP addresses ensures that concurrent browser HTTP/1.1 and HTTP/2 connections originating from the same physical machine produce identical 32-bit FNV-1a digests, preserving 100% session stickiness across parallel requests without leaking state across unrelated upstream instances.
