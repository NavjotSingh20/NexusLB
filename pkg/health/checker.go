package health

import (
	"context"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"

	"nexuslb/pkg/backend"
)

type Checker struct {
	backends []*backend.Backend
	interval time.Duration
	path     string
	client   *http.Client
	mux      sync.RWMutex
}

func NewChecker(backends []*backend.Backend, interval time.Duration, path string) *Checker {
	if interval <= 0 {
		interval = 3 * time.Second
	}
	if path == "" {
		path = "/health"
	}

	return &Checker{
		backends: backends,
		interval: interval,
		path:     path,
		client: &http.Client{
			Timeout: 2 * time.Second,
		},
	}
}

func (c *Checker) Start(ctx context.Context) {
	ticker := time.NewTicker(c.interval)
	go func() {
		// Run initial check immediately
		c.CheckAll()

		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				return
			case <-ticker.C:
				c.CheckAll()
			}
		}
	}()
}

func (c *Checker) CheckAll() {
	c.mux.RLock()
	backendsCopy := make([]*backend.Backend, len(c.backends))
	copy(backendsCopy, c.backends)
	c.mux.RUnlock()

	var wg sync.WaitGroup
	for _, b := range backendsCopy {
		wg.Add(1)
		go func(target *backend.Backend) {
			defer wg.Done()
			c.check(target)
		}(b)
	}
	wg.Wait()
}

func (c *Checker) check(b *backend.Backend) {
	checkURL, err := url.Parse(b.URL.String())
	if err != nil {
		return
	}
	checkURL.Path = c.path

	resp, err := c.client.Get(checkURL.String())
	wasAlive := b.IsUp()

	if err != nil || resp.StatusCode != http.StatusOK {
		if wasAlive {
			log.Printf("[HEALTH] Backend %s state changed: HEALTHY -> UNHEALTHY (err: %v)", b.URL.String(), err)
		}
		b.SetAlive(false)
		if resp != nil {
			resp.Body.Close()
		}
		return
	}
	defer resp.Body.Close()

	if !wasAlive {
		log.Printf("[HEALTH] Backend %s state changed: UNHEALTHY -> HEALTHY", b.URL.String())
	}
	b.SetAlive(true)
}

func (c *Checker) AddBackend(b *backend.Backend) {
	c.mux.Lock()
	defer c.mux.Unlock()
	c.backends = append(c.backends, b)
}

func (c *Checker) GetBackends() []*backend.Backend {
	c.mux.RLock()
	defer c.mux.RUnlock()
	copied := make([]*backend.Backend, len(c.backends))
	copy(copied, c.backends)
	return copied
}
