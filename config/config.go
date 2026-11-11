package config

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

type Config struct {
	ProxyPort                 string        `json:"proxy_port"`
	DashboardPort             string        `json:"dashboard_port"`
	Backends                  []string      `json:"backends"`
	HealthCheckInterval       time.Duration `json:"-"`
	HealthCheckIntervalStr    string        `json:"health_check_interval"`
	HealthCheckPath           string        `json:"health_check_path"`
	Strategy                  string        `json:"strategy"`
	MaxRetries                int           `json:"max_retries"`
	MaxRetryBodyBytes         int64         `json:"max_retry_body_bytes"`
	RetryNonIdempotent        bool          `json:"retry_non_idempotent"`
	RateLimitEnabled          bool          `json:"rate_limit_enabled"`
	RateLimitRate             float64       `json:"rate_limit_rate"`
	RateLimitBurst            int           `json:"rate_limit_burst"`
	CircuitBreakerMaxFailures int64         `json:"circuit_breaker_max_failures"`
	CircuitBreakerCooldown    time.Duration `json:"-"`
	CircuitBreakerCooldownStr string        `json:"circuit_breaker_cooldown"`
}

func DefaultConfig() *Config {
	return &Config{
		ProxyPort:     ":8080",
		DashboardPort: ":8081",
		Backends: []string{
			"http://localhost:8001",
			"http://localhost:8002",
			"http://localhost:8003",
		},
		HealthCheckInterval:       3 * time.Second,
		HealthCheckIntervalStr:    "3s",
		HealthCheckPath:           "/health",
		Strategy:                  "round_robin",
		MaxRetries:                2,
		MaxRetryBodyBytes:         1 << 20, // 1 MiB
		RetryNonIdempotent:        false,
		RateLimitEnabled:          true,
		RateLimitRate:             10.0,
		RateLimitBurst:            10,
		CircuitBreakerMaxFailures: 5,
		CircuitBreakerCooldown:    10 * time.Second,
		CircuitBreakerCooldownStr: "10s",
	}
}

func LoadConfig(path string) (*Config, error) {
	cfg := DefaultConfig()

	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return nil, fmt.Errorf("failed to open config file: %w", err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	if err := decoder.Decode(cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config JSON: %w", err)
	}

	if cfg.HealthCheckIntervalStr != "" {
		dur, err := time.ParseDuration(cfg.HealthCheckIntervalStr)
		if err != nil {
			return nil, fmt.Errorf("invalid health_check_interval %q: %w", cfg.HealthCheckIntervalStr, err)
		}
		cfg.HealthCheckInterval = dur
	}

	if cfg.CircuitBreakerCooldownStr != "" {
		dur, err := time.ParseDuration(cfg.CircuitBreakerCooldownStr)
		if err != nil {
			return nil, fmt.Errorf("invalid circuit_breaker_cooldown %q: %w", cfg.CircuitBreakerCooldownStr, err)
		}
		cfg.CircuitBreakerCooldown = dur
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

func (c *Config) Validate() error {
	switch c.Strategy {
	case "round_robin", "roundrobin", "rr",
		"weighted_round_robin", "weighted_rr", "wrr", "weighted",
		"least_connections", "least_conn", "leastconn", "lc",
		"ip_hash", "iphash", "hash":
	case "":
		c.Strategy = "round_robin"
	default:
		return fmt.Errorf("invalid strategy %q: supported strategies are round_robin, weighted_round_robin, least_connections, ip_hash", c.Strategy)
	}

	if c.MaxRetries < 0 {
		return fmt.Errorf("invalid max_retries %d: must be >= 0", c.MaxRetries)
	}
	if c.MaxRetryBodyBytes < 0 {
		return fmt.Errorf("invalid max_retry_body_bytes %d: must be >= 0", c.MaxRetryBodyBytes)
	}
	if c.RateLimitRate <= 0 {
		return fmt.Errorf("invalid rate_limit_rate %v: must be > 0", c.RateLimitRate)
	}
	if c.RateLimitBurst < 1 {
		return fmt.Errorf("invalid rate_limit_burst %d: must be >= 1", c.RateLimitBurst)
	}
	if c.CircuitBreakerMaxFailures < 1 {
		return fmt.Errorf("invalid circuit_breaker_max_failures %d: must be >= 1", c.CircuitBreakerMaxFailures)
	}
	if c.CircuitBreakerCooldown <= 0 {
		return fmt.Errorf("invalid circuit_breaker_cooldown %v: must be > 0", c.CircuitBreakerCooldown)
	}

	return nil
}
