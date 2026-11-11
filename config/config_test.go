package config

import (
	"os"
	"testing"
	"time"
)

func TestConfigValidation(t *testing.T) {
	validStrategies := []string{
		"adaptive", "smart_auto", "smart", "auto", "dynamic",
		"round_robin", "roundrobin", "rr",
		"weighted_round_robin", "weighted_rr", "wrr", "weighted",
		"least_connections", "least_conn", "leastconn", "lc",
		"ip_hash", "iphash", "hash",
		"",
	}

	for _, strat := range validStrategies {
		cfg := DefaultConfig()
		cfg.Strategy = strat
		if err := cfg.Validate(); err != nil {
			t.Errorf("expected strategy %q to be valid, got: %v", strat, err)
		}
	}

	invalidStrategies := []string{"random", "unknown_strat", "magic", "123"}
	for _, strat := range invalidStrategies {
		cfg := DefaultConfig()
		cfg.Strategy = strat
		if err := cfg.Validate(); err == nil {
			t.Errorf("expected strategy %q to fail validation, but it passed", strat)
		}
	}
}

func TestLoadConfig_NonExistent(t *testing.T) {
	cfg, err := LoadConfig("non_existent_file_nexuslb.json")
	if err != nil {
		t.Fatalf("expected non-existent file to return default config without error, got: %v", err)
	}
	if cfg.Strategy != "round_robin" {
		t.Errorf("expected default strategy round_robin, got %q", cfg.Strategy)
	}
}

func TestLoadConfig_InvalidJSON(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "invalid_cfg_*.json")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())

	tmpFile.WriteString("{invalid-json")
	tmpFile.Close()

	_, err = LoadConfig(tmpFile.Name())
	if err == nil {
		t.Error("expected error for invalid JSON, got nil")
	}
}

func TestLoadConfig_ExternalizedDefaultsAndOverrides(t *testing.T) {
	def := DefaultConfig()
	if def.MaxRetryBodyBytes != 1<<20 {
		t.Errorf("expected default MaxRetryBodyBytes 1048576, got %d", def.MaxRetryBodyBytes)
	}
	if def.RetryNonIdempotent {
		t.Error("expected default RetryNonIdempotent to be false")
	}
	if !def.RateLimitEnabled || def.RateLimitRate != 10.0 || def.RateLimitBurst != 10 {
		t.Errorf("unexpected default rate limit settings: %+v", def)
	}
	if def.CircuitBreakerMaxFailures != 5 || def.CircuitBreakerCooldown != 10*time.Second {
		t.Errorf("unexpected default circuit breaker settings: %+v", def)
	}

	tmpFile, err := os.CreateTemp("", "override_cfg_*.json")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())

	tmpFile.WriteString(`{
		"strategy": "least_connections",
		"max_retries": 3,
		"max_retry_body_bytes": 524288,
		"retry_non_idempotent": true,
		"rate_limit_enabled": false,
		"rate_limit_rate": 25.5,
		"rate_limit_burst": 40,
		"circuit_breaker_max_failures": 8,
		"circuit_breaker_cooldown": "15s"
	}`)
	tmpFile.Close()

	cfg, err := LoadConfig(tmpFile.Name())
	if err != nil {
		t.Fatalf("unexpected error loading override config: %v", err)
	}
	if cfg.MaxRetries != 3 || cfg.MaxRetryBodyBytes != 524288 || !cfg.RetryNonIdempotent {
		t.Errorf("unexpected retry overrides: %+v", cfg)
	}
	if cfg.RateLimitEnabled || cfg.RateLimitRate != 25.5 || cfg.RateLimitBurst != 40 {
		t.Errorf("unexpected rate limit overrides: %+v", cfg)
	}
	if cfg.CircuitBreakerMaxFailures != 8 || cfg.CircuitBreakerCooldown != 15*time.Second {
		t.Errorf("unexpected circuit breaker overrides: %+v", cfg)
	}
}

func TestConfigValidation_ExternalizedBounds(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Config)
	}{
		{"negative max_retries", func(c *Config) { c.MaxRetries = -1 }},
		{"negative max_retry_body_bytes", func(c *Config) { c.MaxRetryBodyBytes = -1 }},
		{"non-positive rate_limit_rate", func(c *Config) { c.RateLimitRate = 0 }},
		{"zero rate_limit_burst", func(c *Config) { c.RateLimitBurst = 0 }},
		{"zero circuit_breaker_max_failures", func(c *Config) { c.CircuitBreakerMaxFailures = 0 }},
		{"non-positive circuit_breaker_cooldown", func(c *Config) { c.CircuitBreakerCooldown = 0 }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tc.mutate(cfg)
			if err := cfg.Validate(); err == nil {
				t.Errorf("expected validation failure for %s", tc.name)
			}
		})
	}
}
