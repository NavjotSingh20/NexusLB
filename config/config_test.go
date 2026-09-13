package config

import (
	"os"
	"testing"
)

func TestConfigValidation(t *testing.T) {
	validStrategies := []string{
		"round_robin", "roundrobin", "rr",
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

	invalidStrategies := []string{"random", "weighted", "magic", "123"}
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
