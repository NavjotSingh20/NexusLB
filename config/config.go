package config

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

type Config struct {
	ProxyPort              string        `json:"proxy_port"`
	DashboardPort          string        `json:"dashboard_port"`
	Backends               []string      `json:"backends"`
	HealthCheckInterval    time.Duration `json:"-"`
	HealthCheckIntervalStr string        `json:"health_check_interval"`
	HealthCheckPath        string        `json:"health_check_path"`
	Strategy               string        `json:"strategy"`
}

func DefaultConfig() *Config {
	return &Config{
		ProxyPort:              ":8080",
		DashboardPort:          ":8081",
		Backends: []string{
			"http://localhost:8001",
			"http://localhost:8002",
			"http://localhost:8003",
		},
		HealthCheckInterval:    3 * time.Second,
		HealthCheckIntervalStr: "3s",
		HealthCheckPath:        "/health",
		Strategy:               "round_robin",
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
		if err == nil {
			cfg.HealthCheckInterval = dur
		}
	}

	return cfg, nil
}
