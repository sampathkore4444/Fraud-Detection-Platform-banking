package config

import (
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config holds all configuration for the fraud service.
type Config struct {
	Server   ServerConfig   `yaml:"server"`
	Scoring  ScoringConfig  `yaml:"scoring"`
	Redis    RedisConfig    `yaml:"redis"`
	Model    ModelConfig    `yaml:"model"`
	Rules    RulesConfig    `yaml:"rules"`
	Metrics  MetricsConfig  `yaml:"metrics"`
}

type ServerConfig struct {
	GRPCPort    int           `yaml:"grpc_port"`
	HTTPPort    int           `yaml:"http_port"`
	ReadTimeout time.Duration `yaml:"read_timeout"`
}

type ScoringConfig struct {
	ApproveThreshold float64 `yaml:"approve_threshold"` // default 0.30
	ReviewThreshold  float64 `yaml:"review_threshold"`  // default 0.70
	DefaultModel     string  `yaml:"default_model"`
	TimeoutMs        int     `yaml:"timeout_ms"`
}

type RedisConfig struct {
	Addr         string        `yaml:"addr"`
	Password     string        `yaml:"password"`
	DB           int           `yaml:"db"`
	PoolSize     int           `yaml:"pool_size"`
	ReadTimeout  time.Duration `yaml:"read_timeout"`
	WriteTimeout time.Duration `yaml:"write_timeout"`
	KeyPrefix    string        `yaml:"key_prefix"`
	VectorTTL    time.Duration `yaml:"vector_ttl"`
}

type ModelConfig struct {
	Path        string        `yaml:"path"`
	Version     string        `yaml:"version"`
	HotReload   bool          `yaml:"hot_reload"`
	CheckInterval time.Duration `yaml:"check_interval"`
	MaxFeatures int           `yaml:"max_features"`
}

type RulesConfig struct {
	Enabled           bool    `yaml:"enabled"`
	MaxAmountPerDay   float64 `yaml:"max_amount_per_day"`
	MaxTxPerHour      int     `yaml:"max_tx_per_hour"`
	MaxCountriesPerDay int    `yaml:"max_countries_per_day"`
	BlockedCountries  []string `yaml:"blocked_countries"`
}

type MetricsConfig struct {
	Enabled bool   `yaml:"enabled"`
	Port    int    `yaml:"port"`
	Prefix  string `yaml:"prefix"`
}

// Load reads configuration from a YAML file and applies defaults.
func Load(path string) (*Config, error) {
	cfg := &Config{
		Server: ServerConfig{
			GRPCPort:    50051,
			HTTPPort:    8080,
			ReadTimeout: 5 * time.Second,
		},
		Scoring: ScoringConfig{
			ApproveThreshold: 0.30,
			ReviewThreshold:  0.70,
			DefaultModel:     "xgboost-v1",
			TimeoutMs:        50,
		},
		Redis: RedisConfig{
			Addr:         "localhost:6379",
			Password:     "",
			DB:           0,
			PoolSize:     20,
			ReadTimeout:  10 * time.Millisecond,
			WriteTimeout: 10 * time.Millisecond,
			KeyPrefix:    "feature_vector:",
			VectorTTL:    5 * time.Minute,
		},
		Model: ModelConfig{
			Path:          "models/fraud_xgboost.json",
			Version:       "v1.0.0",
			HotReload:     true,
			CheckInterval: 30 * time.Second,
			MaxFeatures:   30,
		},
		Rules: RulesConfig{
			Enabled:           true,
			MaxAmountPerDay:   50000.0,
			MaxTxPerHour:      20,
			MaxCountriesPerDay: 3,
		},
		Metrics: MetricsConfig{
			Enabled: true,
			Port:    9090,
			Prefix:  "fraud_service",
		},
	}

	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		if err := yaml.Unmarshal(data, cfg); err != nil {
			return nil, err
		}
	}

	// Environment overrides
	if v := os.Getenv("REDIS_ADDR"); v != "" {
		cfg.Redis.Addr = v
	}
	if v := os.Getenv("GRPC_PORT"); v != "" {
		cfg.Server.GRPCPort = parseEnvInt(v, cfg.Server.GRPCPort)
	}
	if v := os.Getenv("MODEL_VERSION"); v != "" {
		cfg.Model.Version = v
	}

	return cfg, nil
}

func parseEnvInt(s string, def int) int {
	var v int
	for _, c := range s {
		if c >= '0' && c <= '9' {
			v = v*10 + int(c-'0')
		} else {
			return def
		}
	}
	if v == 0 {
		return def
	}
	return v
}
