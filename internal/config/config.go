package config

import (
	"errors"
	"os"

	"gopkg.in/yaml.v2"
)

type Config struct {
	PostgresURL   string        `yaml:"pg_url"`
	ClickHouseURL string        `yaml:"ch_url"`
	Table         string        `yaml:"table"`
	Limit         int           `yaml:"limit"`
	BatchSize     int           `yaml:"batch_size"`
	Polling       PollingConfig `yaml:"polling"`
}

type PollingConfig struct {
	Enabled  bool   `yaml:"enabled"`
	DeltaCol string `yaml:"delta_column"`
	Interval int    `yaml:"interval_seconds"`
}

func Load(path string) (*Config, error) {
	if path == "" {
		path = ".chug.yaml"
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.New("config file not found, please provide --config flag or create .chug.yaml")
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, errors.New("failed to parse config file: " + err.Error())
	}

	return &config, nil
}
