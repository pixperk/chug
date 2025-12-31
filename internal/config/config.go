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
	Tables        []TableConfig `yaml:"tables"`
}

type TableConfig struct {
	Name      string         `yaml:"name"`
	Limit     *int           `yaml:"limit"`
	BatchSize *int           `yaml:"batch_size"`
	Polling   *PollingConfig `yaml:"polling"`
}

type PollingConfig struct {
	Enabled  bool   `yaml:"enabled"`
	DeltaCol string `yaml:"delta_column"`
	Interval int    `yaml:"interval_seconds"`
}

type ResolvedTableConfig struct {
	Name      string
	Limit     int
	BatchSize int
	Polling   PollingConfig
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

func (c *Config) GetEffectiveTableConfigs() []TableConfig {
	if len(c.Tables) > 0 {
		return c.Tables
	}

	if c.Table != "" {
		return []TableConfig{
			{Name: c.Table},
		}
	}

	return []TableConfig{}
}

func (c *Config) ResolveTableConfig(tc TableConfig) ResolvedTableConfig {
	resolved := ResolvedTableConfig{
		Name: tc.Name,
	}

	if tc.Limit != nil {
		resolved.Limit = *tc.Limit
	} else if c.Limit != 0 {
		resolved.Limit = c.Limit
	} else {
		resolved.Limit = 1000
	}

	if tc.BatchSize != nil {
		resolved.BatchSize = *tc.BatchSize
	} else if c.BatchSize != 0 {
		resolved.BatchSize = c.BatchSize
	} else {
		resolved.BatchSize = 500
	}

	if tc.Polling != nil {
		resolved.Polling = *tc.Polling
	} else {
		resolved.Polling = c.Polling
	}

	return resolved
}
