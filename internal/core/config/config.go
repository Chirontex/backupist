package config

import (
	"backupist/pkg/types"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/viper"
	"gopkg.in/yaml.v3"
)

// Config основная конфигурация приложения
type Config struct {
	Database struct {
		Path string `mapstructure:"path" yaml:"path"`
	} `mapstructure:"database" yaml:"database"`

	Logging struct {
		Level  string `mapstructure:"level" yaml:"level"`
		Format string `mapstructure:"format" yaml:"format"`
		File   string `mapstructure:"file" yaml:"file"`
	} `mapstructure:"logging" yaml:"logging"`

	Storage struct {
		// Основные поля, используемые в storage.go
		Type      string `mapstructure:"type" yaml:"type"`
		LocalPath string `mapstructure:"local_path" yaml:"local_path"`

		// S3 конфигурация
		S3Endpoint   string `mapstructure:"s3_endpoint" yaml:"s3_endpoint"`
		S3AccessKey  string `mapstructure:"s3_access_key" yaml:"s3_access_key"`
		S3SecretKey  string `mapstructure:"s3_secret_key" yaml:"s3_secret_key"`
		S3BucketName string `mapstructure:"s3_bucket_name" yaml:"s3_bucket_name"`
		S3UseSSL     bool   `mapstructure:"s3_use_ssl" yaml:"s3_use_ssl"`

		// GCS конфигурация
		GCSBucketName      string `mapstructure:"gcs_bucket_name" yaml:"gcs_bucket_name"`
		GCSCredentialsPath string `mapstructure:"gcs_credentials_path" yaml:"gcs_credentials_path"`

		// Дополнительные поля
		Default string                         `mapstructure:"default" yaml:"default"`
		Configs map[string]types.StorageConfig `mapstructure:"configs" yaml:"configs"`
	} `mapstructure:"storage" yaml:"storage"`

	Encryption struct {
		DefaultAlgorithm string `mapstructure:"default_algorithm" yaml:"default_algorithm"`
		KeyDerivation    struct {
			Algorithm  string `mapstructure:"algorithm" yaml:"algorithm"`
			Iterations int    `mapstructure:"iterations" yaml:"iterations"`
			SaltSize   int    `mapstructure:"salt_size" yaml:"salt_size"`
		} `mapstructure:"key_derivation" yaml:"key_derivation"`
	} `mapstructure:"encryption" yaml:"encryption"`

	Compression struct {
		DefaultAlgorithm string `mapstructure:"default_algorithm" yaml:"default_algorithm"`
		Level            int    `mapstructure:"level" yaml:"level"`
	} `mapstructure:"compression" yaml:"compression"`
}

// NewConfig создает новую конфигурацию с значениями по умолчанию
func NewConfig() *Config {
	return &Config{
		Database: struct {
			Path string `mapstructure:"path" yaml:"path"`
		}{
			Path: getDefaultDatabasePath(),
		},
		Logging: struct {
			Level  string `mapstructure:"level" yaml:"level"`
			Format string `mapstructure:"format" yaml:"format"`
			File   string `mapstructure:"file" yaml:"file"`
		}{
			Level:  "info",
			Format: "json",
			File:   "",
		},
		Storage: struct {
			Type               string                         `mapstructure:"type" yaml:"type"`
			LocalPath          string                         `mapstructure:"local_path" yaml:"local_path"`
			S3Endpoint         string                         `mapstructure:"s3_endpoint" yaml:"s3_endpoint"`
			S3AccessKey        string                         `mapstructure:"s3_access_key" yaml:"s3_access_key"`
			S3SecretKey        string                         `mapstructure:"s3_secret_key" yaml:"s3_secret_key"`
			S3BucketName       string                         `mapstructure:"s3_bucket_name" yaml:"s3_bucket_name"`
			S3UseSSL           bool                           `mapstructure:"s3_use_ssl" yaml:"s3_use_ssl"`
			GCSBucketName      string                         `mapstructure:"gcs_bucket_name" yaml:"gcs_bucket_name"`
			GCSCredentialsPath string                         `mapstructure:"gcs_credentials_path" yaml:"gcs_credentials_path"`
			Default            string                         `mapstructure:"default" yaml:"default"`
			Configs            map[string]types.StorageConfig `mapstructure:"configs" yaml:"configs"`
		}{
			Type:      "local",
			LocalPath: getDefaultBackupPath(),
			S3UseSSL:  true,
			Default:   "local",
			Configs: map[string]types.StorageConfig{
				"local": {
					Type:      types.StorageTypeLocal,
					LocalPath: getDefaultBackupPath(),
				},
			},
		},
		Encryption: struct {
			DefaultAlgorithm string `mapstructure:"default_algorithm" yaml:"default_algorithm"`
			KeyDerivation    struct {
				Algorithm  string `mapstructure:"algorithm" yaml:"algorithm"`
				Iterations int    `mapstructure:"iterations" yaml:"iterations"`
				SaltSize   int    `mapstructure:"salt_size" yaml:"salt_size"`
			} `mapstructure:"key_derivation" yaml:"key_derivation"`
		}{
			DefaultAlgorithm: "AES-256-GCM",
			KeyDerivation: struct {
				Algorithm  string `mapstructure:"algorithm" yaml:"algorithm"`
				Iterations int    `mapstructure:"iterations" yaml:"iterations"`
				SaltSize   int    `mapstructure:"salt_size" yaml:"salt_size"`
			}{
				Algorithm:  "PBKDF2",
				Iterations: 100000,
				SaltSize:   32,
			},
		},
		Compression: struct {
			DefaultAlgorithm string `mapstructure:"default_algorithm" yaml:"default_algorithm"`
			Level            int    `mapstructure:"level" yaml:"level"`
		}{
			DefaultAlgorithm: "gzip",
			Level:            6,
		},
	}
}

// LoadConfig загружает конфигурацию из файла
func LoadConfig(configPath string) (*Config, error) {
	config := NewConfig()

	if configPath != "" {
		// Загрузка из указанного файла
		viper.SetConfigFile(configPath)
	} else {
		// Поиск конфигурации в стандартных местах
		viper.SetConfigName("backup-cli")
		viper.SetConfigType("yaml")
		viper.AddConfigPath(".")
		viper.AddConfigPath("$HOME/.config/backup-cli")
		viper.AddConfigPath("/etc/backup-cli")
	}

	// Переменные окружения
	viper.SetEnvPrefix("BACKUP")
	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("ошибка чтения конфигурации: %w", err)
		}
		// Файл конфигурации не найден, используем значения по умолчанию
	}

	if err := viper.Unmarshal(config); err != nil {
		return nil, fmt.Errorf("ошибка парсинга конфигурации: %w", err)
	}

	return config, nil
}

// SaveConfig сохраняет конфигурацию в файл
func (c *Config) SaveConfig(path string) error {
	data, err := yaml.Marshal(c)
	if err != nil {
		return fmt.Errorf("ошибка сериализации конфигурации: %w", err)
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("ошибка создания директории: %w", err)
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("ошибка записи файла конфигурации: %w", err)
	}

	return nil
}

// getDefaultDatabasePath возвращает путь к базе данных по умолчанию
func getDefaultDatabasePath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return "./backup-cli.db"
	}
	return filepath.Join(home, ".config", "backup-cli", "backup.db")
}

// getDefaultBackupPath возвращает путь для бэкапов по умолчанию
func getDefaultBackupPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return "./backups"
	}
	return filepath.Join(home, "backups")
}
