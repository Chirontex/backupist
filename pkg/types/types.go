package types

import (
	"time"
)

// BackupPolicy определяет политику создания бэкапов
type BackupPolicy struct {
	ID                 string       `json:"id" validate:"required"`
	Name               string       `json:"name" validate:"required,min=1,max=100"`
	SourcePath         string       `json:"source_path" validate:"required,dir"`
	DestinationPath    string       `json:"destination_path" validate:"required"`
	Schedule           string       `json:"schedule" validate:"omitempty,cron"`
	RetentionCount     int          `json:"retention_count" validate:"min=1,max=100"`
	ArchiveEnabled     bool         `json:"archive_enabled"`
	EncryptionEnabled  bool         `json:"encryption_enabled"`
	EncryptionPassword string       `json:"-"` // Не сериализуем пароль
	CreatedAt          time.Time    `json:"created_at"`
	UpdatedAt          time.Time    `json:"updated_at"`
	Status             BackupStatus `json:"status"`
}

// BackupJob представляет задачу бэкапа
type BackupJob struct {
	ID             string       `json:"id"`
	PolicyID       string       `json:"policy_id"`
	Status         JobStatus    `json:"status"`
	StartedAt      time.Time    `json:"started_at"`
	CompletedAt    *time.Time   `json:"completed_at,omitempty"`
	Error          string       `json:"error,omitempty"`
	Progress       *JobProgress `json:"progress,omitempty"` // Для совместимости с API
	FilesProcessed int64        `json:"files_processed"`    // ДОБАВЛЕНО для database.go
	TotalSize      int64        `json:"total_size"`         // ДОБАВЛЕНО для database.go
	BackupPath     string       `json:"backup_path"`        // ДОБАВЛЕНО для database.go
	CreatedAt      time.Time    `json:"created_at"`         // ДОБАВЛЕНО для database.go
}

// JobProgress отслеживает прогресс выполнения задачи
type JobProgress struct {
	FilesProcessed  int64   `json:"files_processed"`
	TotalFiles      int64   `json:"total_files"`
	BytesProcessed  int64   `json:"bytes_processed"`
	TotalBytes      int64   `json:"total_bytes"`
	PercentComplete float64 `json:"percent_complete"`
	CurrentFile     string  `json:"current_file"`
}

// BackupResult содержит результат выполнения бэкапа
type BackupResult struct {
	JobID            string        `json:"job_id"`
	BackupPath       string        `json:"backup_path"`
	FilesProcessed   int64         `json:"files_processed"`
	TotalSize        int64         `json:"total_size"`
	CompressedSize   int64         `json:"compressed_size,omitempty"`
	Duration         time.Duration `json:"duration"`
	Compressed       bool          `json:"compressed"`
	Encrypted        bool          `json:"encrypted"`
	CompressionRatio float64       `json:"compression_ratio,omitempty"`
	Checksum         string        `json:"checksum"`
}

// BackupStatus статус политики бэкапа
type BackupStatus string

const (
	BackupStatusActive   BackupStatus = "active"
	BackupStatusInactive BackupStatus = "inactive"
	BackupStatusPaused   BackupStatus = "paused"
	BackupStatusError    BackupStatus = "error"
)

// JobStatus статус задачи бэкапа
type JobStatus string

const (
	JobStatusPending   JobStatus = "pending"
	JobStatusRunning   JobStatus = "running"
	JobStatusCompleted JobStatus = "completed"
	JobStatusFailed    JobStatus = "failed"
	JobStatusCancelled JobStatus = "cancelled"
	JobStatusDeleted   JobStatus = "deleted"
)

// StorageConfig конфигурация хранилища
type StorageConfig struct {
	Type      StorageType `json:"type"`
	LocalPath string      `json:"local_path,omitempty"`
	S3Config  *S3Config   `json:"s3_config,omitempty"`
	GCSConfig *GCSConfig  `json:"gcs_config,omitempty"`
}

type StorageType string

const (
	StorageTypeLocal StorageType = "local"
	StorageTypeS3    StorageType = "s3"
	StorageTypeGCS   StorageType = "gcs"
)

// S3Config конфигурация для Amazon S3
type S3Config struct {
	Bucket          string `json:"bucket"`
	Region          string `json:"region"`
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"-"`
	Endpoint        string `json:"endpoint,omitempty"`
	UseSSL          bool   `json:"use_ssl"`
}

// GCSConfig конфигурация для Google Cloud Storage
type GCSConfig struct {
	Bucket             string `json:"bucket"`
	ProjectID          string `json:"project_id"`
	CredentialsPath    string `json:"credentials_path"`
	ServiceAccountJSON string `json:"-"`
}
