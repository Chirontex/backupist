package logger

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/natefinch/lumberjack.v2"
)

// StructuredLogger обертка над slog с дополнительной функциональностью
type StructuredLogger struct {
	logger    *slog.Logger
	component string
}

// LogConfig конфигурация логирования
type LogConfig struct {
	Level      slog.Level `json:"level"`
	Format     string     `json:"format"` // "json" или "text"
	OutputFile string     `json:"output_file,omitempty"`
	MaxSize    int        `json:"max_size"` // MB
	MaxBackups int        `json:"max_backups"`
	MaxAge     int        `json:"max_age"` // дни
	Compress   bool       `json:"compress"`
}

// NewStructuredLogger создает новый структурированный логгер
func NewStructuredLogger(component string) *StructuredLogger {
	config := &LogConfig{
		Level:      slog.LevelInfo,
		Format:     "json",
		MaxSize:    100,
		MaxBackups: 3,
		MaxAge:     7,
		Compress:   true,
	}

	return NewStructuredLoggerWithConfig(component, config)
}

// NewStructuredLoggerWithConfig создает логгер с кастомной конфигурацией
func NewStructuredLoggerWithConfig(component string, config *LogConfig) *StructuredLogger {
	var writer io.Writer

	if config.OutputFile != "" {
		// Создаем директорию для логов если нужно
		dir := filepath.Dir(config.OutputFile)
		os.MkdirAll(dir, 0755)

		// Настройка ротации логов
		writer = &lumberjack.Logger{
			Filename:   config.OutputFile,
			MaxSize:    config.MaxSize,
			MaxBackups: config.MaxBackups,
			MaxAge:     config.MaxAge,
			Compress:   config.Compress,
		}
	} else {
		writer = os.Stderr
	}

	// Выбор обработчика в зависимости от формата
	var handler slog.Handler
	opts := &slog.HandlerOptions{
		Level:     config.Level,
		AddSource: config.Level <= slog.LevelDebug,
	}

	if config.Format == "text" {
		handler = slog.NewTextHandler(writer, opts)
	} else {
		handler = slog.NewJSONHandler(writer, opts)
	}

	logger := slog.New(handler)

	return &StructuredLogger{
		logger:    logger,
		component: component,
	}
}

// withComponent добавляет компонент в контекст логирования
func (l *StructuredLogger) withComponent() *slog.Logger {
	return l.logger.With("component", l.component)
}

// Debug логирование на уровне DEBUG
func (l *StructuredLogger) Debug(msg string, args ...any) {
	l.withComponent().Debug(msg, args...)
}

// DebugContext логирование на уровне DEBUG с контекстом
func (l *StructuredLogger) DebugContext(ctx context.Context, msg string, args ...any) {
	l.withComponent().DebugContext(ctx, msg, args...)
}

// Info логирование на уровне INFO
func (l *StructuredLogger) Info(msg string, args ...any) {
	l.withComponent().Info(msg, args...)
}

// InfoContext логирование на уровне INFO с контекстом
func (l *StructuredLogger) InfoContext(ctx context.Context, msg string, args ...any) {
	l.withComponent().InfoContext(ctx, msg, args...)
}

// Warn логирование на уровне WARN
func (l *StructuredLogger) Warn(msg string, args ...any) {
	l.withComponent().Warn(msg, args...)
}

// WarnContext логирование на уровне WARN с контекстом
func (l *StructuredLogger) WarnContext(ctx context.Context, msg string, args ...any) {
	l.withComponent().WarnContext(ctx, msg, args...)
}

// Error логирование на уровне ERROR
func (l *StructuredLogger) Error(msg string, args ...any) {
	l.withComponent().Error(msg, args...)
}

// ErrorContext логирование на уровне ERROR с контекстом
func (l *StructuredLogger) ErrorContext(ctx context.Context, msg string, args ...any) {
	l.withComponent().ErrorContext(ctx, msg, args...)
}

// WithFields создает логгер с предустановленными полями
func (l *StructuredLogger) WithFields(fields map[string]any) *StructuredLogger {
	args := make([]any, 0, len(fields)*2)
	for k, v := range fields {
		args = append(args, k, v)
	}

	return &StructuredLogger{
		logger:    l.logger.With(args...),
		component: l.component,
	}
}

// BackupLogger специализированный логгер для операций бэкапа
type BackupLogger struct {
	*StructuredLogger
	jobID    string
	policyID string
}

// NewBackupLogger создает логгер для операций бэкапа
func NewBackupLogger(jobID, policyID string) *BackupLogger {
	baseLogger := NewStructuredLogger("backup")

	return &BackupLogger{
		StructuredLogger: baseLogger.WithFields(map[string]any{
			"job_id":    jobID,
			"policy_id": policyID,
		}),
		jobID:    jobID,
		policyID: policyID,
	}
}

// LogBackupStart логирует начало бэкапа
func (bl *BackupLogger) LogBackupStart(ctx context.Context, sourcePath, destPath string) {
	bl.InfoContext(ctx, "Начало выполнения бэкапа",
		"source_path", sourcePath,
		"destination_path", destPath,
		"timestamp", time.Now())
}

// LogBackupProgress логирует прогресс бэкапа
func (bl *BackupLogger) LogBackupProgress(ctx context.Context, processed, total int64, currentFile string) {
	progress := float64(processed) / float64(total) * 100
	bl.DebugContext(ctx, "Прогресс бэкапа",
		"files_processed", processed,
		"total_files", total,
		"progress_percent", progress,
		"current_file", currentFile)
}

// LogBackupComplete логирует завершение бэкапа
func (bl *BackupLogger) LogBackupComplete(ctx context.Context, result BackupResult) {
	bl.InfoContext(ctx, "Бэкап завершен успешно",
		"backup_path", result.BackupPath,
		"files_processed", result.FilesProcessed,
		"total_size", result.TotalSize,
		"duration", result.Duration,
		"compressed", result.Compressed,
		"encrypted", result.Encrypted)
}

// LogBackupError логирует ошибку бэкапа
func (bl *BackupLogger) LogBackupError(ctx context.Context, err error, operation string) {
	bl.ErrorContext(ctx, "Ошибка при выполнении бэкапа",
		"error", err.Error(),
		"operation", operation,
		"timestamp", time.Now())
}

// BackupResult результат выполнения бэкапа для логирования
type BackupResult struct {
	BackupPath     string
	FilesProcessed int64
	TotalSize      int64
	Duration       time.Duration
	Compressed     bool
	Encrypted      bool
}
