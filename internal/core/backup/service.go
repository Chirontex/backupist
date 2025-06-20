package backup

import (
	"backupist/internal/core/config"
	"backupist/internal/logger"
	"backupist/pkg/types"
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
)

// Service основной сервис для операций бэкапа
type Service struct {
	config  *config.Config
	logger  *logger.StructuredLogger
	db      *sql.DB
	storage StorageProvider
}

// StorageProvider интерфейс для провайдеров хранения
type StorageProvider interface {
	Upload(ctx context.Context, localPath, remotePath string) error
	Download(ctx context.Context, remotePath, localPath string) error
	Delete(ctx context.Context, remotePath string) error
	List(ctx context.Context, prefix string) ([]string, error)
	Exists(ctx context.Context, path string) (bool, error)
}

// NewService создает новый сервис бэкапа
func NewService(cfg *config.Config, log *logger.StructuredLogger) *Service {
	return &Service{
		config: cfg,
		logger: log,
	}
}

// Initialize инициализирует сервис (подключение к БД, настройка хранилища)
func (s *Service) Initialize(ctx context.Context) error {
	// Инициализация базы данных
	if err := s.initDatabase(); err != nil {
		return fmt.Errorf("ошибка инициализации БД: %w", err)
	}

	// Инициализация хранилища
	if err := s.initStorage(); err != nil {
		return fmt.Errorf("ошибка инициализации хранилища: %w", err)
	}

	s.logger.InfoContext(ctx, "Сервис бэкапа инициализирован")
	return nil
}

// CreateBackupJob создает новую задачу бэкапа
func (s *Service) CreateBackupJob(ctx context.Context, policy *types.BackupPolicy) (*types.BackupJob, error) {
	// Валидация политики
	if err := config.ValidateBackupPolicy(policy); err != nil {
		return nil, fmt.Errorf("ошибка валидации политики: %w", err)
	}

	// Генерация ID
	if policy.ID == "" {
		policy.ID = uuid.New().String()
	}

	// Сохранение политики в БД
	if err := s.savePolicy(ctx, policy); err != nil {
		return nil, fmt.Errorf("ошибка сохранения политики: %w", err)
	}

	// Создание задачи
	job := &types.BackupJob{
		ID:       uuid.New().String(),
		PolicyID: policy.ID,
		Status:   types.JobStatusPending,
	}

	s.logger.InfoContext(ctx, "Создана задача бэкапа",
		"job_id", job.ID,
		"policy_id", policy.ID,
		"source", policy.SourcePath)

	return job, nil
}

// ExecuteBackup выполняет бэкап
func (s *Service) ExecuteBackup(ctx context.Context, job *types.BackupJob) (*types.BackupResult, error) {
	backupLogger := logger.NewBackupLogger(job.ID, job.PolicyID)

	// Получение политики
	policy, err := s.getPolicy(ctx, job.PolicyID)
	if err != nil {
		return nil, fmt.Errorf("ошибка получения политики: %w", err)
	}

	backupLogger.LogBackupStart(ctx, policy.SourcePath, policy.DestinationPath)

	// Обновление статуса задачи
	job.Status = types.JobStatusRunning
	job.StartedAt = time.Now()

	startTime := time.Now()

	// Основная логика бэкапа
	result, err := s.performBackup(ctx, policy, backupLogger)
	if err != nil {
		job.Status = types.JobStatusFailed
		job.Error = err.Error()
		backupLogger.LogBackupError(ctx, err, "backup_execution")
		return nil, err
	}

	// Завершение задачи
	completedAt := time.Now()
	job.Status = types.JobStatusCompleted
	job.CompletedAt = &completedAt

	result.JobID = job.ID
	result.Duration = time.Since(startTime)

	backupLogger.LogBackupComplete(ctx, logger.BackupResult{
		BackupPath:     result.BackupPath,
		FilesProcessed: result.FilesProcessed,
		TotalSize:      result.TotalSize,
		Duration:       result.Duration,
		Compressed:     result.Compressed,
		Encrypted:      result.Encrypted,
	})

	return result, nil
}

// performBackup выполняет основную логику бэкапа
func (s *Service) performBackup(ctx context.Context, policy *types.BackupPolicy, logger *logger.BackupLogger) (*types.BackupResult, error) {
	result := &types.BackupResult{
		Compressed: policy.ArchiveEnabled,
		Encrypted:  policy.EncryptionEnabled,
	}

	// Создание временной директории для подготовки
	tempDir, err := os.MkdirTemp("", "backup-*")
	if err != nil {
		return nil, fmt.Errorf("ошибка создания временной директории: %w", err)
	}
	defer os.RemoveAll(tempDir)

	// Сканирование исходной директории
	files, totalSize, err := s.scanDirectory(ctx, policy.SourcePath)
	if err != nil {
		return nil, fmt.Errorf("ошибка сканирования директории: %w", err)
	}

	result.FilesProcessed = int64(len(files))
	result.TotalSize = totalSize

	// Создание имени файла бэкапа
	backupName := s.generateBackupName(policy)
	backupPath := filepath.Join(tempDir, backupName)

	// Копирование файлов
	if err := s.copyFiles(ctx, policy.SourcePath, backupPath, files, logger); err != nil {
		return nil, fmt.Errorf("ошибка копирования файлов: %w", err)
	}

	// Архивирование (если включено)
	if policy.ArchiveEnabled {
		archivePath := backupPath + ".tar.gz"
		if err := s.createArchive(ctx, backupPath, archivePath); err != nil {
			return nil, fmt.Errorf("ошибка архивирования: %w", err)
		}
		backupPath = archivePath

		// Вычисление коэффициента сжатия
		if originalSize, err := s.getDirectorySize(backupPath); err == nil {
			if compressedSize, err := s.getFileSize(archivePath); err == nil {
				result.CompressionRatio = float64(originalSize) / float64(compressedSize)
				result.CompressedSize = compressedSize
			}
		}
	}

	// Шифрование (если включено)
	if policy.EncryptionEnabled {
		encryptedPath := backupPath + ".enc"
		if err := s.encryptFile(ctx, backupPath, encryptedPath, policy.EncryptionPassword); err != nil {
			return nil, fmt.Errorf("ошибка шифрования: %w", err)
		}
		backupPath = encryptedPath
	}

	// Вычисление контрольной суммы
	checksum, err := s.calculateChecksum(backupPath)
	if err != nil {
		return nil, fmt.Errorf("ошибка вычисления контрольной суммы: %w", err)
	}
	result.Checksum = checksum

	// Загрузка в хранилище
	remotePath := s.generateRemotePath(policy, backupName)
	if err := s.storage.Upload(ctx, backupPath, remotePath); err != nil {
		return nil, fmt.Errorf("ошибка загрузки в хранилище: %w", err)
	}

	result.BackupPath = remotePath

	// Очистка старых бэкапов согласно политике retention
	if err := s.cleanupOldBackups(ctx, policy); err != nil {
		logger.Error("Ошибка очистки старых бэкапов", "error", err)
		// Не прерываем выполнение, только логируем
	}

	return result, nil
}

// scanDirectory сканирует директорию и возвращает список файлов
func (s *Service) scanDirectory(ctx context.Context, path string) ([]string, int64, error) {
	var files []string
	var totalSize int64

	err := filepath.Walk(path, func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() {
			files = append(files, filePath)
			totalSize += info.Size()
		}

		// Проверка отмены контекста
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		return nil
	})

	return files, totalSize, err
}

// copyFiles копирует файлы в целевую директорию
func (s *Service) copyFiles(ctx context.Context, sourcePath, destPath string, files []string, logger *logger.BackupLogger) error {
	for i, file := range files {
		// Вычисление относительного пути
		relPath, err := filepath.Rel(sourcePath, file)
		if err != nil {
			return fmt.Errorf("ошибка вычисления относительного пути: %w", err)
		}

		destFile := filepath.Join(destPath, relPath)

		// Создание директории для файла
		if err := os.MkdirAll(filepath.Dir(destFile), 0755); err != nil {
			return fmt.Errorf("ошибка создания директории: %w", err)
		}

		// Копирование файла
		if err := s.copyFile(file, destFile); err != nil {
			return fmt.Errorf("ошибка копирования файла %s: %w", file, err)
		}

		// Логирование прогресса
		logger.LogBackupProgress(ctx, int64(i+1), int64(len(files)), file)

		// Проверка отмены контекста
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}

	return nil
}

// copyFile копирует один файл
func (s *Service) copyFile(src, dst string) error {
	source, err := os.Open(src)
	if err != nil {
		return err
	}
	defer source.Close()

	destination, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destination.Close()

	_, err = io.Copy(destination, source)
	return err
}

// generateBackupName генерирует имя файла бэкапа
func (s *Service) generateBackupName(policy *types.BackupPolicy) string {
	timestamp := time.Now().Format("20060102-150405")
	baseName := strings.ReplaceAll(policy.Name, " ", "-")
	return fmt.Sprintf("%s-%s", baseName, timestamp)
}

// generateRemotePath генерирует путь в удаленном хранилище
func (s *Service) generateRemotePath(policy *types.BackupPolicy, backupName string) string {
	// Извлекаем путь из destination
	if strings.HasPrefix(policy.DestinationPath, "s3://") ||
		strings.HasPrefix(policy.DestinationPath, "gcs://") {
		parts := strings.SplitN(policy.DestinationPath, "/", 4)
		if len(parts) >= 4 {
			return fmt.Sprintf("%s/%s", parts[3], backupName)
		}
		return backupName
	}

	// Локальный путь
	return filepath.Join(policy.DestinationPath, backupName)
}

// calculateChecksum вычисляет SHA-256 контрольную сумму файла
func (s *Service) calculateChecksum(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}

	return fmt.Sprintf("%x", hash.Sum(nil)), nil
}

// Дополнительные методы (createArchive, encryptFile, cleanupOldBackups и т.д.)
// будут реализованы в отдельных файлах для краткости
