package backup

import (
	"backupist/pkg/types"
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// initDatabase инициализирует SQLite базу данных
func (s *Service) initDatabase() error {
	var err error
	s.db, err = sql.Open("sqlite3", s.config.Database.Path)
	if err != nil {
		return fmt.Errorf("ошибка открытия базы данных: %w", err)
	}

	// Проверка соединения
	if err = s.db.Ping(); err != nil {
		return fmt.Errorf("ошибка подключения к базе данных: %w", err)
	}

	// Создание таблиц
	if err = s.createTables(); err != nil {
		return fmt.Errorf("ошибка создания таблиц: %w", err)
	}

	return nil
}

// createTables создает необходимые таблицы в базе данных
func (s *Service) createTables() error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS backup_policies (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			source_path TEXT NOT NULL,
			destination_path TEXT NOT NULL,
			schedule_cron TEXT,
			retention_count INTEGER DEFAULT 1,
			archive_enabled BOOLEAN DEFAULT true,
			encryption_enabled BOOLEAN DEFAULT false,
			encryption_password TEXT,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
		)`,

		`CREATE TABLE IF NOT EXISTS backup_jobs (
			id TEXT PRIMARY KEY,
			policy_id TEXT NOT NULL,
			status TEXT NOT NULL,
			started_at DATETIME,
			completed_at DATETIME,
			error TEXT,
			files_processed INTEGER DEFAULT 0,
			total_size INTEGER DEFAULT 0,
			backup_path TEXT,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (policy_id) REFERENCES backup_policies(id)
		)`,

		`CREATE TABLE IF NOT EXISTS backup_results (
			id TEXT PRIMARY KEY,
			job_id TEXT NOT NULL,
			backup_path TEXT NOT NULL,
			files_processed INTEGER NOT NULL,
			total_size INTEGER NOT NULL,
			compressed_size INTEGER DEFAULT 0,
			compression_ratio REAL DEFAULT 0,
			encrypted BOOLEAN DEFAULT false,
			compressed BOOLEAN DEFAULT false,
			checksum TEXT,
			duration_seconds INTEGER DEFAULT 0,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (job_id) REFERENCES backup_jobs(id)
		)`,

		`CREATE TABLE IF NOT EXISTS backup_files (
			id TEXT PRIMARY KEY,
			job_id TEXT NOT NULL,
			file_path TEXT NOT NULL,
			relative_path TEXT NOT NULL,
			file_size INTEGER NOT NULL,
			checksum TEXT,
			processed BOOLEAN DEFAULT false,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (job_id) REFERENCES backup_jobs(id)
		)`,

		`CREATE INDEX IF NOT EXISTS idx_backup_policies_name ON backup_policies(name)`,
		`CREATE INDEX IF NOT EXISTS idx_backup_jobs_policy_id ON backup_jobs(policy_id)`,
		`CREATE INDEX IF NOT EXISTS idx_backup_jobs_status ON backup_jobs(status)`,
		`CREATE INDEX IF NOT EXISTS idx_backup_results_job_id ON backup_results(job_id)`,
		`CREATE INDEX IF NOT EXISTS idx_backup_files_job_id ON backup_files(job_id)`,
	}

	for _, query := range queries {
		if _, err := s.db.Exec(query); err != nil {
			return fmt.Errorf("ошибка выполнения запроса: %w", err)
		}
	}

	return nil
}

// savePolicy сохраняет политику бэкапа в базу данных
func (s *Service) savePolicy(ctx context.Context, policy *types.BackupPolicy) error {
	query := `
		INSERT OR REPLACE INTO backup_policies (
			id, name, source_path, destination_path, schedule_cron,
			retention_count, archive_enabled, encryption_enabled, encryption_password,
			updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)`

	_, err := s.db.ExecContext(ctx, query,
		policy.ID,
		policy.Name,
		policy.SourcePath,
		policy.DestinationPath,
		policy.Schedule,
		policy.RetentionCount,
		policy.ArchiveEnabled,
		policy.EncryptionEnabled,
		policy.EncryptionPassword,
	)

	if err != nil {
		return fmt.Errorf("ошибка сохранения политики: %w", err)
	}

	s.logger.InfoContext(ctx, "Политика бэкапа сохранена",
		"policy_id", policy.ID,
		"name", policy.Name)

	return nil
}

// getPolicy получает политику бэкапа из базы данных
func (s *Service) getPolicy(ctx context.Context, policyID string) (*types.BackupPolicy, error) {
	query := `
		SELECT id, name, source_path, destination_path, schedule_cron,
			   retention_count, archive_enabled, encryption_enabled, encryption_password,
			   created_at, updated_at
		FROM backup_policies 
		WHERE id = ?`

	row := s.db.QueryRowContext(ctx, query, policyID)

	policy := &types.BackupPolicy{}
	var createdAt, updatedAt time.Time

	err := row.Scan(
		&policy.ID,
		&policy.Name,
		&policy.SourcePath,
		&policy.DestinationPath,
		&policy.Schedule,
		&policy.RetentionCount,
		&policy.ArchiveEnabled,
		&policy.EncryptionEnabled,
		&policy.EncryptionPassword,
		&createdAt,
		&updatedAt,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("политика с ID %s не найдена", policyID)
		}
		return nil, fmt.Errorf("ошибка получения политики: %w", err)
	}

	policy.CreatedAt = createdAt
	policy.UpdatedAt = updatedAt

	return policy, nil
}

// saveBackupJob сохраняет задачу бэкапа в базу данных
func (s *Service) saveBackupJob(ctx context.Context, job *types.BackupJob) error {
	query := `
		INSERT OR REPLACE INTO backup_jobs (
			id, policy_id, status, started_at, completed_at, error,
			files_processed, total_size, backup_path
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`

	_, err := s.db.ExecContext(ctx, query,
		job.ID,
		job.PolicyID,
		job.Status,
		job.StartedAt,
		job.CompletedAt,
		job.Error,
		job.FilesProcessed,
		job.TotalSize,
		job.BackupPath,
	)

	if err != nil {
		return fmt.Errorf("ошибка сохранения задачи бэкапа: %w", err)
	}

	return nil
}

// saveBackupResult сохраняет результат бэкапа в базу данных
func (s *Service) saveBackupResult(ctx context.Context, result *types.BackupResult) error {
	query := `
		INSERT INTO backup_results (
			id, job_id, backup_path, files_processed, total_size,
			compressed_size, compression_ratio, encrypted, compressed,
			checksum, duration_seconds
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	resultID := fmt.Sprintf("result_%s", result.JobID)
	durationSeconds := int64(result.Duration.Seconds())

	_, err := s.db.ExecContext(ctx, query,
		resultID,
		result.JobID,
		result.BackupPath,
		result.FilesProcessed,
		result.TotalSize,
		result.CompressedSize,
		result.CompressionRatio,
		result.Encrypted,
		result.Compressed,
		result.Checksum,
		durationSeconds,
	)

	if err != nil {
		return fmt.Errorf("ошибка сохранения результата бэкапа: %w", err)
	}

	return nil
}

// getBackupHistory получает историю бэкапов для политики
func (s *Service) getBackupHistory(ctx context.Context, policyID string, limit int) ([]*types.BackupJob, error) {
	query := `
		SELECT id, policy_id, status, started_at, completed_at, error,
			   files_processed, total_size, backup_path, created_at
		FROM backup_jobs 
		WHERE policy_id = ? 
		ORDER BY created_at DESC 
		LIMIT ?`

	rows, err := s.db.QueryContext(ctx, query, policyID, limit)
	if err != nil {
		return nil, fmt.Errorf("ошибка получения истории бэкапов: %w", err)
	}
	defer rows.Close()

	var jobs []*types.BackupJob
	for rows.Next() {
		job := &types.BackupJob{}
		var createdAt time.Time

		err := rows.Scan(
			&job.ID,
			&job.PolicyID,
			&job.Status,
			&job.StartedAt,
			&job.CompletedAt,
			&job.Error,
			&job.FilesProcessed,
			&job.TotalSize,
			&job.BackupPath,
			&createdAt,
		)
		if err != nil {
			return nil, fmt.Errorf("ошибка сканирования строки: %w", err)
		}

		job.CreatedAt = createdAt
		jobs = append(jobs, job)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("ошибка итерации по строкам: %w", err)
	}

	return jobs, nil
}

// getAllPolicies получает все политики бэкапа
func (s *Service) getAllPolicies(ctx context.Context) ([]*types.BackupPolicy, error) {
	query := `
		SELECT id, name, source_path, destination_path, schedule_cron,
			   retention_count, archive_enabled, encryption_enabled, 
			   created_at, updated_at
		FROM backup_policies 
		ORDER BY created_at DESC`

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("ошибка получения политик: %w", err)
	}
	defer rows.Close()

	var policies []*types.BackupPolicy
	for rows.Next() {
		policy := &types.BackupPolicy{}

		err := rows.Scan(
			&policy.ID,
			&policy.Name,
			&policy.SourcePath,
			&policy.DestinationPath,
			&policy.Schedule,
			&policy.RetentionCount,
			&policy.ArchiveEnabled,
			&policy.EncryptionEnabled,
			&policy.CreatedAt,
			&policy.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("ошибка сканирования политики: %w", err)
		}

		policies = append(policies, policy)
	}

	return policies, nil
}

// deletePolicy удаляет политику бэкапа
func (s *Service) deletePolicy(ctx context.Context, policyID string) error {
	// Начинаем транзакцию
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("ошибка начала транзакции: %w", err)
	}
	defer tx.Rollback()

	// Удаляем связанные файлы бэкапов
	_, err = tx.ExecContext(ctx, "DELETE FROM backup_files WHERE job_id IN (SELECT id FROM backup_jobs WHERE policy_id = ?)", policyID)
	if err != nil {
		return fmt.Errorf("ошибка удаления файлов бэкапов: %w", err)
	}

	// Удаляем результаты бэкапов
	_, err = tx.ExecContext(ctx, "DELETE FROM backup_results WHERE job_id IN (SELECT id FROM backup_jobs WHERE policy_id = ?)", policyID)
	if err != nil {
		return fmt.Errorf("ошибка удаления результатов бэкапов: %w", err)
	}

	// Удаляем задачи бэкапов
	_, err = tx.ExecContext(ctx, "DELETE FROM backup_jobs WHERE policy_id = ?", policyID)
	if err != nil {
		return fmt.Errorf("ошибка удаления задач бэкапов: %w", err)
	}

	// Удаляем политику
	_, err = tx.ExecContext(ctx, "DELETE FROM backup_policies WHERE id = ?", policyID)
	if err != nil {
		return fmt.Errorf("ошибка удаления политики: %w", err)
	}

	// Подтверждаем транзакцию
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("ошибка подтверждения транзакции: %w", err)
	}

	s.logger.InfoContext(ctx, "Политика бэкапа удалена", "policy_id", policyID)
	return nil
}
