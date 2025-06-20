package backup

import (
	"backupist/pkg/types"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// cleanupOldBackups удаляет старые бэкапы в соответствии с политикой хранения
func (s *Service) cleanupOldBackups(ctx context.Context, policy *types.BackupPolicy) error {
	// Пропускаем очистку, если политика хранения не ограничена
	if policy.RetentionCount <= 0 {
		s.logger.InfoContext(ctx, "Очистка пропущена: неограниченное хранение",
			"policy_id", policy.ID,
			"policy_name", policy.Name)
		return nil
	}

	// Получаем список существующих бэкапов для данной политики
	backups, err := s.getBackupsForPolicy(ctx, policy)
	if err != nil {
		return fmt.Errorf("ошибка получения списка бэкапов: %w", err)
	}

	// Если количество бэкапов не превышает RetentionCount, нет необходимости в очистке
	if len(backups) <= policy.RetentionCount {
		s.logger.InfoContext(ctx, "Очистка не требуется: количество бэкапов не превышает RetentionCount",
			"policy_id", policy.ID,
			"policy_name", policy.Name,
			"backups_count", len(backups),
			"retention_count", policy.RetentionCount)
		return nil
	}

	// Сортируем бэкапы по дате создания (от новых к старым)
	sort.Slice(backups, func(i, j int) bool {
		return backups[i].CreatedAt.After(backups[j].CreatedAt)
	})

	// Оставляем только RetentionCount последних бэкапов
	backupsToDelete := backups[policy.RetentionCount:]

	s.logger.InfoContext(ctx, "Начинаем очистку старых бэкапов",
		"policy_id", policy.ID,
		"policy_name", policy.Name,
		"total_backups", len(backups),
		"to_delete", len(backupsToDelete),
		"to_keep", policy.RetentionCount)

	// Удаляем лишние бэкапы
	for _, backup := range backupsToDelete {
		if err := s.deleteBackup(ctx, backup); err != nil {
			s.logger.WarnContext(ctx, "Ошибка удаления бэкапа",
				"backup_id", backup.ID,
				"backup_path", backup.BackupPath,
				"error", err.Error())
			// Продолжаем удаление остальных бэкапов
			continue
		}

		s.logger.InfoContext(ctx, "Бэкап удален",
			"backup_id", backup.ID,
			"backup_path", backup.BackupPath,
			"created_at", backup.CreatedAt.Format(time.RFC3339))
	}

	s.logger.InfoContext(ctx, "Очистка старых бэкапов завершена",
		"policy_id", policy.ID,
		"policy_name", policy.Name,
		"deleted_count", len(backupsToDelete))

	return nil
}

// getBackupsForPolicy получает список бэкапов для заданной политики
func (s *Service) getBackupsForPolicy(ctx context.Context, policy *types.BackupPolicy) ([]*types.BackupJob, error) {
	// Сначала получаем список из базы данных
	dbBackups, err := s.getBackupHistory(ctx, policy.ID, 1000) // Ограничиваем 1000 записями
	if err != nil {
		return nil, fmt.Errorf("ошибка получения истории бэкапов из БД: %w", err)
	}

	// Фильтруем только успешные бэкапы
	var successfulBackups []*types.BackupJob
	for _, backup := range dbBackups {
		if backup.Status == types.JobStatusCompleted && backup.BackupPath != "" {
			successfulBackups = append(successfulBackups, backup)
		}
	}

	// Проверяем наличие файлов бэкапов в хранилище
	var verifiedBackups []*types.BackupJob
	for _, backup := range successfulBackups {
		// Проверяем наличие файла в хранилище
		exists, err := s.backupExists(ctx, backup.BackupPath)
		if err != nil {
			s.logger.WarnContext(ctx, "Ошибка проверки наличия бэкапа",
				"backup_id", backup.ID,
				"backup_path", backup.BackupPath,
				"error", err.Error())
			continue
		}

		if exists {
			verifiedBackups = append(verifiedBackups, backup)
		} else {
			s.logger.WarnContext(ctx, "Бэкап не найден в хранилище",
				"backup_id", backup.ID,
				"backup_path", backup.BackupPath)
		}
	}

	return verifiedBackups, nil
}

// backupExists проверяет наличие файла бэкапа в хранилище
func (s *Service) backupExists(ctx context.Context, backupPath string) (bool, error) {
	return s.storage.Exists(ctx, backupPath)
}

// deleteBackup удаляет бэкап из хранилища и базы данных
func (s *Service) deleteBackup(ctx context.Context, backup *types.BackupJob) error {
	// Удаляем файл из хранилища
	if err := s.storage.Delete(ctx, backup.BackupPath); err != nil {
		return fmt.Errorf("ошибка удаления файла из хранилища: %w", err)
	}

	// Начинаем транзакцию
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("ошибка начала транзакции: %w", err)
	}
	defer tx.Rollback()

	// Удаляем записи о файлах бэкапа
	_, err = tx.ExecContext(ctx, "DELETE FROM backup_files WHERE job_id = ?", backup.ID)
	if err != nil {
		return fmt.Errorf("ошибка удаления записей о файлах: %w", err)
	}

	// Удаляем результат бэкапа
	_, err = tx.ExecContext(ctx, "DELETE FROM backup_results WHERE job_id = ?", backup.ID)
	if err != nil {
		return fmt.Errorf("ошибка удаления результата бэкапа: %w", err)
	}

	// Обновляем статус задачи бэкапа
	_, err = tx.ExecContext(ctx, "UPDATE backup_jobs SET status = 'deleted' WHERE id = ?", backup.ID)
	if err != nil {
		return fmt.Errorf("ошибка обновления статуса задачи: %w", err)
	}

	// Подтверждаем транзакцию
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("ошибка подтверждения транзакции: %w", err)
	}

	return nil
}

// applyRetentionPolicies применяет политики хранения ко всем бэкапам
func (s *Service) applyRetentionPolicies(ctx context.Context) error {
	// Получаем все политики
	policies, err := s.getAllPolicies(ctx)
	if err != nil {
		return fmt.Errorf("ошибка получения политик: %w", err)
	}

	s.logger.InfoContext(ctx, "Начинаем применение политик хранения",
		"policies_count", len(policies))

	// Применяем политику хранения к каждой политике
	for _, policy := range policies {
		if err := s.cleanupOldBackups(ctx, policy); err != nil {
			s.logger.WarnContext(ctx, "Ошибка применения политики хранения",
				"policy_id", policy.ID,
				"policy_name", policy.Name,
				"error", err.Error())
			// Продолжаем с другими политиками
			continue
		}
	}

	s.logger.InfoContext(ctx, "Применение политик хранения завершено",
		"policies_count", len(policies))

	return nil
}

// cleanupOrphanedBackups удаляет осиротевшие бэкапы (без политики)
func (s *Service) cleanupOrphanedBackups(ctx context.Context) error {
	// Получаем список осиротевших бэкапов
	query := `
		SELECT j.id, j.backup_path, j.created_at
		FROM backup_jobs j
		LEFT JOIN backup_policies p ON j.policy_id = p.id
		WHERE p.id IS NULL AND j.status = 'completed'
	`

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("ошибка получения осиротевших бэкапов: %w", err)
	}
	defer rows.Close()

	var orphanedBackups []*types.BackupJob
	for rows.Next() {
		backup := &types.BackupJob{}
		if err := rows.Scan(&backup.ID, &backup.BackupPath, &backup.CreatedAt); err != nil {
			return fmt.Errorf("ошибка сканирования результата: %w", err)
		}
		orphanedBackups = append(orphanedBackups, backup)
	}

	if err = rows.Err(); err != nil {
		return fmt.Errorf("ошибка обработки результатов: %w", err)
	}

	s.logger.InfoContext(ctx, "Начинаем очистку осиротевших бэкапов",
		"orphaned_count", len(orphanedBackups))

	// Удаляем осиротевшие бэкапы
	for _, backup := range orphanedBackups {
		if err := s.deleteBackup(ctx, backup); err != nil {
			s.logger.WarnContext(ctx, "Ошибка удаления осиротевшего бэкапа",
				"backup_id", backup.ID,
				"backup_path", backup.BackupPath,
				"error", err.Error())
			// Продолжаем с другими бэкапами
			continue
		}

		s.logger.InfoContext(ctx, "Осиротевший бэкап удален",
			"backup_id", backup.ID,
			"backup_path", backup.BackupPath)
	}

	s.logger.InfoContext(ctx, "Очистка осиротевших бэкапов завершена",
		"deleted_count", len(orphanedBackups))

	return nil
}

// cleanupFailedBackups удаляет старые неудачные бэкапы
func (s *Service) cleanupFailedBackups(ctx context.Context, olderThan time.Duration) error {
	cutoffTime := time.Now().Add(-olderThan)

	query := `
		SELECT id, policy_id, backup_path, error, created_at
		FROM backup_jobs
		WHERE status = 'failed' AND created_at < ?
	`

	rows, err := s.db.QueryContext(ctx, query, cutoffTime)
	if err != nil {
		return fmt.Errorf("ошибка получения неудачных бэкапов: %w", err)
	}
	defer rows.Close()

	var failedBackups []*types.BackupJob
	for rows.Next() {
		backup := &types.BackupJob{}
		if err := rows.Scan(&backup.ID, &backup.PolicyID, &backup.BackupPath, &backup.Error, &backup.CreatedAt); err != nil {
			return fmt.Errorf("ошибка сканирования результата: %w", err)
		}
		failedBackups = append(failedBackups, backup)
	}

	if err = rows.Err(); err != nil {
		return fmt.Errorf("ошибка обработки результатов: %w", err)
	}

	s.logger.InfoContext(ctx, "Начинаем очистку старых неудачных бэкапов",
		"failed_count", len(failedBackups),
		"older_than", olderThan.String())

	// Удаляем неудачные бэкапы
	for _, backup := range failedBackups {
		// Для неудачных бэкапов достаточно удалить записи из БД
		query := `
			DELETE FROM backup_jobs
			WHERE id = ?
		`
		if _, err := s.db.ExecContext(ctx, query, backup.ID); err != nil {
			s.logger.WarnContext(ctx, "Ошибка удаления неудачного бэкапа из БД",
				"backup_id", backup.ID,
				"error", err.Error())
			continue
		}

		// Если есть путь к файлу, пытаемся удалить его из хранилища
		if backup.BackupPath != "" {
			if err := s.storage.Delete(ctx, backup.BackupPath); err != nil {
				s.logger.WarnContext(ctx, "Ошибка удаления файла неудачного бэкапа",
					"backup_id", backup.ID,
					"backup_path", backup.BackupPath,
					"error", err.Error())
				// Продолжаем, так как запись из БД уже удалена
			}
		}

		s.logger.InfoContext(ctx, "Неудачный бэкап удален",
			"backup_id", backup.ID,
			"policy_id", backup.PolicyID,
			"created_at", backup.CreatedAt.Format(time.RFC3339))
	}

	s.logger.InfoContext(ctx, "Очистка старых неудачных бэкапов завершена",
		"deleted_count", len(failedBackups))

	return nil
}

// cleanupTempFiles удаляет временные файлы, созданные в процессе бэкапа
func (s *Service) cleanupTempFiles(ctx context.Context) error {
	// Получаем список временных директорий
	tempDirs, err := filepath.Glob(filepath.Join(os.TempDir(), "backup-*"))
	if err != nil {
		return fmt.Errorf("ошибка получения списка временных директорий: %w", err)
	}

	s.logger.InfoContext(ctx, "Начинаем очистку временных файлов",
		"temp_dirs_count", len(tempDirs))

	// Удаляем каждую временную директорию
	for _, dir := range tempDirs {
		fileInfo, err := os.Stat(dir)
		if err != nil {
			s.logger.WarnContext(ctx, "Ошибка получения информации о директории",
				"dir", dir,
				"error", err.Error())
			continue
		}

		// Пропускаем если это не директория
		if !fileInfo.IsDir() {
			continue
		}

		// Удаляем директорию рекурсивно
		if err := os.RemoveAll(dir); err != nil {
			s.logger.WarnContext(ctx, "Ошибка удаления временной директории",
				"dir", dir,
				"error", err.Error())
			continue
		}

		s.logger.InfoContext(ctx, "Временная директория удалена",
			"dir", dir,
			"mod_time", fileInfo.ModTime().Format(time.RFC3339))
	}

	s.logger.InfoContext(ctx, "Очистка временных файлов завершена")

	return nil
}

// applyAgeBasedRetention удаляет бэкапы старше указанного возраста
func (s *Service) applyAgeBasedRetention(ctx context.Context, policy *types.BackupPolicy, maxAge time.Duration) error {
	if maxAge <= 0 {
		return nil // Неограниченное хранение
	}

	cutoffTime := time.Now().Add(-maxAge)

	// Получаем список бэкапов
	backups, err := s.getBackupsForPolicy(ctx, policy)
	if err != nil {
		return fmt.Errorf("ошибка получения списка бэкапов: %w", err)
	}

	s.logger.InfoContext(ctx, "Начинаем применение возрастной политики хранения",
		"policy_id", policy.ID,
		"policy_name", policy.Name,
		"max_age", maxAge.String(),
		"cutoff_time", cutoffTime.Format(time.RFC3339))

	// Фильтруем бэкапы старше указанного возраста
	var backupsToDelete []*types.BackupJob
	for _, backup := range backups {
		if backup.CreatedAt.Before(cutoffTime) {
			backupsToDelete = append(backupsToDelete, backup)
		}
	}

	// Удаляем старые бэкапы
	for _, backup := range backupsToDelete {
		if err := s.deleteBackup(ctx, backup); err != nil {
			s.logger.WarnContext(ctx, "Ошибка удаления старого бэкапа",
				"backup_id", backup.ID,
				"backup_path", backup.BackupPath,
				"error", err.Error())
			// Продолжаем с другими бэкапами
			continue
		}

		s.logger.InfoContext(ctx, "Старый бэкап удален",
			"backup_id", backup.ID,
			"backup_path", backup.BackupPath,
			"created_at", backup.CreatedAt.Format(time.RFC3339),
			"age", time.Since(backup.CreatedAt).String())
	}

	s.logger.InfoContext(ctx, "Применение возрастной политики хранения завершено",
		"policy_id", policy.ID,
		"policy_name", policy.Name,
		"deleted_count", len(backupsToDelete))

	return nil
}
