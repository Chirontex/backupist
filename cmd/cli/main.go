package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"backupist/internal/core/backup"
	"backupist/internal/core/config"
	"backupist/internal/logger"
	"backupist/pkg/types"

	"github.com/adhocore/gronx"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
)

var (
	// Параметры командной строки
	cfgFile         string
	sourcePath      string
	destinationPath string
	schedule        string
	retentionCount  int
	archiveEnabled  bool
	encryptEnabled  bool
	encryptPassword string
	policyName      string
)

// Корневая команда
var rootCmd = &cobra.Command{
	Use:   "backupist",
	Short: "Backupist - надежный инструмент для создания бэкапов",
	Long: `Backupist - приложение для создания бэкапов файлов и директорий.
Поддерживает различные типы хранилищ, шифрование и планирование.`,
}

// Команда для создания задачи бэкапа
var createCmd = &cobra.Command{
	Use:   "create",
	Short: "Создать задачу резервного копирования",
	Long: `Создает новую задачу резервного копирования с указанными параметрами.

Пример использования:
  backupist create --source /home/user/documents --destination /backups
  backupist create -s /data -d s3://my-bucket/backups --schedule "0 2 * * *" --encrypt`,
	PreRunE: validateCreateFlags,
	RunE:    runCreate,
}

func init() {
	cobra.OnInitialize(initConfig)

	// Глобальные флаги
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "путь к файлу конфигурации")

	// Флаги команды create
	createCmd.Flags().StringVarP(&sourcePath, "source", "s", "", "путь к исходным файлам для бэкапа (обязательный)")
	createCmd.Flags().StringVarP(&destinationPath, "destination", "d", "", "путь для сохранения бэкапа (обязательный)")
	createCmd.Flags().StringVarP(&schedule, "schedule", "", "", "cron-расписание для создания бэкапа (необязательно)")
	createCmd.Flags().IntVarP(&retentionCount, "retention", "r", 1, "количество версий для хранения (по умолчанию 1)")
	createCmd.Flags().BoolVarP(&archiveEnabled, "archive", "a", true, "архивировать бэкап (по умолчанию true)")
	createCmd.Flags().BoolVarP(&encryptEnabled, "encrypt", "e", false, "шифровать бэкап (по умолчанию false)")
	createCmd.Flags().StringVarP(&encryptPassword, "password", "p", "", "пароль для шифрования (обязателен, если --encrypt=true)")
	createCmd.Flags().StringVarP(&policyName, "name", "n", "", "имя политики бэкапа (по умолчанию генерируется автоматически)")

	// Обязательные флаги
	createCmd.MarkFlagRequired("source")
	createCmd.MarkFlagRequired("destination")

	// Добавляем команду create к корневой команде
	rootCmd.AddCommand(createCmd)
}

// initConfig читает конфигурационный файл
func initConfig() {
	// Будет использовано в команде
}

// validateCreateFlags проверяет флаги команды create
func validateCreateFlags(cmd *cobra.Command, args []string) error {
	// Проверяем исходный путь
	if _, err := os.Stat(sourcePath); os.IsNotExist(err) {
		return fmt.Errorf("исходный путь не существует: %s", sourcePath)
	}

	// Проверяем расписание
	if schedule != "" {
		gron := gronx.New()
		if !gron.IsValid(schedule) {
			return fmt.Errorf("неверный формат cron-выражения: %s", schedule)
		}
	}

	// Проверяем шифрование
	if encryptEnabled && encryptPassword == "" {
		return fmt.Errorf("для шифрования необходимо указать пароль (--password)")
	}

	// Проверка retention count
	if retentionCount < 1 {
		return fmt.Errorf("количество версий должно быть не менее 1")
	}

	return nil
}

// runCreate выполняет команду create
func runCreate(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Обработка сигналов для graceful shutdown
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-signalCh
		fmt.Println("\nПолучен сигнал прерывания, завершаем работу...")
		cancel()
	}()

	// Загрузка конфигурации
	cfg, err := config.LoadConfig(cfgFile)
	if err != nil {
		return fmt.Errorf("ошибка загрузки конфигурации: %w", err)
	}

	// Создание логгера
	log := logger.NewStructuredLogger("main")

	// Инициализация сервиса бэкапа
	service := backup.NewService(cfg, log)
	if err := service.Initialize(ctx); err != nil {
		return fmt.Errorf("ошибка инициализации сервиса: %w", err)
	}

	// Создание политики бэкапа
	policy := &types.BackupPolicy{
		ID:                 uuid.New().String(),
		Name:               policyName,
		SourcePath:         sourcePath,
		DestinationPath:    destinationPath,
		Schedule:           schedule,
		RetentionCount:     retentionCount,
		ArchiveEnabled:     archiveEnabled,
		EncryptionEnabled:  encryptEnabled,
		EncryptionPassword: encryptPassword,
		CreatedAt:          time.Now(),
		UpdatedAt:          time.Now(),
	}

	// Если имя не указано, генерируем его на основе исходного пути
	if policy.Name == "" {
		policy.Name = fmt.Sprintf("backup-%s", filepath.Base(sourcePath))
	}

	// Создание задачи бэкапа
	job, err := service.CreateBackupJob(ctx, policy)
	if err != nil {
		return fmt.Errorf("ошибка создания задачи бэкапа: %w", err)
	}

	fmt.Printf("Создана задача бэкапа: %s\n", job.ID)
	fmt.Printf("Политика: %s (%s)\n", policy.Name, policy.ID)
	fmt.Printf("Источник: %s\n", policy.SourcePath)
	fmt.Printf("Назначение: %s\n", policy.DestinationPath)

	// Если расписание указано, выводим информацию о нем
	if policy.Schedule != "" {
		fmt.Printf("Расписание: %s\n", policy.Schedule)
	} else {
		fmt.Println("Расписание: не указано (ручной запуск)")
	}

	fmt.Printf("Хранить версий: %d\n", policy.RetentionCount)
	fmt.Printf("Архивация: %v\n", policy.ArchiveEnabled)
	fmt.Printf("Шифрование: %v\n", policy.EncryptionEnabled)

	// Запуск бэкапа
	fmt.Println("\nЗапуск процесса бэкапа...")
	result, err := service.ExecuteBackup(ctx, job)
	if err != nil {
		return fmt.Errorf("ошибка выполнения бэкапа: %w", err)
	}

	// Вывод результатов
	fmt.Println("\nБэкап успешно завершен:")
	fmt.Printf("Путь к бэкапу: %s\n", result.BackupPath)
	fmt.Printf("Обработано файлов: %d\n", result.FilesProcessed)
	fmt.Printf("Общий размер: %d байт (%.2f МБ)\n", result.TotalSize, float64(result.TotalSize)/1024/1024)
	fmt.Printf("Длительность: %s\n", result.Duration.String())

	if result.Compressed {
		fmt.Printf("Размер после сжатия: %d байт (%.2f МБ)\n", result.CompressedSize, float64(result.CompressedSize)/1024/1024)
		fmt.Printf("Коэффициент сжатия: %.2f\n", result.CompressionRatio)
	}

	if result.Encrypted {
		fmt.Println("Бэкап зашифрован.")
	}

	fmt.Printf("Контрольная сумма: %s\n", result.Checksum)

	return nil
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
