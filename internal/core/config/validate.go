package config

import (
	"backupist/pkg/types"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/adhocore/gronx"
	"github.com/go-playground/validator"
)

var validate *validator.Validate

func init() {
	validate = validator.New()

	// Регистрация кастомных валидаторов
	validate.RegisterValidation("cron", validateCron)
	validate.RegisterValidation("dir", validateDirectory)
	validate.RegisterValidation("storage_type", validateStorageType)
}

// ValidateBackupPolicy валидирует политику бэкапа
func ValidateBackupPolicy(policy *types.BackupPolicy) error {
	if err := validate.Struct(policy); err != nil {
		return formatValidationError(err)
	}
	return nil
}

// validateCron валидирует cron-выражение
func validateCron(fl validator.FieldLevel) bool {
	cronExpr := fl.Field().String()
	if cronExpr == "" {
		return true // Пустое значение допустимо
	}

	gron := gronx.New()
	return gron.IsValid(cronExpr)
}

// validateDirectory проверяет существование директории
func validateDirectory(fl validator.FieldLevel) bool {
	path := fl.Field().String()
	if path == "" {
		return false
	}

	info, err := os.Stat(path)
	if err != nil {
		return false
	}

	return info.IsDir()
}

// validateStorageType проверяет тип хранилища
func validateStorageType(fl validator.FieldLevel) bool {
	storageType := fl.Field().String()
	validTypes := []string{"local", "s3", "gcs"}

	for _, validType := range validTypes {
		if storageType == validType {
			return true
		}
	}
	return false
}

// formatValidationError форматирует ошибки валидации в понятный вид
func formatValidationError(err error) error {
	var messages []string

	if validationErrors, ok := err.(validator.ValidationErrors); ok {
		for _, e := range validationErrors {
			message := formatFieldError(e)
			messages = append(messages, message)
		}
	}

	if len(messages) > 0 {
		return fmt.Errorf("ошибки валидации: %s", strings.Join(messages, "; "))
	}

	return err
}

// formatFieldError форматирует ошибку конкретного поля
func formatFieldError(e validator.FieldError) string {
	field := e.Field()
	tag := e.Tag()

	switch tag {
	case "required":
		return fmt.Sprintf("поле '%s' обязательно для заполнения", field)
	case "min":
		return fmt.Sprintf("поле '%s' должно содержать минимум %s символов/элементов", field, e.Param())
	case "max":
		return fmt.Sprintf("поле '%s' может содержать максимум %s символов/элементов", field, e.Param())
	case "cron":
		return fmt.Sprintf("поле '%s' содержит некорректное cron-выражение", field)
	case "dir":
		return fmt.Sprintf("поле '%s' должно содержать путь к существующей директории", field)
	case "storage_type":
		return fmt.Sprintf("поле '%s' содержит неподдерживаемый тип хранилища", field)
	default:
		return fmt.Sprintf("поле '%s' не прошло валидацию '%s'", field, tag)
	}
}

// ValidateS3Config валидирует конфигурацию S3
func ValidateS3Config(config *types.S3Config) error {
	if config == nil {
		return fmt.Errorf("конфигурация S3 не может быть пустой")
	}

	if config.Bucket == "" {
		return fmt.Errorf("имя bucket S3 обязательно")
	}

	if config.Region == "" {
		return fmt.Errorf("регион S3 обязателен")
	}

	if config.AccessKeyID == "" {
		return fmt.Errorf("Access Key ID обязателен")
	}

	if config.SecretAccessKey == "" {
		return fmt.Errorf("Secret Access Key обязателен")
	}

	// Валидация имени bucket
	if !isValidS3BucketName(config.Bucket) {
		return fmt.Errorf("некорректное имя bucket S3")
	}

	return nil
}

// ValidateGCSConfig валидирует конфигурацию Google Cloud Storage
func ValidateGCSConfig(config *types.GCSConfig) error {
	if config == nil {
		return fmt.Errorf("конфигурация GCS не может быть пустой")
	}

	if config.Bucket == "" {
		return fmt.Errorf("имя bucket GCS обязательно")
	}

	if config.ProjectID == "" {
		return fmt.Errorf("Project ID обязателен")
	}

	if config.CredentialsPath == "" && config.ServiceAccountJSON == "" {
		return fmt.Errorf("необходимо указать путь к файлу credentials или JSON ключ")
	}

	if config.CredentialsPath != "" {
		if _, err := os.Stat(config.CredentialsPath); os.IsNotExist(err) {
			return fmt.Errorf("файл credentials не найден: %s", config.CredentialsPath)
		}
	}

	return nil
}

// isValidS3BucketName проверяет корректность имени S3 bucket
func isValidS3BucketName(bucket string) bool {
	// Основные правила для имен S3 bucket
	if len(bucket) < 3 || len(bucket) > 63 {
		return false
	}

	// Имя должно состоять только из строчных букв, цифр, точек и дефисов
	matched, _ := regexp.MatchString(`^[a-z0-9.-]+$`, bucket)
	if !matched {
		return false
	}

	// Не должно начинаться или заканчиваться точкой или дефисом
	if strings.HasPrefix(bucket, ".") || strings.HasSuffix(bucket, ".") ||
		strings.HasPrefix(bucket, "-") || strings.HasSuffix(bucket, "-") {
		return false
	}

	return true
}
