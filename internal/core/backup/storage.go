package backup

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// initStorage инициализирует провайдер хранилища на основе конфигурации
func (s *Service) initStorage() error {
	switch s.config.Storage.Type {
	case "local":
		s.storage = NewLocalStorage(s.config.Storage.LocalPath)
	case "s3":
		client, err := NewS3Storage(
			s.config.Storage.S3Endpoint,
			s.config.Storage.S3AccessKey,
			s.config.Storage.S3SecretKey,
			s.config.Storage.S3BucketName,
			s.config.Storage.S3UseSSL,
		)
		if err != nil {
			return fmt.Errorf("ошибка инициализации S3 хранилища: %w", err)
		}
		s.storage = client
	case "gcs":
		client, err := NewGCSStorage(
			s.config.Storage.GCSBucketName,
			s.config.Storage.GCSCredentialsPath,
		)
		if err != nil {
			return fmt.Errorf("ошибка инициализации GCS хранилища: %w", err)
		}
		s.storage = client
	default:
		return fmt.Errorf("неподдерживаемый тип хранилища: %s", s.config.Storage.Type)
	}

	return nil
}

// LocalStorage реализация локального хранилища
type LocalStorage struct {
	basePath string
}

// NewLocalStorage создает новый экземпляр локального хранилища
func NewLocalStorage(basePath string) *LocalStorage {
	return &LocalStorage{
		basePath: basePath,
	}
}

// Upload загружает файл в локальное хранилище
func (ls *LocalStorage) Upload(ctx context.Context, localPath, remotePath string) error {
	fullPath := filepath.Join(ls.basePath, remotePath)

	// Создаем директорию если она не существует
	if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
		return fmt.Errorf("ошибка создания директории: %w", err)
	}

	// Открываем источник
	src, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("ошибка открытия исходного файла: %w", err)
	}
	defer src.Close()

	// Создаем целевой файл
	dst, err := os.Create(fullPath)
	if err != nil {
		return fmt.Errorf("ошибка создания целевого файла: %w", err)
	}
	defer dst.Close()

	// Копируем данные
	_, err = io.Copy(dst, src)
	if err != nil {
		return fmt.Errorf("ошибка копирования файла: %w", err)
	}

	return nil
}

// Download скачивает файл из локального хранилища
func (ls *LocalStorage) Download(ctx context.Context, remotePath, localPath string) error {
	fullPath := filepath.Join(ls.basePath, remotePath)

	// Создаем директорию для локального файла
	if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
		return fmt.Errorf("ошибка создания локальной директории: %w", err)
	}

	// Открываем источник
	src, err := os.Open(fullPath)
	if err != nil {
		return fmt.Errorf("ошибка открытия файла в хранилище: %w", err)
	}
	defer src.Close()

	// Создаем локальный файл
	dst, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("ошибка создания локального файла: %w", err)
	}
	defer dst.Close()

	// Копируем данные
	_, err = io.Copy(dst, src)
	if err != nil {
		return fmt.Errorf("ошибка копирования файла: %w", err)
	}

	return nil
}

// Delete удаляет файл из локального хранилища
func (ls *LocalStorage) Delete(ctx context.Context, remotePath string) error {
	fullPath := filepath.Join(ls.basePath, remotePath)
	return os.Remove(fullPath)
}

// List возвращает список файлов в директории
func (ls *LocalStorage) List(ctx context.Context, prefix string) ([]string, error) {
	fullPath := filepath.Join(ls.basePath, prefix)

	var files []string
	err := filepath.Walk(fullPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() {
			// Получаем относительный путь
			relPath, err := filepath.Rel(ls.basePath, path)
			if err != nil {
				return err
			}
			files = append(files, relPath)
		}

		return nil
	})

	return files, err
}

func (ls *LocalStorage) Exists(ctx context.Context, remotePath string) (bool, error) {
	fullPath := filepath.Join(ls.basePath, remotePath)
	_, err := os.Stat(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("ошибка проверки существования файла: %w", err)
	}
	return true, nil
}

// S3Storage реализация S3-совместимого хранилища
type S3Storage struct {
	client     *minio.Client
	bucketName string
}

// NewS3Storage создает новый экземпляр S3 хранилища
func NewS3Storage(endpoint, accessKey, secretKey, bucketName string, useSSL bool) (*S3Storage, error) {
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("ошибка создания MinIO клиента: %w", err)
	}

	return &S3Storage{
		client:     client,
		bucketName: bucketName,
	}, nil
}

// Upload загружает файл в S3
func (s3 *S3Storage) Upload(ctx context.Context, localPath, remotePath string) error {
	_, err := s3.client.FPutObject(ctx, s3.bucketName, remotePath, localPath, minio.PutObjectOptions{
		ContentType: "application/octet-stream",
	})
	if err != nil {
		return fmt.Errorf("ошибка загрузки файла в S3: %w", err)
	}

	return nil
}

// Download скачивает файл из S3
func (s3 *S3Storage) Download(ctx context.Context, remotePath, localPath string) error {
	// Создаем директорию для локального файла
	if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
		return fmt.Errorf("ошибка создания локальной директории: %w", err)
	}

	return s3.client.FGetObject(ctx, s3.bucketName, remotePath, localPath, minio.GetObjectOptions{})
}

// Delete удаляет файл из S3
func (s3 *S3Storage) Delete(ctx context.Context, remotePath string) error {
	return s3.client.RemoveObject(ctx, s3.bucketName, remotePath, minio.RemoveObjectOptions{})
}

// List возвращает список объектов в S3
func (s3 *S3Storage) List(ctx context.Context, prefix string) ([]string, error) {
	var objects []string

	objectCh := s3.client.ListObjects(ctx, s3.bucketName, minio.ListObjectsOptions{
		Prefix: prefix,
	})

	for object := range objectCh {
		if object.Err != nil {
			return nil, fmt.Errorf("ошибка получения списка объектов: %w", object.Err)
		}
		objects = append(objects, object.Key)
	}

	return objects, nil
}

func (s3 *S3Storage) Exists(ctx context.Context, remotePath string) (bool, error) {
	_, err := s3.client.StatObject(ctx, s3.bucketName, remotePath, minio.StatObjectOptions{})
	if err != nil {
		errResponse := minio.ToErrorResponse(err)
		if errResponse.Code == "NoSuchKey" {
			return false, nil
		}
		return false, fmt.Errorf("ошибка проверки существования объекта в S3: %w", err)
	}
	return true, nil
}

// GCSStorage реализация Google Cloud Storage
type GCSStorage struct {
	client     *storage.Client
	bucketName string
}

// NewGCSStorage создает новый экземпляр GCS хранилища
func NewGCSStorage(bucketName, credentialsPath string) (*GCSStorage, error) {
	ctx := context.Background()

	var client *storage.Client
	var err error

	if credentialsPath != "" {
		client, err = storage.NewClient(ctx, option.WithCredentialsFile(credentialsPath))
	} else {
		// Используем Application Default Credentials
		client, err = storage.NewClient(ctx)
	}

	if err != nil {
		return nil, fmt.Errorf("ошибка создания GCS клиента: %w", err)
	}

	return &GCSStorage{
		client:     client,
		bucketName: bucketName,
	}, nil
}

// Upload загружает файл в GCS
func (gcs *GCSStorage) Upload(ctx context.Context, localPath, remotePath string) error {
	// Открываем локальный файл
	file, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("ошибка открытия локального файла: %w", err)
	}
	defer file.Close()

	// Создаем writer для GCS объекта
	obj := gcs.client.Bucket(gcs.bucketName).Object(remotePath)
	writer := obj.NewWriter(ctx)
	defer writer.Close()

	// Копируем данные
	_, err = io.Copy(writer, file)
	if err != nil {
		return fmt.Errorf("ошибка загрузки файла в GCS: %w", err)
	}

	return nil
}

// Download скачивает файл из GCS
func (gcs *GCSStorage) Download(ctx context.Context, remotePath, localPath string) error {
	// Создаем директорию для локального файла
	if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
		return fmt.Errorf("ошибка создания локальной директории: %w", err)
	}

	// Открываем GCS объект для чтения
	obj := gcs.client.Bucket(gcs.bucketName).Object(remotePath)
	reader, err := obj.NewReader(ctx)
	if err != nil {
		return fmt.Errorf("ошибка открытия GCS объекта: %w", err)
	}
	defer reader.Close()

	// Создаем локальный файл
	file, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("ошибка создания локального файла: %w", err)
	}
	defer file.Close()

	// Копируем данные
	_, err = io.Copy(file, reader)
	if err != nil {
		return fmt.Errorf("ошибка скачивания файла из GCS: %w", err)
	}

	return nil
}

// Delete удаляет файл из GCS
func (gcs *GCSStorage) Delete(ctx context.Context, remotePath string) error {
	obj := gcs.client.Bucket(gcs.bucketName).Object(remotePath)
	return obj.Delete(ctx)
}

// List возвращает список объектов в GCS
func (gcs *GCSStorage) List(ctx context.Context, prefix string) ([]string, error) {
	var objects []string

	query := &storage.Query{Prefix: prefix}
	it := gcs.client.Bucket(gcs.bucketName).Objects(ctx, query)

	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("ошибка итерации по объектам GCS: %w", err)
		}

		objects = append(objects, attrs.Name)
	}

	return objects, nil
}

func (gcs *GCSStorage) Exists(ctx context.Context, remotePath string) (bool, error) {
	obj := gcs.client.Bucket(gcs.bucketName).Object(remotePath)
	_, err := obj.Attrs(ctx)
	if err != nil {
		if err == storage.ErrObjectNotExist {
			return false, nil
		}
		return false, fmt.Errorf("ошибка проверки существования объекта в GCS: %w", err)
	}
	return true, nil
}

// StorageFactory фабрика для создания провайдеров хранилища
type StorageFactory struct{}

// CreateStorage создает провайдер хранилища на основе URL
func (sf *StorageFactory) CreateStorage(storageURL string) (StorageProvider, error) {
	if strings.HasPrefix(storageURL, "s3://") {
		// Парсим S3 URL: s3://bucket/path
		parts := strings.SplitN(strings.TrimPrefix(storageURL, "s3://"), "/", 2)
		if len(parts) < 1 {
			return nil, fmt.Errorf("неверный формат S3 URL: %s", storageURL)
		}

		// Эти параметры должны браться из конфигурации
		return NewS3Storage("s3.amazonaws.com", "", "", parts[0], true)
	}

	if strings.HasPrefix(storageURL, "gcs://") {
		// Парсим GCS URL: gcs://bucket/path
		parts := strings.SplitN(strings.TrimPrefix(storageURL, "gcs://"), "/", 2)
		if len(parts) < 1 {
			return nil, fmt.Errorf("неверный формат GCS URL: %s", storageURL)
		}

		return NewGCSStorage(parts[0], "")
	}

	// Для локального пути
	return NewLocalStorage(storageURL), nil
}

// MultiStorage позволяет работать с несколькими хранилищами одновременно
type MultiStorage struct {
	storages []StorageProvider
}

// NewMultiStorage создает новый мульти-хранилище
func NewMultiStorage(storages ...StorageProvider) *MultiStorage {
	return &MultiStorage{
		storages: storages,
	}
}

// Upload загружает файл во все хранилища
func (ms *MultiStorage) Upload(ctx context.Context, localPath, remotePath string) error {
	for i, storage := range ms.storages {
		if err := storage.Upload(ctx, localPath, remotePath); err != nil {
			return fmt.Errorf("ошибка загрузки в хранилище %d: %w", i, err)
		}
	}
	return nil
}

// Download скачивает файл из первого доступного хранилища
func (ms *MultiStorage) Download(ctx context.Context, remotePath, localPath string) error {
	var lastErr error

	for i, storage := range ms.storages {
		if err := storage.Download(ctx, remotePath, localPath); err == nil {
			return nil
		} else {
			lastErr = fmt.Errorf("ошибка скачивания из хранилища %d: %w", i, err)
		}
	}

	return lastErr
}

// Delete удаляет файл из всех хранилищ
func (ms *MultiStorage) Delete(ctx context.Context, remotePath string) error {
	var errors []error

	for i, storage := range ms.storages {
		if err := storage.Delete(ctx, remotePath); err != nil {
			errors = append(errors, fmt.Errorf("ошибка удаления из хранилища %d: %w", i, err))
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("ошибки при удалении: %v", errors)
	}

	return nil
}

// List возвращает список файлов из первого хранилища
func (ms *MultiStorage) List(ctx context.Context, prefix string) ([]string, error) {
	if len(ms.storages) == 0 {
		return nil, fmt.Errorf("нет доступных хранилищ")
	}

	return ms.storages[0].List(ctx, prefix)
}
