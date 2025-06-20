package backup

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// createArchive создает tar.gz архив из директории
func (s *Service) createArchive(ctx context.Context, sourcePath, archivePath string) error {
	// Создаем выходной файл
	outFile, err := os.Create(archivePath)
	if err != nil {
		return fmt.Errorf("ошибка создания файла архива: %w", err)
	}
	defer outFile.Close()

	// Создаем gzip writer
	gzWriter := gzip.NewWriter(outFile)
	defer gzWriter.Close()

	// Создаем tar writer
	tarWriter := tar.NewWriter(gzWriter)
	defer tarWriter.Close()

	// Определяем базовую директорию для расчета относительных путей
	baseDir := filepath.Dir(sourcePath)

	// Рекурсивно добавляем файлы в архив
	err = filepath.Walk(sourcePath, func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			return fmt.Errorf("ошибка при обходе файла %s: %w", filePath, err)
		}

		// Проверяем контекст на отмену
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Пропускаем сам архив, если он находится внутри исходной директории
		if filePath == archivePath {
			return nil
		}

		// Создаем заголовок tar
		header, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return fmt.Errorf("ошибка создания заголовка tar для %s: %w", filePath, err)
		}

		// Вычисляем относительный путь
		relPath, err := filepath.Rel(baseDir, filePath)
		if err != nil {
			return fmt.Errorf("ошибка вычисления относительного пути: %w", err)
		}

		// Нормализуем путь для архива (используем прямые слеши)
		header.Name = filepath.ToSlash(relPath)

		// Записываем заголовок
		if err := tarWriter.WriteHeader(header); err != nil {
			return fmt.Errorf("ошибка записи заголовка tar: %w", err)
		}

		// Если это файл (не диркетория), записываем содержимое
		if !info.IsDir() {
			file, err := os.Open(filePath)
			if err != nil {
				return fmt.Errorf("ошибка открытия файла %s: %w", filePath, err)
			}
			defer file.Close()

			// Копируем содержимое файла в архив
			if _, err := io.Copy(tarWriter, file); err != nil {
				return fmt.Errorf("ошибка записи файла %s в архив: %w", filePath, err)
			}
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("ошибка создания архива: %w", err)
	}

	s.logger.InfoContext(ctx, "Архив создан успешно",
		"source", sourcePath,
		"archive", archivePath)

	return nil
}

// extractArchive извлекает tar.gz архив в указанную директорию
func (s *Service) extractArchive(ctx context.Context, archivePath, destPath string) error {
	// Открываем файл архива
	file, err := os.Open(archivePath)
	if err != nil {
		return fmt.Errorf("ошибка открытия архива: %w", err)
	}
	defer file.Close()

	// Создаем gzip reader
	gzReader, err := gzip.NewReader(file)
	if err != nil {
		return fmt.Errorf("ошибка создания gzip reader: %w", err)
	}
	defer gzReader.Close()

	// Создаем tar reader
	tarReader := tar.NewReader(gzReader)

	// Извлекаем файлы
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("ошибка чтения заголовка tar: %w", err)
		}

		// Проверяем контекст на отмену
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Создаем полный путь для извлечения
		fullPath := filepath.Join(destPath, header.Name)

		// Проверяем на попытку выхода за пределы целевой директории (zip slip)
		if !strings.HasPrefix(fullPath, filepath.Clean(destPath)+string(os.PathSeparator)) {
			return fmt.Errorf("небезопасный путь в архиве: %s", header.Name)
		}

		switch header.Typeflag {
		case tar.TypeDir:
			// Создаем директорию
			if err := os.MkdirAll(fullPath, os.FileMode(header.Mode)); err != nil {
				return fmt.Errorf("ошибка создания директории %s: %w", fullPath, err)
			}

		case tar.TypeReg:
			// Создаем директорию для файла
			if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
				return fmt.Errorf("ошибка создания директории для файла %s: %w", fullPath, err)
			}

			// Создаем файл
			outFile, err := os.OpenFile(fullPath, os.O_CREATE|os.O_WRONLY, os.FileMode(header.Mode))
			if err != nil {
				return fmt.Errorf("ошибка создания файла %s: %w", fullPath, err)
			}

			// Копируем содержимое
			if _, err := io.Copy(outFile, tarReader); err != nil {
				outFile.Close()
				return fmt.Errorf("ошибка записи файла %s: %w", fullPath, err)
			}
			outFile.Close()

		default:
			s.logger.WarnContext(ctx, "Неподдерживаемый тип файла в архиве",
				"type", header.Typeflag,
				"name", header.Name)
		}
	}

	s.logger.InfoContext(ctx, "Архив извлечен успешно",
		"archive", archivePath,
		"destination", destPath)

	return nil
}

// getDirectorySize вычисляет общий размер директории в байтах
func (s *Service) getDirectorySize(path string) (int64, error) {
	var totalSize int64

	err := filepath.Walk(path, func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			// Логируем ошибку, но продолжаем подсчет
			s.logger.Warn("Ошибка при обходе файла",
				"file", filePath,
				"error", err.Error())
			return nil
		}

		// Добавляем размер файла к общему размеру
		if !info.IsDir() {
			totalSize += info.Size()
		}

		return nil
	})

	if err != nil {
		return 0, fmt.Errorf("ошибка при вычислении размера директории: %w", err)
	}

	return totalSize, nil
}

// getFileSize возвращает размер файла в байтах
func (s *Service) getFileSize(filePath string) (int64, error) {
	info, err := os.Stat(filePath)
	if err != nil {
		return 0, fmt.Errorf("ошибка получения информации о файле: %w", err)
	}

	return info.Size(), nil
}

// getArchiveInfo возвращает информацию об архиве
func (s *Service) getArchiveInfo(archivePath string) (*ArchiveInfo, error) {
	info := &ArchiveInfo{
		Path: archivePath,
	}

	// Получаем размер архива
	fileInfo, err := os.Stat(archivePath)
	if err != nil {
		return nil, fmt.Errorf("ошибка получения информации об архиве: %w", err)
	}
	info.Size = fileInfo.Size()
	info.ModTime = fileInfo.ModTime()

	// Открываем архив для анализа содержимого
	file, err := os.Open(archivePath)
	if err != nil {
		return nil, fmt.Errorf("ошибка открытия архива: %w", err)
	}
	defer file.Close()

	// Создаем gzip reader
	gzReader, err := gzip.NewReader(file)
	if err != nil {
		return nil, fmt.Errorf("ошибка создания gzip reader: %w", err)
	}
	defer gzReader.Close()

	// Создаем tar reader
	tarReader := tar.NewReader(gzReader)

	// Анализируем содержимое
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("ошибка чтения заголовка tar: %w", err)
		}

		info.FileCount++
		info.UncompressedSize += header.Size

		if header.Typeflag == tar.TypeDir {
			info.DirectoryCount++
		}
	}

	// Вычисляем коэффициент сжатия
	if info.UncompressedSize > 0 {
		info.CompressionRatio = float64(info.UncompressedSize) / float64(info.Size)
	}

	return info, nil
}

// ArchiveInfo содержит информацию об архиве
type ArchiveInfo struct {
	Path             string    `json:"path"`
	Size             int64     `json:"size"`
	UncompressedSize int64     `json:"uncompressed_size"`
	CompressionRatio float64   `json:"compression_ratio"`
	FileCount        int       `json:"file_count"`
	DirectoryCount   int       `json:"directory_count"`
	ModTime          time.Time `json:"mod_time"`
}

// validateArchive проверяет целостность архива
func (s *Service) validateArchive(ctx context.Context, archivePath string) error {
	file, err := os.Open(archivePath)
	if err != nil {
		return fmt.Errorf("ошибка открытия архива: %w", err)
	}
	defer file.Close()

	// Проверяем gzip заголовок
	gzReader, err := gzip.NewReader(file)
	if err != nil {
		return fmt.Errorf("ошибка чтения gzip заголовка: %w", err)
	}
	defer gzReader.Close()

	// Проверяем tar содержимое
	tarReader := tar.NewReader(gzReader)

	fileCount := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("ошибка чтения tar заголовка: %w", err)
		}

		fileCount++

		// Проверяем, что можем прочитать содержимое файла
		if header.Typeflag == tar.TypeReg {
			// Читаем и отбрасываем содержимое для проверки целостности
			if _, err := io.Copy(io.Discard, tarReader); err != nil {
				return fmt.Errorf("ошибка чтения содержимого файла %s: %w", header.Name, err)
			}
		}
	}

	s.logger.InfoContext(ctx, "Архив валидирован успешно",
		"archive", archivePath,
		"files", fileCount)

	return nil
}

// createIncrementalArchive создает инкрементальный архив
func (s *Service) createIncrementalArchive(ctx context.Context, sourcePath, archivePath, baselinePath string) error {
	// Получаем время модификации базового архива
	baselineTime := time.Time{}
	if baselinePath != "" {
		if info, err := os.Stat(baselinePath); err == nil {
			baselineTime = info.ModTime()
		}
	}

	// Создаем выходной файл
	outFile, err := os.Create(archivePath)
	if err != nil {
		return fmt.Errorf("ошибка создания файла архива: %w", err)
	}
	defer outFile.Close()

	// Создаем gzip writer
	gzWriter := gzip.NewWriter(outFile)
	defer gzWriter.Close()

	// Создаем tar writer
	tarWriter := tar.NewWriter(gzWriter)
	defer tarWriter.Close()

	baseDir := filepath.Dir(sourcePath)
	addedFiles := 0

	// Добавляем только измененные файлы
	err = filepath.Walk(sourcePath, func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Пропускаем файлы, которые не изменились с момента создания базового архива
		if !baselineTime.IsZero() && info.ModTime().Before(baselineTime) {
			return nil
		}

		// Создаем заголовок tar
		header, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return fmt.Errorf("ошибка создания заголовка tar: %w", err)
		}

		relPath, err := filepath.Rel(baseDir, filePath)
		if err != nil {
			return fmt.Errorf("ошибка вычисления относительного пути: %w", err)
		}

		header.Name = filepath.ToSlash(relPath)

		if err := tarWriter.WriteHeader(header); err != nil {
			return fmt.Errorf("ошибка записи заголовка tar: %w", err)
		}

		if !info.IsDir() {
			file, err := os.Open(filePath)
			if err != nil {
				return fmt.Errorf("ошибка открытия файла: %w", err)
			}
			defer file.Close()

			if _, err := io.Copy(tarWriter, file); err != nil {
				return fmt.Errorf("ошибка записи файла в архив: %w", err)
			}

			addedFiles++
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("ошибка создания инкрементального архива: %w", err)
	}

	s.logger.InfoContext(ctx, "Инкрементальный архив создан",
		"archive", archivePath,
		"files_added", addedFiles,
		"baseline", baselinePath)

	return nil
}
