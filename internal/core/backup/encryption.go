package backup

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/crypto/pbkdf2"
)

// encryptFile шифрует файл с использованием AES-256-GCM
func (s *Service) encryptFile(ctx context.Context, inputPath, outputPath, password string) error {
	// Генерируем ключ из пароля
	key := s.deriveKey(password)

	// Открываем входной файл
	inputFile, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("ошибка открытия входного файла: %w", err)
	}
	defer inputFile.Close()

	// Создаем выходной файл
	if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
		return fmt.Errorf("ошибка создания директории: %w", err)
	}

	outputFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("ошибка создания выходного файла: %w", err)
	}
	defer outputFile.Close()

	// Создаем AES cipher
	block, err := aes.NewCipher(key)
	if err != nil {
		return fmt.Errorf("ошибка создания AES cipher: %w", err)
	}

	// Создаем GCM mode
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return fmt.Errorf("ошибка создания GCM mode: %w", err)
	}

	// Генерируем случайный nonce
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return fmt.Errorf("ошибка генерации nonce: %w", err)
	}

	// Записываем nonce в начало файла
	if _, err := outputFile.Write(nonce); err != nil {
		return fmt.Errorf("ошибка записи nonce: %w", err)
	}

	// Шифруем файл блоками
	buffer := make([]byte, 64*1024) // 64KB блоки
	for {
		// Проверяем контекст на отмену
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		n, err := inputFile.Read(buffer)
		if err != nil && err != io.EOF {
			return fmt.Errorf("ошибка чтения входного файла: %w", err)
		}

		if n == 0 {
			break
		}

		// Шифруем блок
		ciphertext := gcm.Seal(nil, nonce, buffer[:n], nil)

		// Записываем зашифрованный блок
		if _, err := outputFile.Write(ciphertext); err != nil {
			return fmt.Errorf("ошибка записи зашифрованного блока: %w", err)
		}

		// Обновляем nonce для следующего блока (простое инкрементирование)
		s.incrementNonce(nonce)
	}

	s.logger.InfoContext(ctx, "Файл зашифрован успешно",
		"input", inputPath,
		"output", outputPath)

	return nil
}

// decryptFile расшифровывает файл
func (s *Service) decryptFile(ctx context.Context, inputPath, outputPath, password string) error {
	// Генерируем ключ из пароля
	key := s.deriveKey(password)

	// Открываем зашифрованный файл
	inputFile, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("ошибка открытия зашифрованного файла: %w", err)
	}
	defer inputFile.Close()

	// Создаем выходной файл
	if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
		return fmt.Errorf("ошибка создания директории: %w", err)
	}

	outputFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("ошибка создания выходного файла: %w", err)
	}
	defer outputFile.Close()

	// Создаем AES cipher
	block, err := aes.NewCipher(key)
	if err != nil {
		return fmt.Errorf("ошибка создания AES cipher: %w", err)
	}

	// Создаем GCM mode
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return fmt.Errorf("ошибка создания GCM mode: %w", err)
	}

	// Читаем nonce из начала файла
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(inputFile, nonce); err != nil {
		return fmt.Errorf("ошибка чтения nonce: %w", err)
	}

	// Расшифровываем файл блоками
	// Размер зашифрованного блока = размер исходного блока + размер tag (16 байт для GCM)
	encryptedBlockSize := 64*1024 + gcm.Overhead()
	buffer := make([]byte, encryptedBlockSize)

	for {
		// Проверяем контекст на отмену
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		n, err := inputFile.Read(buffer)
		if err != nil && err != io.EOF {
			return fmt.Errorf("ошибка чтения зашифрованного файла: %w", err)
		}

		if n == 0 {
			break
		}

		// Расшифровываем блок
		plaintext, err := gcm.Open(nil, nonce, buffer[:n], nil)
		if err != nil {
			return fmt.Errorf("ошибка расшифровки блока: %w", err)
		}

		// Записываем расшифрованный блок
		if _, err := outputFile.Write(plaintext); err != nil {
			return fmt.Errorf("ошибка записи расшифрованного блока: %w", err)
		}

		// Обновляем nonce для следующего блока
		s.incrementNonce(nonce)
	}

	s.logger.InfoContext(ctx, "Файл расшифрован успешно",
		"input", inputPath,
		"output", outputPath)

	return nil
}

// deriveKey создает ключ из пароля с использованием PBKDF2
func (s *Service) deriveKey(password string) []byte {
	// Используем статическую соль для простоты
	// В production среде следует использовать случайную соль и сохранять её
	salt := []byte("backupist-salt-2024")

	// Генерируем 32-байтовый ключ (256 бит) для AES-256
	return pbkdf2.Key([]byte(password), salt, 100000, 32, sha256.New)
}

// incrementNonce увеличивает nonce на 1 (для использования в блочном шифровании)
func (s *Service) incrementNonce(nonce []byte) {
	for i := len(nonce) - 1; i >= 0; i-- {
		nonce[i]++
		if nonce[i] != 0 {
			break
		}
	}
}

// encryptFileStream шифрует файл с использованием потокового шифрования
func (s *Service) encryptFileStream(ctx context.Context, inputPath, outputPath, password string) error {
	// Генерируем ключ из пароля
	key := s.deriveKey(password)

	// Открываем входной файл
	inputFile, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("ошибка открытия входного файла: %w", err)
	}
	defer inputFile.Close()

	// Создаем выходной файл
	if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
		return fmt.Errorf("ошибка создания директории: %w", err)
	}

	outputFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("ошибка создания выходного файла: %w", err)
	}
	defer outputFile.Close()

	// Создаем AES cipher
	block, err := aes.NewCipher(key)
	if err != nil {
		return fmt.Errorf("ошибка создания AES cipher: %w", err)
	}

	// Генерируем случайный IV
	iv := make([]byte, aes.BlockSize)
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return fmt.Errorf("ошибка генерации IV: %w", err)
	}

	// Записываем IV в начало файла
	if _, err := outputFile.Write(iv); err != nil {
		return fmt.Errorf("ошибка записи IV: %w", err)
	}

	// Создаем шифратор в режиме CFB
	stream := cipher.NewCFBEncrypter(block, iv)

	// Создаем StreamWriter для шифрования на лету
	writer := &cipher.StreamWriter{S: stream, W: outputFile}

	// Копируем и шифруем данные
	_, err = io.Copy(writer, inputFile)
	if err != nil {
		return fmt.Errorf("ошибка шифрования файла: %w", err)
	}

	s.logger.InfoContext(ctx, "Файл зашифрован потоково",
		"input", inputPath,
		"output", outputPath)

	return nil
}

// decryptFileStream расшифровывает файл с использованием потокового шифрования
func (s *Service) decryptFileStream(ctx context.Context, inputPath, outputPath, password string) error {
	// Генерируем ключ из пароля
	key := s.deriveKey(password)

	// Открываем зашифрованный файл
	inputFile, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("ошибка открытия зашифрованного файла: %w", err)
	}
	defer inputFile.Close()

	// Создаем выходной файл
	if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
		return fmt.Errorf("ошибка создания директории: %w", err)
	}

	outputFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("ошибка создания выходного файла: %w", err)
	}
	defer outputFile.Close()

	// Создаем AES cipher
	block, err := aes.NewCipher(key)
	if err != nil {
		return fmt.Errorf("ошибка создания AES cipher: %w", err)
	}

	// Читаем IV из начала файла
	iv := make([]byte, aes.BlockSize)
	if _, err := io.ReadFull(inputFile, iv); err != nil {
		return fmt.Errorf("ошибка чтения IV: %w", err)
	}

	// Создаем дешифратор в режиме CFB
	stream := cipher.NewCFBDecrypter(block, iv)

	// Создаем StreamReader для расшифровки на лету
	reader := &cipher.StreamReader{S: stream, R: inputFile}

	// Копируем и расшифровываем данные
	_, err = io.Copy(outputFile, reader)
	if err != nil {
		return fmt.Errorf("ошибка расшифровки файла: %w", err)
	}

	s.logger.InfoContext(ctx, "Файл расшифрован потоково",
		"input", inputPath,
		"output", outputPath)

	return nil
}

// generateSecurePassword генерирует криптографически стойкий пароль
func (s *Service) generateSecurePassword(length int) (string, error) {
	const charset = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!@#$%^&*"

	password := make([]byte, length)
	for i := range password {
		randomIndex := make([]byte, 1)
		if _, err := rand.Read(randomIndex); err != nil {
			return "", fmt.Errorf("ошибка генерации случайного числа: %w", err)
		}
		password[i] = charset[int(randomIndex[0])%len(charset)]
	}

	return string(password), nil
}

// validatePassword проверяет силу пароля
func (s *Service) validatePassword(password string) error {
	if len(password) < 8 {
		return fmt.Errorf("пароль должен содержать минимум 8 символов")
	}

	hasUpper := false
	hasLower := false
	hasDigit := false
	hasSpecial := false

	for _, char := range password {
		switch {
		case char >= 'A' && char <= 'Z':
			hasUpper = true
		case char >= 'a' && char <= 'z':
			hasLower = true
		case char >= '0' && char <= '9':
			hasDigit = true
		case char >= 32 && char <= 126:
			// Печатаемые ASCII символы, не буквы и не цифры
			if !((char >= 'A' && char <= 'Z') || (char >= 'a' && char <= 'z') || (char >= '0' && char <= '9')) {
				hasSpecial = true
			}
		}
	}

	if !hasUpper {
		return fmt.Errorf("пароль должен содержать заглавные буквы")
	}
	if !hasLower {
		return fmt.Errorf("пароль должен содержать строчные буквы")
	}
	if !hasDigit {
		return fmt.Errorf("пароль должен содержать цифры")
	}
	if !hasSpecial {
		return fmt.Errorf("пароль должен содержать специальные символы")
	}

	return nil
}

// EncryptionMetadata содержит метаданные о шифровании
type EncryptionMetadata struct {
	Algorithm     string `json:"algorithm"`
	KeyDerivation string `json:"key_derivation"`
	Salt          string `json:"salt"`
	Iterations    int    `json:"iterations"`
	Encrypted     bool   `json:"encrypted"`
}

// getEncryptionMetadata возвращает метаданные о шифровании файла
func (s *Service) getEncryptionMetadata(filePath string) (*EncryptionMetadata, error) {
	// Проверяем, является ли файл зашифрованным по расширению
	if !strings.HasSuffix(filePath, ".enc") {
		return &EncryptionMetadata{
			Encrypted: false,
		}, nil
	}

	return &EncryptionMetadata{
		Algorithm:     "AES-256-GCM",
		KeyDerivation: "PBKDF2",
		Salt:          "backupist-salt-2024",
		Iterations:    100000,
		Encrypted:     true,
	}, nil
}

// secureDelete безопасно удаляет файл (перезаписывает случайными данными)
func (s *Service) secureDelete(filePath string) error {
	// Получаем информацию о файле
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return fmt.Errorf("ошибка получения информации о файле: %w", err)
	}

	// Открываем файл для записи
	file, err := os.OpenFile(filePath, os.O_WRONLY, 0)
	if err != nil {
		return fmt.Errorf("ошибка открытия файла для перезаписи: %w", err)
	}
	defer file.Close()

	// Перезаписываем файл случайными данными
	fileSize := fileInfo.Size()
	buffer := make([]byte, 4096)

	for written := int64(0); written < fileSize; {
		// Генерируем случайные данные
		if _, err := rand.Read(buffer); err != nil {
			return fmt.Errorf("ошибка генерации случайных данных: %w", err)
		}

		// Определяем количество байт для записи
		toWrite := int64(len(buffer))
		if written+toWrite > fileSize {
			toWrite = fileSize - written
		}

		// Записываем случайные данные
		if _, err := file.Write(buffer[:toWrite]); err != nil {
			return fmt.Errorf("ошибка записи случайных данных: %w", err)
		}

		written += toWrite
	}

	// Синхронизируем данные с диском
	if err := file.Sync(); err != nil {
		return fmt.Errorf("ошибка синхронизации с диском: %w", err)
	}

	// Удаляем файл
	if err := os.Remove(filePath); err != nil {
		return fmt.Errorf("ошибка удаления файла: %w", err)
	}

	return nil
}
