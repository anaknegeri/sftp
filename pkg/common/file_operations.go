package common

import (
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"jarvist/sftp-service/pkg/utils"
)

// FileOperations provides common file operations used across services
type FileOperations struct{}

// NewFileOperations creates a new instance of FileOperations
func NewFileOperations() *FileOperations {
	return &FileOperations{}
}

// CleanupFile removes a file and logs any errors
func (f *FileOperations) CleanupFile(filePath string) error {
	if err := os.Remove(filePath); err != nil {
		log.Printf("Warning: Failed to remove file %s: %v", filePath, err)
		return err
	}
	return nil
}

// MoveToFailedDirectory moves a file to the failed directory
func (f *FileOperations) MoveToFailedDirectory(filePath, fileName, failedPath string) error {
	failedFilePath := filepath.Join(failedPath, fileName)

	if err := os.Rename(filePath, failedFilePath); err != nil {
		log.Printf("Warning: Failed to move file %s to failed directory: %v", fileName, err)
		if copyErr := f.CopyFile(filePath, failedFilePath); copyErr != nil {
			log.Printf("Warning: Failed to copy file %s to failed directory: %v", fileName, copyErr)
			return copyErr
		}
	} else {
		log.Printf("Moved failed file %s to failed directory", fileName)
	}

	return nil
}

// CopyFile copies a file from src to dst
func (f *FileOperations) CopyFile(src, dst string) error {
	sourceFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	destFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, sourceFile)
	return err
}

// EnsureDirectory creates directory if it doesn't exist
func (f *FileOperations) EnsureDirectory(path string) error {
	return os.MkdirAll(path, 0755)
}

// CleanupExpiredFiles removes files older than specified duration
func (f *FileOperations) CleanupExpiredFiles(dirPath string, maxAge time.Duration) error {
	return filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() && time.Since(info.ModTime()) > maxAge {
			if removeErr := os.Remove(path); removeErr != nil {
				log.Printf("Warning: Failed to remove expired file %s: %v", path, removeErr)
			}
		}

		return nil
	})
}

// GetFileAge returns the age of a file
func (f *FileOperations) GetFileAge(filePath string) (time.Duration, error) {
	info, err := os.Stat(filePath)
	if err != nil {
		return 0, err
	}
	return time.Since(info.ModTime()), nil
}

// FileExists checks if a file exists
func (f *FileOperations) FileExists(filePath string) bool {
	_, err := os.Stat(filePath)
	return !os.IsNotExist(err)
}

// GetFileSize returns the size of a file
func (f *FileOperations) GetFileSize(filePath string) (int64, error) {
	info, err := os.Stat(filePath)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// ErrorHandler provides common error handling patterns
type ErrorHandler struct{}

// NewErrorHandler creates a new instance of ErrorHandler
func NewErrorHandler() *ErrorHandler {
	return &ErrorHandler{}
}

// IsConnectionError checks if error is connection-related using utils
func (e *ErrorHandler) IsConnectionError(err error) bool {
	return utils.IsConnectionError(err)
}

// IsTemporaryError checks if error might be temporary using utils
func (e *ErrorHandler) IsTemporaryError(err error) bool {
	return utils.IsTemporaryError(err)
}

// IsAuthenticationError checks if error is authentication-related using utils
func (e *ErrorHandler) IsAuthenticationError(err error) bool {
	return utils.IsAuthenticationError(err)
}

// IsNetworkError checks if error is network-related using utils
func (e *ErrorHandler) IsNetworkError(err error) bool {
	return utils.IsNetworkError(err)
}

// GetErrorCategory categorizes the error type using utils
func (e *ErrorHandler) GetErrorCategory(err error) string {
	return utils.GetErrorCategory(err)
}

// LogError logs an error with context and category
func (e *ErrorHandler) LogError(operation, context string, err error) {
	category := e.GetErrorCategory(err)
	log.Printf("Error in %s [%s] (%s): %v", operation, context, category, err)
}

// LogRetryAttempt logs a retry attempt with error categorization
func (e *ErrorHandler) LogRetryAttempt(operation string, attempt, maxAttempts int, err error) {
	category := e.GetErrorCategory(err)
	log.Printf("Retry %s (attempt %d/%d) (%s): %v", operation, attempt, maxAttempts, category, err)
}

// ShouldRetry determines if an error is worth retrying
func (e *ErrorHandler) ShouldRetry(err error) bool {
	if err == nil {
		return false
	}

	// Retry for connection and temporary errors
	return e.IsConnectionError(err) || e.IsTemporaryError(err)
}

// GetRetryDelay returns appropriate delay for retry based on error type
func (e *ErrorHandler) GetRetryDelay(attempt int, err error) time.Duration {
	baseDelay := time.Duration(attempt) * 2 * time.Second

	if e.IsConnectionError(err) {
		// Longer delay for connection errors
		return baseDelay * 2
	}
	if e.IsTemporaryError(err) {
		// Shorter delay for temporary errors
		return baseDelay / 2
	}

	return baseDelay
}
