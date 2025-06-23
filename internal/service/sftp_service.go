package service

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"jarvist/sftp-service/internal/config"
	"jarvist/sftp-service/internal/domain/entity"
	"jarvist/sftp-service/internal/repository"
	"jarvist/sftp-service/internal/types"
	"jarvist/sftp-service/pkg/utils"

	"github.com/google/uuid"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

type SFTPService interface {
	UploadFile(job types.UploadSFTPJob) error
	UploadAllPendingFiles(tenantID string) error
}

type sftpService struct {
	sftpLogRepo repository.SFTPLogRepository
	localPath   string
}

func NewSFTPService(sftpLogRepo repository.SFTPLogRepository, localPath string) SFTPService {
	return &sftpService{
		sftpLogRepo: sftpLogRepo,
		localPath:   localPath,
	}
}

func (s *sftpService) UploadFile(job types.UploadSFTPJob) error {
	log.Printf("[SFTP] Starting upload for tenant %s, file %s", job.TenantID, job.FileName)

	// Get tenant configuration
	tenantConfig, exists := config.GetTenantByID(job.TenantID)
	if !exists {
		return fmt.Errorf("tenant %s not found or disabled", job.TenantID)
	}

	// Check if there's an existing log for this file
	existingLog, err := s.sftpLogRepo.GetByFileName(job.FileName)
	if err != nil {
		log.Printf("[SFTP] Failed to check existing log: %v", err)
	}

	var transferLog *entity.SFTPTransferLog

	if existingLog != nil && existingLog.Status == "PENDING" {
		// Update existing log
		transferLog = existingLog
		transferLog.TransferStartTime = time.Now()
		log.Printf("[SFTP] Using existing PENDING log for file %s (ID: %s)", job.FileName, transferLog.ID)
	} else {
		// Create new log
		transferLog = &entity.SFTPTransferLog{
			ID:                uuid.New().String(),
			TenantID:          job.TenantID,
			LocationID:        job.LocationID,
			FileName:          job.FileName,
			FilePath:          job.FilePath,
			RemotePath:        job.RemotePath,
			Status:            "PENDING",
			TransferStartTime: time.Now(),
			FileType:          job.FileType,
			CreatedAt:         time.Now(),
		}

		// Get file size and record count
		if fileInfo, err := os.Stat(job.FilePath); err == nil {
			transferLog.FileSize = fileInfo.Size()
		}
		recordCount := utils.CountFileRecords(job.FilePath)
		transferLog.RecordCount = &recordCount

		// Save initial log
		if err := s.sftpLogRepo.Create(transferLog); err != nil {
			log.Printf("[SFTP] Failed to create transfer log: %v", err)
		}
	}

	// Perform upload with retry
	var uploadErr error
	maxRetries := 3

	for attempt := 1; attempt <= maxRetries; attempt++ {
		log.Printf("[SFTP] Upload attempt %d/%d for file %s", attempt, maxRetries, job.FileName)

		uploadErr = s.performUpload(tenantConfig.SFTP, job.FilePath, job.RemotePath)
		if uploadErr == nil {
			break
		}

		log.Printf("[SFTP] Upload attempt %d failed: %v", attempt, uploadErr)

		if attempt < maxRetries {
			// Determine retry delay based on error type
			delay := s.getRetryDelay(attempt, uploadErr)
			log.Printf("[SFTP] Retrying in %v...", delay)
			time.Sleep(delay)
		}
	}

	// Update transfer log
	now := time.Now()
	transferLog.TransferEndTime = &now

	if uploadErr != nil {
		transferLog.Status = "FAILED"
		errorMsg := uploadErr.Error()
		transferLog.ErrorMessage = &errorMsg
		log.Printf("[SFTP] Upload failed for file %s: %v", job.FileName, uploadErr)
	} else {
		transferLog.Status = "SUCCESS"
		log.Printf("[SFTP] Upload successful for file %s", job.FileName)
	}

	// Update log in database
	if err := s.sftpLogRepo.Update(transferLog); err != nil {
		log.Printf("[SFTP] Failed to update transfer log: %v", err)
	}

	return uploadErr
}

func (s *sftpService) UploadAllPendingFiles(tenantID string) error {
	log.Printf("[SFTP] Scanning for pending files for tenant %s", tenantID)

	tenantDir := filepath.Join(s.localPath, tenantID)
	if _, err := os.Stat(tenantDir); os.IsNotExist(err) {
		log.Printf("[SFTP] No directory found for tenant %s", tenantID)
		return nil
	}

	files, err := filepath.Glob(filepath.Join(tenantDir, "*.csv"))
	if err != nil {
		return fmt.Errorf("failed to scan files: %w", err)
	}

	log.Printf("[SFTP] Found %d CSV files for tenant %s", len(files), tenantID)

	successCount := 0
	for _, filePath := range files {
		fileName := filepath.Base(filePath)
		// Create upload job
		fileType := utils.DetermineFileType(fileName)

		// Get tenant config for proper remote path
		tenantConfig, exists := config.GetTenantByID(tenantID)
		if !exists {
			log.Printf("[SFTP] Tenant %s not found, skipping file %s", tenantID, fileName)
			continue
		}

		// Use forward slashes for remote path
		remotePath := strings.ReplaceAll(filepath.Join(tenantConfig.SFTP.BasePath, fileName), "\\", "/")

		uploadJob := types.UploadSFTPJob{
			TenantID:   tenantID,
			FilePath:   filePath,
			FileName:   fileName,
			RemotePath: remotePath,
			FileType:   fileType,
			CreatedAt:  time.Now(),
		}

		if err := s.UploadFile(uploadJob); err != nil {
			log.Printf("[SFTP] Failed to upload file %s: %v", fileName, err)
			continue
		}

		successCount++
	}

	log.Printf("[SFTP] Upload completed for tenant %s: %d/%d files uploaded successfully",
		tenantID, successCount, len(files))
	return nil
}

func (s *sftpService) performUpload(sftpConfig config.SFTPConfig, localPath, remotePath string) error {
	// Normalize remote path untuk Unix (remove Windows backslashes)
	remotePath = strings.ReplaceAll(remotePath, "\\", "/")

	log.Printf("[SFTP] Uploading %s to %s", localPath, remotePath)

	// Create SSH client config
	sshConfig := &ssh.ClientConfig{
		User:            sftpConfig.User,
		Auth:            []ssh.AuthMethod{},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), // Use proper host key verification in production
		Timeout:         15 * time.Second,            // Increased timeout like in infrastructure code
	}

	// Add authentication methods
	if sftpConfig.Password != "" {
		sshConfig.Auth = append(sshConfig.Auth, ssh.Password(sftpConfig.Password))
	}

	// If key path is provided, use key authentication
	if sftpConfig.KeyPath != "" {
		key, err := os.ReadFile(sftpConfig.KeyPath)
		if err != nil {
			return fmt.Errorf("failed to read private key: %w", err)
		}

		signer, err := ssh.ParsePrivateKey(key)
		if err != nil {
			return fmt.Errorf("failed to parse private key: %w", err)
		}

		sshConfig.Auth = append(sshConfig.Auth, ssh.PublicKeys(signer))
	}

	// Connect to SSH server with retry logic (like infrastructure code)
	var sshClient *ssh.Client
	var err error
	maxRetries := 3
	for attempt := 1; attempt <= maxRetries; attempt++ {
		addr := fmt.Sprintf("%s:%s", sftpConfig.Host, sftpConfig.Port)
		sshClient, err = ssh.Dial("tcp", addr, sshConfig)
		if err == nil {
			break
		}

		if attempt < maxRetries {
			time.Sleep(time.Duration(attempt) * 2 * time.Second)
			continue
		}
		return fmt.Errorf("SSH connection failed after %d attempts: %w", maxRetries, err)
	}
	defer sshClient.Close()

	// Create SFTP client
	sftpClient, err := sftp.NewClient(sshClient)
	if err != nil {
		return fmt.Errorf("failed to create SFTP client: %w", err)
	}
	defer sftpClient.Close()

	// Open local file
	localFile, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("failed to open local file: %w", err)
	}
	defer localFile.Close()

	// Create remote directory if it doesn't exist
	remoteDir := filepath.Dir(remotePath)
	if remoteDir != "." && remoteDir != "/" {
		remoteDir = strings.ReplaceAll(remoteDir, "\\", "/")
		log.Printf("[SFTP] Creating remote directory: %s", remoteDir)

		// Check if directory exists first
		if _, err := sftpClient.Stat(remoteDir); err != nil {
			// Directory doesn't exist, try to create it
			if err := sftpClient.MkdirAll(remoteDir); err != nil {
				return fmt.Errorf("failed to create remote directory %s: %w", remoteDir, err)
			}
		}
	}

	// Create remote file using OpenFile with proper flags (like infrastructure code)
	var remoteFile *sftp.File

	// Try OpenFile with flags that are more compatible
	remoteFile, err = sftpClient.OpenFile(remotePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC)
	if err != nil {
		// If still fails, try with different flags
		log.Printf("[SFTP] OpenFile with TRUNC failed: %v, trying without TRUNC", err)
		remoteFile, err = sftpClient.OpenFile(remotePath, os.O_WRONLY|os.O_CREATE)
		if err != nil {
			// If connection error, this might need reconnection handling
			if utils.IsConnectionError(err) {
				return fmt.Errorf("connection error while creating file %s: %w", remotePath, err)
			}
			return fmt.Errorf("failed to create remote file %s: %w", remotePath, err)
		}

		// If we opened without TRUNC, we need to truncate manually
		if err := remoteFile.Truncate(0); err != nil {
			remoteFile.Close()
			return fmt.Errorf("failed to truncate remote file %s: %w", remotePath, err)
		}
	}
	defer remoteFile.Close()

	// Create context with timeout for the upload (like infrastructure code)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Copy file content with timeout and progress monitoring
	err = s.copyFileWithTimeout(ctx, remoteFile, localFile)
	if err != nil {
		return fmt.Errorf("failed to upload file: %w", err)
	}

	log.Printf("[SFTP] File uploaded successfully: %s -> %s", localPath, remotePath)
	return nil
}

func (s *sftpService) copyFileWithTimeout(ctx context.Context, dst io.Writer, src io.Reader) error {
	buffer := make([]byte, 32*1024) // 32KB buffer
	totalBytes := int64(0)
	lastProgressTime := time.Now()

	// Create a channel to track copy completion
	done := make(chan error, 1)

	go func() {
		for {
			select {
			case <-ctx.Done():
				done <- ctx.Err()
				return
			default:
				// Set read deadline on source if possible
				if conn, ok := src.(*os.File); ok {
					conn.SetReadDeadline(time.Now().Add(30 * time.Second))
				}

				n, err := src.Read(buffer)
				if err != nil {
					if err == io.EOF {
						done <- nil // Success
						return
					}
					done <- fmt.Errorf("read error: %w", err)
					return
				}

				if n > 0 {
					written, writeErr := dst.Write(buffer[:n])
					if writeErr != nil {
						done <- fmt.Errorf("write error: %w", writeErr)
						return
					}

					totalBytes += int64(written)

					// Log progress every 10 seconds
					if time.Since(lastProgressTime) > 10*time.Second {
						log.Printf("[SFTP] Upload progress: %d bytes transferred", totalBytes)
						lastProgressTime = time.Now()
					}
				}
			}
		}
	}()

	// Wait for completion or timeout
	select {
	case <-ctx.Done():
		return fmt.Errorf("upload timeout after 5 minutes")
	case err := <-done:
		if err != nil {
			return err
		}
		log.Printf("[SFTP] Upload completed successfully: %d bytes transferred", totalBytes)
		return nil
	}
}

func (s *sftpService) getRetryDelay(attempt int, err error) time.Duration {
	baseDelay := time.Duration(attempt) * 2 * time.Second

	if utils.IsConnectionError(err) {
		// Longer delay for connection errors
		return baseDelay * 2
	}
	if utils.IsTemporaryError(err) {
		// Shorter delay for temporary errors
		return baseDelay / 2
	}

	return baseDelay
}
