package service

import (
	"fmt"
	"jarvist/sftp/internal/config"
	"jarvist/sftp/internal/domain/entity"
	"jarvist/sftp/internal/repository"
	"jarvist/sftp/internal/types"
	"jarvist/sftp/pkg/utils"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

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
	} else if existingLog != nil && existingLog.Status == "SUCCESS" {
		// File already uploaded successfully
		log.Printf("[SFTP] File %s already uploaded successfully, skipping", job.FileName)
		return nil
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

		// Check if file was already uploaded successfully
		existingLog, err := s.sftpLogRepo.GetByFileName(fileName)
		if err != nil {
			log.Printf("[SFTP] Failed to check existing log for %s: %v", fileName, err)
			continue
		}

		if existingLog != nil && existingLog.Status == "SUCCESS" {
			log.Printf("[SFTP] File %s already uploaded successfully, skipping", fileName)
			continue
		}

		// Create upload job
		fileType := utils.DetermineFileType(fileName)

		// Get tenant config for proper remote path
		tenantConfig, exists := config.GetTenantByID(tenantID)
		if !exists {
			log.Printf("[SFTP] Tenant %s not found, skipping file %s", tenantID, fileName)
			continue
		}

		// Use forward slashes for remote path (Unix-style)
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
		User: sftpConfig.User,
		Auth: []ssh.AuthMethod{
			ssh.Password(sftpConfig.Password),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), // Use proper host key verification in production
		Timeout:         30 * time.Second,
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

		sshConfig.Auth = []ssh.AuthMethod{ssh.PublicKeys(signer)}
	}

	// Connect to SSH server
	addr := fmt.Sprintf("%s:%s", sftpConfig.Host, sftpConfig.Port)
	sshClient, err := ssh.Dial("tcp", addr, sshConfig)
	if err != nil {
		return fmt.Errorf("failed to connect to SSH server: %w", err)
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

		// Try to create directory, but don't fail if it already exists
		if err := sftpClient.MkdirAll(remoteDir); err != nil {
			// Check if it's a permission error or if directory already exists
			if _, statErr := sftpClient.Stat(remoteDir); statErr != nil {
				return fmt.Errorf("failed to create remote directory %s: %w", remoteDir, err)
			}
			log.Printf("[SFTP] Directory %s already exists or permission issue, continuing...", remoteDir)
		}
	}

	// Create remote file
	remoteFile, err := sftpClient.Create(remotePath)
	if err != nil {
		return fmt.Errorf("failed to create remote file %s: %w", remotePath, err)
	}
	defer remoteFile.Close()

	// Copy file content
	_, err = remoteFile.ReadFrom(localFile)
	if err != nil {
		return fmt.Errorf("failed to upload file: %w", err)
	}

	log.Printf("[SFTP] File uploaded successfully: %s -> %s", localPath, remotePath)
	return nil
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
