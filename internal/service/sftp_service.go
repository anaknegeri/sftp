package service

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
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
	UploadFilesBatch(jobs []types.UploadSFTPJob) error
	Close() error
}

// SFTPConnection represents a reusable SFTP connection
type SFTPConnection struct {
	sshClient  *ssh.Client
	sftpClient *sftp.Client
	lastUsed   time.Time
	tenantID   string
	isHealthy  bool
	mu         sync.RWMutex
}

// SFTPConnectionPool manages connections per tenant
type SFTPConnectionPool struct {
	connections map[string]*SFTPConnection
	mu          sync.RWMutex
	maxIdle     time.Duration
	maxLifetime time.Duration
}

type sftpService struct {
	sftpLogRepo     repository.SFTPLogRepository
	localPath       string
	connectionPools map[string]*SFTPConnectionPool
	poolMutex       sync.RWMutex
	cleanupTicker   *time.Ticker
	stopCleanup     chan struct{}
}

func NewSFTPService(sftpLogRepo repository.SFTPLogRepository, localPath string) SFTPService {
	service := &sftpService{
		sftpLogRepo:     sftpLogRepo,
		localPath:       localPath,
		connectionPools: make(map[string]*SFTPConnectionPool),
		stopCleanup:     make(chan struct{}),
	}

	// Start cleanup routine for idle connections
	service.cleanupTicker = time.NewTicker(2 * time.Minute)
	go service.cleanupIdleConnections()

	return service
}

func (s *sftpService) Close() error {
	log.Println("[SFTP] Closing SFTP service and all connections")

	// Stop cleanup routine
	if s.cleanupTicker != nil {
		s.cleanupTicker.Stop()
	}
	close(s.stopCleanup)

	// Close all connections
	s.poolMutex.Lock()
	defer s.poolMutex.Unlock()

	for tenantID, pool := range s.connectionPools {
		pool.mu.Lock()
		for connKey, conn := range pool.connections {
			s.closeConnection(conn)
			delete(pool.connections, connKey)
		}
		pool.mu.Unlock()
		log.Printf("[SFTP] Closed all connections for tenant %s", tenantID)
	}

	return nil
}

func (s *sftpService) cleanupIdleConnections() {
	for {
		select {
		case <-s.cleanupTicker.C:
			s.performCleanup()
		case <-s.stopCleanup:
			return
		}
	}
}

func (s *sftpService) performCleanup() {
	s.poolMutex.RLock()
	defer s.poolMutex.RUnlock()

	for tenantID, pool := range s.connectionPools {
		pool.mu.Lock()
		for connKey, conn := range pool.connections {
			conn.mu.RLock()
			if time.Since(conn.lastUsed) > pool.maxIdle || time.Since(conn.lastUsed) > pool.maxLifetime {
				conn.mu.RUnlock()
				log.Printf("[SFTP] Cleaning up idle connection for tenant %s", tenantID)
				s.closeConnection(conn)
				delete(pool.connections, connKey)
			} else {
				conn.mu.RUnlock()
			}
		}
		pool.mu.Unlock()
	}
}

func (s *sftpService) getOrCreatePool(tenantID string) *SFTPConnectionPool {
	s.poolMutex.RLock()
	pool, exists := s.connectionPools[tenantID]
	s.poolMutex.RUnlock()

	if !exists {
		s.poolMutex.Lock()
		defer s.poolMutex.Unlock()

		// Double-check after acquiring write lock
		pool, exists = s.connectionPools[tenantID]
		if !exists {
			pool = &SFTPConnectionPool{
				connections: make(map[string]*SFTPConnection),
				maxIdle:     5 * time.Minute,
				maxLifetime: 30 * time.Minute,
			}
			s.connectionPools[tenantID] = pool
		}
	}

	return pool
}

func (s *sftpService) getConnection(tenantConfig *config.TenantConfig) (*SFTPConnection, error) {
	pool := s.getOrCreatePool(tenantConfig.ID)
	connKey := fmt.Sprintf("%s:%s@%s:%s", tenantConfig.SFTP.User, tenantConfig.SFTP.Host, tenantConfig.SFTP.Port, tenantConfig.ID)

	pool.mu.RLock()
	conn, exists := pool.connections[connKey]
	pool.mu.RUnlock()

	// Check if existing connection is healthy
	if exists && s.isConnectionHealthy(conn) {
		conn.mu.Lock()
		conn.lastUsed = time.Now()
		conn.mu.Unlock()
		return conn, nil
	}

	// Remove unhealthy connection
	if exists {
		pool.mu.Lock()
		delete(pool.connections, connKey)
		pool.mu.Unlock()
		s.closeConnection(conn)
	}

	// Create new connection
	return s.createNewConnection(pool, connKey, tenantConfig)
}

func (s *sftpService) isConnectionHealthy(conn *SFTPConnection) bool {
	if conn == nil {
		return false
	}

	conn.mu.RLock()
	defer conn.mu.RUnlock()

	if !conn.isHealthy || conn.sshClient == nil || conn.sftpClient == nil {
		return false
	}

	// Quick health check - try to stat a directory
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	done := make(chan bool, 1)
	go func() {
		_, err := conn.sftpClient.Stat(".")
		done <- err == nil
	}()

	select {
	case healthy := <-done:
		if !healthy {
			conn.isHealthy = false
		}
		return healthy
	case <-ctx.Done():
		conn.isHealthy = false
		return false
	}
}

func (s *sftpService) createNewConnection(pool *SFTPConnectionPool, connKey string, tenantConfig *config.TenantConfig) (*SFTPConnection, error) {
	log.Printf("[SFTP] Creating new connection for tenant %s", tenantConfig.ID)

	// Create SSH client config
	sshConfig := &ssh.ClientConfig{
		User:            tenantConfig.SFTP.User,
		Auth:            []ssh.AuthMethod{},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         15 * time.Second,
	}

	// Add authentication methods
	if tenantConfig.SFTP.Password != "" {
		sshConfig.Auth = append(sshConfig.Auth, ssh.Password(tenantConfig.SFTP.Password))
	}

	if tenantConfig.SFTP.KeyPath != "" {
		key, err := os.ReadFile(tenantConfig.SFTP.KeyPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read private key: %w", err)
		}

		signer, err := ssh.ParsePrivateKey(key)
		if err != nil {
			return nil, fmt.Errorf("failed to parse private key: %w", err)
		}

		sshConfig.Auth = append(sshConfig.Auth, ssh.PublicKeys(signer))
	}

	// Connect to SSH server with retry
	var sshClient *ssh.Client
	var err error
	maxRetries := 3

	for attempt := 1; attempt <= maxRetries; attempt++ {
		addr := fmt.Sprintf("%s:%s", tenantConfig.SFTP.Host, tenantConfig.SFTP.Port)
		sshClient, err = ssh.Dial("tcp", addr, sshConfig)
		if err == nil {
			break
		}

		log.Printf("[SFTP] SSH connection attempt %d/%d failed: %v", attempt, maxRetries, err)
		if attempt < maxRetries {
			time.Sleep(time.Duration(attempt) * 2 * time.Second)
		}
	}

	if err != nil {
		return nil, fmt.Errorf("SSH connection failed after %d attempts: %w", maxRetries, err)
	}

	// Create SFTP client
	sftpClient, err := sftp.NewClient(sshClient)
	if err != nil {
		sshClient.Close()
		return nil, fmt.Errorf("failed to create SFTP client: %w", err)
	}

	// Create connection object
	conn := &SFTPConnection{
		sshClient:  sshClient,
		sftpClient: sftpClient,
		lastUsed:   time.Now(),
		tenantID:   tenantConfig.ID,
		isHealthy:  true,
	}

	// Store in pool
	pool.mu.Lock()
	pool.connections[connKey] = conn
	pool.mu.Unlock()

	log.Printf("[SFTP] New connection created for tenant %s", tenantConfig.ID)
	return conn, nil
}

func (s *sftpService) closeConnection(conn *SFTPConnection) {
	if conn == nil {
		return
	}

	conn.mu.Lock()
	defer conn.mu.Unlock()

	conn.isHealthy = false

	if conn.sftpClient != nil {
		conn.sftpClient.Close()
	}
	if conn.sshClient != nil {
		conn.sshClient.Close()
	}
}

func (s *sftpService) UploadFilesBatch(jobs []types.UploadSFTPJob) error {
	if len(jobs) == 0 {
		return nil
	}

	log.Printf("[SFTP] Starting batch upload for %d files", len(jobs))

	const maxConcurrency = 8 // Adjust based on SFTP server capacity
	semaphore := make(chan struct{}, maxConcurrency)
	var wg sync.WaitGroup

	errorChan := make(chan error, len(jobs))
	successCount := int32(0)

	for _, job := range jobs {
		wg.Add(1)
		go func(job types.UploadSFTPJob) {
			defer wg.Done()
			semaphore <- struct{}{}        // Acquire
			defer func() { <-semaphore }() // Release

			if err := s.UploadFile(job); err != nil {
				errorChan <- fmt.Errorf("failed to upload %s: %w", job.FileName, err)
			} else {
				successCount++
			}
		}(job)
	}

	wg.Wait()
	close(errorChan)

	// Collect errors
	var errors []error
	for err := range errorChan {
		errors = append(errors, err)
	}

	log.Printf("[SFTP] Batch upload completed: %d/%d files uploaded successfully", successCount, len(jobs))

	if len(errors) > 0 {
		return fmt.Errorf("batch upload had %d failures: %v", len(errors), errors[0])
	}

	return nil
}

func (s *sftpService) UploadFile(job types.UploadSFTPJob) error {
	log.Printf("[SFTP] Starting upload for tenant %s, file %s", job.TenantID, job.FileName)

	// Get tenant configuration
	tenantConfig, exists := config.GetTenantByID(job.TenantID)
	if !exists {
		return fmt.Errorf("tenant %s not found or disabled", job.TenantID)
	}

	// Perform upload with retry
	var uploadErr error
	maxRetries := 3

	for attempt := 1; attempt <= maxRetries; attempt++ {
		log.Printf("[SFTP] Upload attempt %d/%d for file %s", attempt, maxRetries, job.FileName)

		uploadErr = s.performUploadWithConnection(tenantConfig, job.FilePath, job.RemotePath)
		if uploadErr == nil {
			break
		}

		log.Printf("[SFTP] Upload attempt %d failed: %v", attempt, uploadErr)

		if attempt < maxRetries {
			delay := s.getRetryDelay(attempt, uploadErr)
			log.Printf("[SFTP] Retrying in %v...", delay)
			time.Sleep(delay)
		}
	}

	existingLog, err := s.sftpLogRepo.GetByFileName(job.FileName)
	if err != nil {
		log.Printf("[SFTP] Failed to check existing log: %v", err)
	}

	var transferLog *entity.SFTPTransferLog

	if existingLog != nil {
		// Use existing log
		transferLog = existingLog
		log.Printf("[SFTP] Using existing log for file %s (ID: %s, Status: %s)", job.FileName, transferLog.ID, transferLog.Status)
	} else {
		// Create new log if not exists
		log.Printf("[SFTP] No existing log found for %s, creating new log", job.FileName)

		// Get file information
		fileInfo, err := os.Stat(job.FilePath)
		if err != nil {
			log.Printf("[SFTP] Failed to get file info for logging: %v", err)
		}

		recordCount := 0
		if err == nil {
			recordCount = utils.CountFileRecords(job.FilePath)
		}

		transferLog = &entity.SFTPTransferLog{
			ID:                uuid.New().String(),
			TenantID:          job.TenantID,
			LocationID:        job.LocationID,
			FileName:          job.FileName,
			FilePath:          job.FilePath,
			RemotePath:        job.RemotePath,
			Status:            "PENDING",
			TransferStartTime: time.Now(),
			RecordCount:       &recordCount,
			FileType:          job.FileType,
			CreatedAt:         time.Now(),
		}

		if err == nil {
			transferLog.FileSize = fileInfo.Size()
		}

		// Save new log to database
		if createErr := s.sftpLogRepo.Create(transferLog); createErr != nil {
			log.Printf("[SFTP] Failed to create transfer log: %v", createErr)
			// Don't return error, upload was already attempted
		} else {
			log.Printf("[SFTP] Created new log for file %s (ID: %s)", job.FileName, transferLog.ID)
		}
	}

	// Update final status based on upload result
	if uploadErr != nil {
		errorMsg := uploadErr.Error()
		if updateErr := s.sftpLogRepo.UpdateStatus(transferLog.ID, "FAILED", &errorMsg); updateErr != nil {
			log.Printf("[SFTP] Failed to update status to FAILED: %v", updateErr)
		}
		log.Printf("[SFTP] Upload failed for file %s: %v", job.FileName, uploadErr)
	} else {
		if updateErr := s.sftpLogRepo.UpdateStatus(transferLog.ID, "SUCCESS", nil); updateErr != nil {
			log.Printf("[SFTP] Failed to update status to SUCCESS: %v", updateErr)
		}
		log.Printf("[SFTP] Upload successful for file %s", job.FileName)
	}

	return uploadErr
}

func (s *sftpService) performUploadWithConnection(tenantConfig *config.TenantConfig, localPath, remotePath string) error {
	// Normalize remote path untuk Unix
	remotePath = strings.ReplaceAll(remotePath, "\\", "/")

	log.Printf("[SFTP] Uploading %s to %s", localPath, remotePath)

	// Get connection from pool
	conn, err := s.getConnection(tenantConfig)
	if err != nil {
		return fmt.Errorf("failed to get SFTP connection: %w", err)
	}

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
		if _, err := conn.sftpClient.Stat(remoteDir); err != nil {
			if err := conn.sftpClient.MkdirAll(remoteDir); err != nil {
				// Connection might be broken, mark as unhealthy
				conn.mu.Lock()
				conn.isHealthy = false
				conn.mu.Unlock()
				return fmt.Errorf("failed to create remote directory %s: %w", remoteDir, err)
			}
		}
	}

	// Create remote file
	remoteFile, err := conn.sftpClient.OpenFile(remotePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC)
	if err != nil {
		// Try alternative approach if TRUNC fails
		log.Printf("[SFTP] OpenFile with TRUNC failed: %v, trying without TRUNC", err)
		remoteFile, err = conn.sftpClient.OpenFile(remotePath, os.O_WRONLY|os.O_CREATE)
		if err != nil {
			// Connection might be broken
			conn.mu.Lock()
			conn.isHealthy = false
			conn.mu.Unlock()

			if utils.IsConnectionError(err) {
				return fmt.Errorf("connection error while creating file %s: %w", remotePath, err)
			}
			return fmt.Errorf("failed to create remote file %s: %w", remotePath, err)
		}

		// Truncate manually if we opened without TRUNC
		if err := remoteFile.Truncate(0); err != nil {
			remoteFile.Close()
			conn.mu.Lock()
			conn.isHealthy = false
			conn.mu.Unlock()
			return fmt.Errorf("failed to truncate remote file %s: %w", remotePath, err)
		}
	}
	defer remoteFile.Close()

	// Copy file content with optimized buffer and timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute) // Increased timeout
	defer cancel()

	err = s.copyFileWithTimeout(ctx, remoteFile, localFile)
	if err != nil {
		conn.mu.Lock()
		conn.isHealthy = false
		conn.mu.Unlock()
		return fmt.Errorf("failed to upload file: %w", err)
	}

	log.Printf("[SFTP] File uploaded successfully: %s -> %s", localPath, remotePath)
	return nil
}

func (s *sftpService) copyFileWithTimeout(ctx context.Context, dst io.Writer, src io.Reader) error {
	// Increased buffer size for better performance
	buffer := make([]byte, 256*1024) // 256KB buffer
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

					// Log progress every 30 seconds for large files
					if time.Since(lastProgressTime) > 30*time.Second {
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
		return fmt.Errorf("upload timeout after 10 minutes")
	case err := <-done:
		if err != nil {
			return err
		}
		if totalBytes > 1024*1024 { // Only log for files > 1MB
			log.Printf("[SFTP] Upload completed successfully: %d bytes transferred", totalBytes)
		}
		return nil
	}
}

func (s *sftpService) UploadAllPendingFiles(tenantID string) error {
	log.Printf("[SFTP] Getting pending uploads from database for tenant %s", tenantID)

	pendingLogs, err := s.sftpLogRepo.GetPendingUploads(tenantID)
	if err != nil {
		return fmt.Errorf("failed to get pending uploads: %w", err)
	}

	if len(pendingLogs) == 0 {
		log.Printf("[SFTP] No pending uploads found for tenant %s", tenantID)
		return nil
	}

	log.Printf("[SFTP] Found %d pending uploads for tenant %s", len(pendingLogs), tenantID)

	// CONCURRENT UPLOAD dengan kontrol concurrency
	const maxConcurrency = 10 // Sesuaikan dengan kapasitas SFTP server
	semaphore := make(chan struct{}, maxConcurrency)
	var wg sync.WaitGroup

	// Channel untuk collect results
	resultChan := make(chan UploadResult, len(pendingLogs))

	for _, logEntry := range pendingLogs {
		// Check file existence dulu
		if _, err := os.Stat(logEntry.FilePath); os.IsNotExist(err) {
			log.Printf("[SFTP] Pending file does not exist, marking as FAILED: %s", logEntry.FilePath)
			errorMsg := fmt.Sprintf("File not found: %s", logEntry.FilePath)
			s.sftpLogRepo.UpdateStatus(logEntry.ID, "FAILED", &errorMsg)
			resultChan <- UploadResult{FileName: logEntry.FileName, Success: false, Error: err}
			continue
		}

		wg.Add(1)
		go func(log *entity.SFTPTransferLog) {
			defer wg.Done()

			// Acquire semaphore
			semaphore <- struct{}{}
			defer func() { <-semaphore }() // Release semaphore

			result := s.uploadSingleFile(log)
			resultChan <- result
		}(logEntry)
	}

	// Wait for all uploads to complete
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results
	successCount := 0
	failureCount := 0
	for result := range resultChan {
		if result.Success {
			successCount++
			log.Printf("[SFTP] ✅ Successfully uploaded: %s", result.FileName)
		} else {
			failureCount++
			log.Printf("[SFTP] ❌ Failed to upload: %s - %v", result.FileName, result.Error)
		}
	}

	log.Printf("[SFTP] Concurrent upload completed for tenant %s: %d success, %d failed",
		tenantID, successCount, failureCount)

	if failureCount > 0 {
		return fmt.Errorf("some uploads failed: %d success, %d failed", successCount, failureCount)
	}

	return nil
}

// Helper struct untuk result
type UploadResult struct {
	FileName string
	Success  bool
	Error    error
}

// Helper method untuk upload single file
func (s *sftpService) uploadSingleFile(logEntry *entity.SFTPTransferLog) UploadResult {
	uploadJob := types.UploadSFTPJob{
		TenantID:   logEntry.TenantID,
		FilePath:   logEntry.FilePath,
		FileName:   logEntry.FileName,
		RemotePath: logEntry.RemotePath,
		FileType:   logEntry.FileType,
		LocationID: logEntry.LocationID,
		CreatedAt:  time.Now(),
	}

	log.Printf("[SFTP] 🚀 Starting concurrent upload: %s (log ID: %s)",
		uploadJob.FileName, logEntry.ID)

	// Update transfer start time
	logEntry.TransferStartTime = time.Now()
	if err := s.sftpLogRepo.Update(logEntry); err != nil {
		log.Printf("[SFTP] Failed to update transfer start time for %s: %v", uploadJob.FileName, err)
	}

	// Perform upload
	err := s.uploadFileFromJob(uploadJob)

	// Update status based on result
	if err != nil {
		errorMsg := err.Error()
		if updateErr := s.sftpLogRepo.UpdateStatus(logEntry.ID, "FAILED", &errorMsg); updateErr != nil {
			log.Printf("[SFTP] Failed to update status to FAILED for %s: %v", uploadJob.FileName, updateErr)
		}
		return UploadResult{FileName: uploadJob.FileName, Success: false, Error: err}
	} else {
		if updateErr := s.sftpLogRepo.UpdateStatus(logEntry.ID, "SUCCESS", nil); updateErr != nil {
			log.Printf("[SFTP] Failed to update status to SUCCESS for %s: %v", uploadJob.FileName, updateErr)
		}
		return UploadResult{FileName: uploadJob.FileName, Success: true, Error: nil}
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

func (s *sftpService) uploadFileFromJob(job types.UploadSFTPJob) error {
	// Get tenant configuration
	tenantConfig, exists := config.GetTenantByID(job.TenantID)
	if !exists {
		return fmt.Errorf("tenant %s not found or disabled", job.TenantID)
	}

	// Check if file exists
	if _, err := os.Stat(job.FilePath); os.IsNotExist(err) {
		return fmt.Errorf("file does not exist: %s", job.FilePath)
	}

	// Perform upload with retry
	var uploadErr error
	maxRetries := 3

	for attempt := 1; attempt <= maxRetries; attempt++ {
		log.Printf("[SFTP] Upload attempt %d/%d for pending file %s", attempt, maxRetries, job.FileName)

		uploadErr = s.performUploadWithConnection(tenantConfig, job.FilePath, job.RemotePath)
		if uploadErr == nil {
			break
		}

		log.Printf("[SFTP] Upload attempt %d failed: %v", attempt, uploadErr)

		if attempt < maxRetries {
			delay := s.getRetryDelay(attempt, uploadErr)
			log.Printf("[SFTP] Retrying in %v...", delay)
			time.Sleep(delay)
		}
	}

	return uploadErr
}
