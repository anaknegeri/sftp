// File: internal/service/sftp_service.go - COMPLETE FIX
package service

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"jarvist/sftp-service/internal/config"
	"jarvist/sftp-service/internal/repository"
	"jarvist/sftp-service/internal/types"
	"jarvist/sftp-service/pkg/utils"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

type SFTPService interface {
	UploadFile(job types.UploadSFTPJob) error
	UploadAllPendingFiles(tenantID string) error
	UploadFilesBatch(jobs []types.UploadSFTPJob) error
	Close() error
}

type SFTPConnection struct {
	sshClient  *ssh.Client
	sftpClient *sftp.Client
	lastUsed   time.Time
	tenantID   string
	isHealthy  bool
	mu         sync.RWMutex
}

type SFTPConnectionPool struct {
	connections map[string]*SFTPConnection
	mu          sync.RWMutex
	maxIdle     time.Duration
	maxLifetime time.Duration
}

type sftpService struct {
	sftpLogRepo          repository.SFTPLogRepository
	localPath            string
	connectionPools      map[string]*SFTPConnectionPool
	poolMutex            sync.RWMutex
	cleanupTicker        *time.Ticker
	stopCleanup          chan struct{}
	uploadLocks          sync.Map // map[string]time.Time for logID -> start time
	activeUploads        int64    // atomic counter
	maxConcurrentUploads int64
}

type UploadResult struct {
	LogID    string
	FileName string
	Success  bool
	Error    error
}

func NewSFTPService(sftpLogRepo repository.SFTPLogRepository, localPath string) SFTPService {
	service := &sftpService{
		sftpLogRepo:          sftpLogRepo,
		localPath:            localPath,
		connectionPools:      make(map[string]*SFTPConnectionPool),
		stopCleanup:          make(chan struct{}),
		maxConcurrentUploads: 30, // Increased concurrent uploads
	}

	service.cleanupTicker = time.NewTicker(1 * time.Minute) // More frequent cleanup
	go service.cleanupIdleConnections()

	return service
}

func (s *sftpService) UploadFile(job types.UploadSFTPJob) error {
	// Check if already processing this exact job
	if s.isUploadInProgress(job.LogID) {
		log.Printf("[SFTP] Upload already in progress for log ID: %s", job.LogID)
		return nil
	}

	// Wait for available upload slot
	if !s.waitForUploadSlot(30 * time.Second) {
		return fmt.Errorf("upload queue full, try again later")
	}

	// Increment active uploads
	atomic.AddInt64(&s.activeUploads, 1)
	defer atomic.AddInt64(&s.activeUploads, -1)

	log.Printf("[SFTP] Starting upload for file %s (log ID: %s) [Active: %d]",
		job.FileName, job.LogID, atomic.LoadInt64(&s.activeUploads))

	// Mark as processing
	s.uploadLocks.Store(job.LogID, time.Now())
	defer s.uploadLocks.Delete(job.LogID)

	_, exists := config.GetTenantByID(job.TenantID)
	if !exists {
		errorMsg := fmt.Sprintf("tenant %s not found or disabled", job.TenantID)
		s.sftpLogRepo.UpdateStatus(job.LogID, "FAILED", &errorMsg)
		return fmt.Errorf("%s", errorMsg)
	}

	result := s.uploadFileWithLogID(job)

	if !result.Success {
		log.Printf("[SFTP] Upload failed for file %s: %v", job.FileName, result.Error)
		return result.Error
	}

	log.Printf("[SFTP] Upload successful for file %s [Active: %d]",
		job.FileName, atomic.LoadInt64(&s.activeUploads))
	return nil
}

func (s *sftpService) waitForUploadSlot(timeout time.Duration) bool {
	start := time.Now()
	for {
		if atomic.LoadInt64(&s.activeUploads) < s.maxConcurrentUploads {
			return true
		}

		if time.Since(start) > timeout {
			log.Printf("[SFTP] Upload slot wait timeout after %v", timeout)
			return false
		}

		time.Sleep(100 * time.Millisecond)
	}
}

// ENHANCED: Better duplicate detection with database check
func (s *sftpService) isUploadInProgress(logID string) bool {
	// Check in-memory lock first
	if startTime, exists := s.uploadLocks.Load(logID); exists {
		if time.Since(startTime.(time.Time)) < 5*time.Minute {
			log.Printf("[SFTP] Upload already in progress (memory lock): %s", logID)
			return true
		}
		// Clean up expired lock
		s.uploadLocks.Delete(logID)
	}

	// Check database status
	if existingLog, err := s.sftpLogRepo.GetByID(logID); err == nil && existingLog != nil {
		switch existingLog.Status {
		case "SUCCESS":
			log.Printf("[SFTP] Upload already completed (SUCCESS): %s", logID)
			return true
		case "PROCESSING":
			// Check if processing for too long (stuck)
			if time.Since(existingLog.TransferStartTime) > 10*time.Minute {
				log.Printf("[SFTP] Processing stuck for %v, allowing retry: %s",
					time.Since(existingLog.TransferStartTime), logID)
				// Reset to PENDING to allow retry
				s.sftpLogRepo.UpdateStatus(logID, "PENDING", nil)
				return false
			}
			log.Printf("[SFTP] Upload currently processing: %s", logID)
			return true
		}
	}

	return false
}

func (s *sftpService) uploadFileWithLogID(job types.UploadSFTPJob) UploadResult {
	maxRetries := 2
	var uploadErr error

	// START: Verify log entry exists before processing
	existingLog, err := s.sftpLogRepo.GetByID(job.LogID)
	if err != nil {
		log.Printf("[SFTP] ERROR: Failed to get log entry %s: %v", job.LogID, err)
		return UploadResult{LogID: job.LogID, FileName: job.FileName, Success: false, Error: fmt.Errorf("log entry not found: %w", err)}
	}

	if existingLog == nil {
		log.Printf("[SFTP] ERROR: Log entry %s does not exist", job.LogID)
		return UploadResult{LogID: job.LogID, FileName: job.FileName, Success: false, Error: fmt.Errorf("log entry not found")}
	}

	// Check if already SUCCESS
	if existingLog.Status == "SUCCESS" {
		log.Printf("[SFTP] ✅ ALREADY SUCCESS: %s (log ID: %s)", job.FileName, job.LogID)
		return UploadResult{LogID: job.LogID, FileName: job.FileName, Success: true, Error: nil}
	}

	// FIXED: Only update to PROCESSING if not already PROCESSING to avoid conflicts
	if existingLog.Status == "PENDING" {
		processingMsg := "Upload in progress"
		if updateErr := s.updateStatusWithRetry(job.LogID, "PROCESSING", &processingMsg, 2); updateErr != nil {
			log.Printf("[SFTP] WARN: Failed to update PROCESSING status for %s: %v", job.FileName, updateErr)
			// Don't fail the upload, continue anyway
		} else {
			log.Printf("[SFTP] 🔄 PROCESSING: %s (log ID: %s)", job.FileName, job.LogID)
		}
	}

	for attempt := 1; attempt <= maxRetries; attempt++ {
		log.Printf("[SFTP] Upload attempt %d/%d for %s (log ID: %s)",
			attempt, maxRetries, job.FileName, job.LogID)

		uploadErr = s.performUpload(job)
		if uploadErr == nil {
			// SUCCESS - update status with enhanced retry logic
			log.Printf("[SFTP] Upload completed successfully, updating database status for %s", job.FileName)

			// FIXED: Enhanced retry with different strategies
			if updateErr := s.updateStatusWithRetry(job.LogID, "SUCCESS", nil, 5); updateErr != nil {
				log.Printf("[SFTP] ERROR: Failed to update SUCCESS status for %s after 5 retries: %v",
					job.FileName, updateErr)

				// CRITICAL: Log this as a critical issue but don't fail the upload
				log.Printf("[SFTP] 🚨 CRITICAL: Upload succeeded but database update failed for %s", job.FileName)

				// Try one final time with a longer delay
				time.Sleep(5 * time.Second)
				finalErr := s.sftpLogRepo.UpdateStatus(job.LogID, "SUCCESS", nil)
				if finalErr != nil {
					log.Printf("[SFTP] 🚨 FINAL DATABASE UPDATE FAILED for %s: %v", job.FileName, finalErr)
				} else {
					log.Printf("[SFTP] ✅ FINAL SUCCESS: Database updated for %s", job.FileName)
				}
			} else {
				log.Printf("[SFTP] ✅ SUCCESS: %s (log ID: %s) - Database updated", job.FileName, job.LogID)
			}

			return UploadResult{LogID: job.LogID, FileName: job.FileName, Success: true, Error: nil}
		}

		errorCategory := utils.GetErrorCategory(uploadErr)
		log.Printf("[SFTP] ❌ Attempt %d failed for %s [%s]: %v",
			attempt, job.FileName, errorCategory, uploadErr)

		if attempt < maxRetries && s.shouldRetry(uploadErr) {
			delay := s.getRetryDelay(attempt, uploadErr)
			log.Printf("[SFTP] Retrying %s in %v...", job.FileName, delay)
			time.Sleep(delay)
		}
	}

	// FAILED after all retries - update status with enhanced retry logic
	log.Printf("[SFTP] Upload failed after %d attempts, updating database status for %s", maxRetries, job.FileName)

	errorMsg := uploadErr.Error()
	if updateErr := s.updateStatusWithRetry(job.LogID, "FAILED", &errorMsg, 5); updateErr != nil {
		log.Printf("[SFTP] ERROR: Failed to update FAILED status for %s after 5 retries: %v",
			job.FileName, updateErr)
	} else {
		log.Printf("[SFTP] ❌ FINAL FAILURE: %s - %v (log ID: %s) - Database updated",
			job.FileName, uploadErr, job.LogID)
	}

	return UploadResult{LogID: job.LogID, FileName: job.FileName, Success: false, Error: uploadErr}
}

// NEW: Retry mechanism for database updates
func (s *sftpService) updateStatusWithRetry(logID, status string, errorMessage *string, maxRetries int) error {
	var updateErr error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		updateErr = s.sftpLogRepo.UpdateStatus(logID, status, errorMessage)
		if updateErr == nil {
			if attempt > 1 {
				log.Printf("[SFTP] Database update succeeded on attempt %d for log ID: %s", attempt, logID)
			}
			return nil
		}

		// FIXED: Better error categorization
		errorMsg := updateErr.Error()
		isTimeoutError := strings.Contains(errorMsg, "timeout") ||
			strings.Contains(errorMsg, "context deadline exceeded") ||
			strings.Contains(errorMsg, "connection reset")

		isTransactionError := strings.Contains(errorMsg, "transaction has already been committed") ||
			strings.Contains(errorMsg, "transaction has already been rolled back") ||
			strings.Contains(errorMsg, "deadlock detected")

		log.Printf("[SFTP] Database update attempt %d/%d failed for log ID %s [%s]: %v", attempt, maxRetries, logID, s.categorizeError(updateErr), updateErr)

		if attempt < maxRetries {
			// FIXED: Different retry strategies based on error type
			var delay time.Duration
			if isTimeoutError {
				delay = time.Duration(attempt) * 2 * time.Second // Longer delay for timeouts
			} else if isTransactionError {
				delay = time.Duration(attempt) * 500 * time.Millisecond // Shorter delay for transaction conflicts
			} else {
				delay = time.Duration(attempt) * time.Second // Standard delay
			}

			log.Printf("[SFTP] Retrying database update in %v...", delay)
			time.Sleep(delay)
		}
	}

	return fmt.Errorf("failed after %d attempts: %w", maxRetries, updateErr)
}

func (s *sftpService) categorizeError(err error) string {
	if err == nil {
		return "none"
	}

	errorMsg := strings.ToLower(err.Error())

	if strings.Contains(errorMsg, "timeout") || strings.Contains(errorMsg, "context deadline exceeded") {
		return "timeout"
	}
	if strings.Contains(errorMsg, "transaction") || strings.Contains(errorMsg, "deadlock") {
		return "transaction"
	}
	if strings.Contains(errorMsg, "connection") {
		return "connection"
	}
	if strings.Contains(errorMsg, "not found") {
		return "not_found"
	}

	return "unknown"
}

func (s *sftpService) shouldRetry(err error) bool {
	if err == nil {
		return false
	}

	// Don't retry authentication errors
	if utils.IsAuthenticationError(err) {
		return false
	}

	// Retry connection and temporary errors
	return utils.IsConnectionError(err) || utils.IsTemporaryError(err)
}

func (s *sftpService) UploadAllPendingFiles(tenantID string) error {
	log.Printf("[SFTP] UploadAllPendingFiles called for tenant: %s", tenantID)

	pendingLogs, err := s.sftpLogRepo.GetPendingUploads(tenantID)
	if err != nil {
		log.Printf("[SFTP] Failed to get pending uploads: %v", err)
		return fmt.Errorf("failed to get pending uploads: %w", err)
	}

	log.Printf("[SFTP] Retrieved %d pending files from database", len(pendingLogs))

	if len(pendingLogs) == 0 {
		log.Printf("[SFTP] No pending uploads found for tenant %s", tenantID)
		return nil
	}

	// Process in smaller batches to avoid overwhelming
	batchSize := 50
	for i := 0; i < len(pendingLogs); i += batchSize {
		end := i + batchSize
		if end > len(pendingLogs) {
			end = len(pendingLogs)
		}

		batchLogs := pendingLogs[i:end]
		validJobs := []types.UploadSFTPJob{}
		missingFiles := 0

		for _, logEntry := range batchLogs {
			if _, err := os.Stat(logEntry.FilePath); os.IsNotExist(err) {
				log.Printf("[SFTP] File missing: %s", logEntry.FilePath)
				errorMsg := fmt.Sprintf("File not found: %s", logEntry.FilePath)
				s.sftpLogRepo.UpdateStatus(logEntry.ID, "FAILED", &errorMsg)
				missingFiles++
				continue
			}

			validJobs = append(validJobs, types.UploadSFTPJob{
				LogID:      logEntry.ID,
				TenantID:   logEntry.TenantID,
				FilePath:   logEntry.FilePath,
				FileName:   logEntry.FileName,
				RemotePath: logEntry.RemotePath,
				FileType:   logEntry.FileType,
				LocationID: logEntry.LocationID,
				CreatedAt:  time.Now(),
			})
		}

		log.Printf("[SFTP] Batch %d-%d: %d valid, %d missing files",
			i+1, end, len(validJobs), missingFiles)

		if len(validJobs) > 0 {
			if err := s.UploadFilesBatch(validJobs); err != nil {
				log.Printf("[SFTP] Batch %d-%d failed: %v", i+1, end, err)
			}
		}

		// Small delay between batches
		if end < len(pendingLogs) {
			time.Sleep(500 * time.Millisecond)
		}
	}

	return nil
}

func (s *sftpService) UploadFilesBatch(jobs []types.UploadSFTPJob) error {
	if len(jobs) == 0 {
		return nil
	}

	log.Printf("[SFTP] Starting batch upload for %d files", len(jobs))

	// Increased concurrency but with better control
	maxConcurrency := 15
	semaphore := make(chan struct{}, maxConcurrency)
	var wg sync.WaitGroup

	resultChan := make(chan UploadResult, len(jobs))
	var successCount, failureCount int32
	startTime := time.Now()

	// Process jobs with controlled concurrency
	for _, job := range jobs {
		wg.Add(1)
		go func(job types.UploadSFTPJob) {
			defer wg.Done()

			select {
			case semaphore <- struct{}{}:
				defer func() { <-semaphore }()
			case <-time.After(30 * time.Second):
				log.Printf("[SFTP] Semaphore timeout for %s", job.FileName)
				resultChan <- UploadResult{
					LogID:    job.LogID,
					FileName: job.FileName,
					Success:  false,
					Error:    fmt.Errorf("timeout acquiring semaphore"),
				}
				return
			}

			result := s.uploadFileWithLogID(job)
			resultChan <- result

			if result.Success {
				atomic.AddInt32(&successCount, 1)
			} else {
				atomic.AddInt32(&failureCount, 1)
			}
		}(job)
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	var errors []error
	for result := range resultChan {
		if !result.Success {
			errors = append(errors, fmt.Errorf("%s: %v", result.FileName, result.Error))
		}
	}

	elapsed := time.Since(startTime)
	rate := float64(len(jobs)) / elapsed.Seconds()

	log.Printf("[SFTP] Batch upload completed in %v: %d success, %d failed (%.1f files/sec)",
		elapsed, successCount, failureCount, rate)

	// Log only first 3 errors to avoid spam
	if len(errors) > 0 && len(errors) <= 3 {
		for _, err := range errors {
			log.Printf("[SFTP] Upload error: %v", err)
		}
	} else if len(errors) > 3 {
		log.Printf("[SFTP] %d upload errors (showing first 3):", len(errors))
		for i := 0; i < 3; i++ {
			log.Printf("[SFTP] Upload error: %v", errors[i])
		}
	}

	if failureCount > 0 {
		return fmt.Errorf("batch upload had %d failures out of %d total", failureCount, len(jobs))
	}

	return nil
}

func (s *sftpService) performUpload(job types.UploadSFTPJob) error {
	tenantConfig, exists := config.GetTenantByID(job.TenantID)
	if !exists {
		return fmt.Errorf("tenant %s not found", job.TenantID)
	}

	if _, err := os.Stat(job.FilePath); os.IsNotExist(err) {
		return fmt.Errorf("file does not exist: %s", job.FilePath)
	}

	return s.performUploadWithConnection(tenantConfig, job.FilePath, job.RemotePath)
}

func (s *sftpService) getRetryDelay(attempt int, err error) time.Duration {
	baseDelay := time.Duration(attempt) * time.Second

	if utils.IsConnectionError(err) {
		return baseDelay * 2
	}
	if utils.IsTemporaryError(err) {
		return baseDelay
	}

	return baseDelay
}

func (s *sftpService) Close() error {
	log.Println("[SFTP] Closing SFTP service and all connections")

	if s.cleanupTicker != nil {
		s.cleanupTicker.Stop()
	}
	close(s.stopCleanup)

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
	// Clean up expired upload locks
	expiredCount := 0
	cutoff := time.Now().Add(-5 * time.Minute)

	s.uploadLocks.Range(func(key, value interface{}) bool {
		if startTime, ok := value.(time.Time); ok && startTime.Before(cutoff) {
			s.uploadLocks.Delete(key)
			expiredCount++
		}
		return true
	})

	if expiredCount > 0 {
		log.Printf("[SFTP] Cleaned up %d expired upload locks", expiredCount)
	}

	// Clean up idle connections
	s.poolMutex.RLock()
	defer s.poolMutex.RUnlock()

	for _, pool := range s.connectionPools {
		pool.mu.Lock()
		for connKey, conn := range pool.connections {
			conn.mu.RLock()
			if time.Since(conn.lastUsed) > pool.maxIdle || time.Since(conn.lastUsed) > pool.maxLifetime {
				conn.mu.RUnlock()
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

	if exists && s.isConnectionHealthy(conn) {
		conn.mu.Lock()
		conn.lastUsed = time.Now()
		conn.mu.Unlock()
		return conn, nil
	}

	if exists {
		pool.mu.Lock()
		delete(pool.connections, connKey)
		pool.mu.Unlock()
		s.closeConnection(conn)
	}

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

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
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
	sshConfig := &ssh.ClientConfig{
		User:            tenantConfig.SFTP.User,
		Auth:            []ssh.AuthMethod{},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         10 * time.Second,
	}

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

	var sshClient *ssh.Client
	var err error
	maxRetries := 2

	for attempt := 1; attempt <= maxRetries; attempt++ {
		addr := fmt.Sprintf("%s:%s", tenantConfig.SFTP.Host, tenantConfig.SFTP.Port)
		sshClient, err = ssh.Dial("tcp", addr, sshConfig)
		if err == nil {
			break
		}

		if attempt < maxRetries {
			time.Sleep(time.Duration(attempt) * time.Second)
		}
	}

	if err != nil {
		return nil, fmt.Errorf("SSH connection failed after %d attempts: %w", maxRetries, err)
	}

	sftpClient, err := sftp.NewClient(sshClient)
	if err != nil {
		sshClient.Close()
		return nil, fmt.Errorf("failed to create SFTP client: %w", err)
	}

	conn := &SFTPConnection{
		sshClient:  sshClient,
		sftpClient: sftpClient,
		lastUsed:   time.Now(),
		tenantID:   tenantConfig.ID,
		isHealthy:  true,
	}

	pool.mu.Lock()
	pool.connections[connKey] = conn
	pool.mu.Unlock()

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

func (s *sftpService) performUploadWithConnection(tenantConfig *config.TenantConfig, localPath, remotePath string) error {
	remotePath = strings.ReplaceAll(remotePath, "\\", "/")

	conn, err := s.getConnection(tenantConfig)
	if err != nil {
		return fmt.Errorf("failed to get SFTP connection: %w", err)
	}

	localFile, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("failed to open local file: %w", err)
	}
	defer localFile.Close()

	remoteFile, err := conn.sftpClient.OpenFile(remotePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC)
	if err != nil {
		remoteFile, err = conn.sftpClient.OpenFile(remotePath, os.O_WRONLY|os.O_CREATE)
		if err != nil {
			conn.mu.Lock()
			conn.isHealthy = false
			conn.mu.Unlock()
			return fmt.Errorf("failed to create remote file %s: %w", remotePath, err)
		}

		if err := remoteFile.Truncate(0); err != nil {
			remoteFile.Close()
			conn.mu.Lock()
			conn.isHealthy = false
			conn.mu.Unlock()
			return fmt.Errorf("failed to truncate remote file %s: %w", remotePath, err)
		}
	}
	defer remoteFile.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	err = s.copyFileWithTimeout(ctx, remoteFile, localFile)
	if err != nil {
		conn.mu.Lock()
		conn.isHealthy = false
		conn.mu.Unlock()
		return fmt.Errorf("failed to upload file: %w", err)
	}

	return nil
}

func (s *sftpService) copyFileWithTimeout(ctx context.Context, dst io.Writer, src io.Reader) error {
	buffer := make([]byte, 128*1024)
	totalBytes := int64(0)

	done := make(chan error, 1)

	go func() {
		for {
			select {
			case <-ctx.Done():
				done <- ctx.Err()
				return
			default:
				n, err := src.Read(buffer)
				if err != nil {
					if err == io.EOF {
						done <- nil
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
				}
			}
		}
	}()

	select {
	case <-ctx.Done():
		return fmt.Errorf("upload timeout after 5 minutes")
	case err := <-done:
		return err
	}
}
