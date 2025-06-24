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

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

type SFTPService interface {
	UploadFile(job types.UploadSFTPJob) error
	UploadAllPendingFiles(tenantID string) error
	UploadFilesBatch(jobs []types.UploadSFTPJob) error
	FixStuckPendingFiles(tenantID string) error
	VerifyAndFixSingleFile(tenantID, fileName string) error
	TestSingleUpload(tenantID, fileName string) error
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

type UploadResult struct {
	FileName string
	Success  bool
	Error    error
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

// FixStuckPendingFiles - method untuk fix files yang stuck di PENDING
func (s *sftpService) FixStuckPendingFiles(tenantID string) error {
	log.Printf("[FIX] Starting to fix stuck PENDING files for tenant %s", tenantID)

	// Get files yang pernah diproses tapi masih PENDING
	stuckFiles, err := s.sftpLogRepo.GetStuckPendingFiles(tenantID)
	if err != nil {
		return fmt.Errorf("failed to get stuck files: %w", err)
	}

	if len(stuckFiles) == 0 {
		log.Printf("[FIX] No stuck PENDING files found for tenant %s", tenantID)
		return nil
	}

	log.Printf("[FIX] Found %d stuck PENDING files", len(stuckFiles))

	// Process dalam batch kecil untuk menghindari overload
	batchSize := 20
	successCount := 0
	failureCount := 0
	verifiedCount := 0

	for i := 0; i < len(stuckFiles); i += batchSize {
		end := i + batchSize
		if end > len(stuckFiles) {
			end = len(stuckFiles)
		}

		currentBatch := stuckFiles[i:end]
		log.Printf("[FIX] Processing verification batch %d-%d of %d stuck files", i+1, end, len(stuckFiles))

		// Verify each file di SFTP server sebelum update status
		for _, file := range currentBatch {
			log.Printf("[FIX] Verifying file: %s", file.FileName)

			exists, fileSize := s.verifyFileOnSFTP(file)
			if exists {
				// File exists on SFTP, safe to mark as SUCCESS
				if err := s.sftpLogRepo.UpdateStatusWithRetry(file.ID, "SUCCESS", nil, 3); err != nil {
					log.Printf("[FIX] ❌ Failed to update %s to SUCCESS: %v", file.FileName, err)
					failureCount++
				} else {
					log.Printf("[FIX] ✅ Fixed %s to SUCCESS (size: %d bytes)", file.FileName, fileSize)
					successCount++
				}
				verifiedCount++
			} else {
				// File not on SFTP, mark as FAILED
				errorMsg := "File verification failed - not found on SFTP server during fix"
				if err := s.sftpLogRepo.UpdateStatusWithRetry(file.ID, "FAILED", &errorMsg, 3); err != nil {
					log.Printf("[FIX] ❌ Failed to update %s to FAILED: %v", file.FileName, err)
					failureCount++
				} else {
					log.Printf("[FIX] ⚠️ Marked %s as FAILED (not on SFTP)", file.FileName)
					successCount++
				}
			}
		}

		// Progress report
		log.Printf("[FIX] Batch %d-%d completed: %d verified on SFTP, %d status updates successful, %d failed",
			i+1, end, verifiedCount, successCount, failureCount)

		// Small delay between batches
		time.Sleep(500 * time.Millisecond)
	}

	log.Printf("[FIX] Completed fixing stuck files: %d verified on SFTP, %d status updates successful, %d failures",
		verifiedCount, successCount, failureCount)
	return nil
}

// VerifyAndFixSingleFile - verify dan fix single file
func (s *sftpService) VerifyAndFixSingleFile(tenantID, fileName string) error {
	log.Printf("[FIX] Verifying single file: %s", fileName)

	// Get file log
	fileLog, err := s.sftpLogRepo.GetByFileName(fileName)
	if err != nil || fileLog == nil {
		return fmt.Errorf("file log not found for %s: %v", fileName, err)
	}

	if fileLog.TenantID != tenantID {
		return fmt.Errorf("file %s does not belong to tenant %s", fileName, tenantID)
	}

	log.Printf("[FIX] Found file log: %s, current status: %s", fileName, fileLog.Status)

	// Verify on SFTP
	exists, fileSize := s.verifyFileOnSFTP(fileLog)
	if exists {
		if fileLog.Status != "SUCCESS" {
			if err := s.sftpLogRepo.UpdateStatus(fileLog.ID, "SUCCESS", nil); err != nil {
				return fmt.Errorf("failed to update status to SUCCESS: %w", err)
			}
			log.Printf("[FIX] ✅ File %s verified and marked as SUCCESS (size: %d bytes)", fileName, fileSize)
		} else {
			log.Printf("[FIX] ✅ File %s already marked as SUCCESS", fileName)
		}
	} else {
		errorMsg := "File not found on SFTP server"
		if err := s.sftpLogRepo.UpdateStatus(fileLog.ID, "FAILED", &errorMsg); err != nil {
			return fmt.Errorf("failed to update status to FAILED: %w", err)
		}
		log.Printf("[FIX] ❌ File %s not found on SFTP, marked as FAILED", fileName)
	}

	return nil
}

// TestSingleUpload - test upload single file
func (s *sftpService) TestSingleUpload(tenantID, fileName string) error {
	log.Printf("[TEST] Testing single upload for file: %s", fileName)

	// Get pending file
	pendingLogs, err := s.sftpLogRepo.GetPendingUploads(tenantID)
	if err != nil {
		return fmt.Errorf("failed to get pending uploads: %w", err)
	}

	var targetLog *entity.SFTPTransferLog
	for _, log := range pendingLogs {
		if log.FileName == fileName {
			targetLog = log
			break
		}
	}

	if targetLog == nil {
		return fmt.Errorf("file %s not found in pending uploads", fileName)
	}

	log.Printf("[TEST] Found target file: %s, ID: %s, Status: %s",
		targetLog.FileName, targetLog.ID, targetLog.Status)

	// Create upload job
	uploadJob := types.UploadSFTPJob{
		TenantID:   targetLog.TenantID,
		FilePath:   targetLog.FilePath,
		FileName:   targetLog.FileName,
		RemotePath: targetLog.RemotePath,
		FileType:   targetLog.FileType,
		LocationID: targetLog.LocationID,
		CreatedAt:  time.Now(),
	}

	// Test upload
	result := s.uploadFileOptimized(uploadJob)

	log.Printf("[TEST] Upload result for %s: Success=%v, Error=%v",
		fileName, result.Success, result.Error)

	return result.Error
}

// verifyFileOnSFTP - check if file exists on SFTP server
func (s *sftpService) verifyFileOnSFTP(logEntry *entity.SFTPTransferLog) (bool, int64) {
	// Get tenant config
	tenantConfig, exists := config.GetTenantByID(logEntry.TenantID)
	if !exists {
		log.Printf("[VERIFY] Tenant %s not found", logEntry.TenantID)
		return false, 0
	}

	// Get SFTP connection with timeout
	conn, err := s.getConnectionWithTimeout(tenantConfig, 10*time.Second)
	if err != nil {
		log.Printf("[VERIFY] Failed to get SFTP connection for %s: %v", logEntry.FileName, err)
		return false, 0
	}

	// Check if file exists on remote server
	fileInfo, err := conn.sftpClient.Stat(logEntry.RemotePath)
	if err != nil {
		log.Printf("[VERIFY] File %s not found on SFTP at %s: %v", logEntry.FileName, logEntry.RemotePath, err)
		return false, 0
	}

	log.Printf("[VERIFY] ✅ File %s exists on SFTP (size: %d bytes)", logEntry.FileName, fileInfo.Size())
	return true, fileInfo.Size()
}

// UploadAllPendingFiles dengan improved logging dan error handling
func (s *sftpService) UploadAllPendingFiles(tenantID string) error {
	log.Printf("[SFTP] UploadAllPendingFiles called for tenant: %s", tenantID)

	pendingLogs, err := s.sftpLogRepo.GetPendingUploads(tenantID)
	if err != nil {
		log.Printf("[SFTP] ❌ Failed to get pending uploads: %v", err)
		return fmt.Errorf("failed to get pending uploads: %w", err)
	}

	log.Printf("[SFTP] Retrieved %d pending files from database", len(pendingLogs))

	if len(pendingLogs) == 0 {
		log.Printf("[SFTP] No pending uploads found for tenant %s", tenantID)
		return nil
	}

	// Debug: log sample files
	log.Printf("[SFTP] Sample pending files to process:")
	for i, logEntry := range pendingLogs {
		if i >= 5 {
			break
		}
		log.Printf("[SFTP] - %s (path: %s, created: %v)",
			logEntry.FileName, logEntry.FilePath, logEntry.CreatedAt)
	}

	// Validate files exist
	validJobs := []types.UploadSFTPJob{}
	missingFiles := 0

	for _, logEntry := range pendingLogs {
		if _, err := os.Stat(logEntry.FilePath); os.IsNotExist(err) {
			log.Printf("[SFTP] ❌ File missing: %s", logEntry.FilePath)
			errorMsg := fmt.Sprintf("File not found: %s", logEntry.FilePath)
			s.sftpLogRepo.UpdateStatus(logEntry.ID, "FAILED", &errorMsg)
			missingFiles++
			continue
		}

		validJobs = append(validJobs, types.UploadSFTPJob{
			TenantID:   logEntry.TenantID,
			FilePath:   logEntry.FilePath,
			FileName:   logEntry.FileName,
			RemotePath: logEntry.RemotePath,
			FileType:   logEntry.FileType,
			LocationID: logEntry.LocationID,
			CreatedAt:  time.Now(),
		})
	}

	log.Printf("[SFTP] File validation: %d valid, %d missing", len(validJobs), missingFiles)

	if len(validJobs) == 0 {
		log.Printf("[SFTP] No valid files to upload for tenant %s", tenantID)
		return nil
	}

	log.Printf("[SFTP] Starting batch upload for %d valid files", len(validJobs))
	return s.UploadFilesBatch(validJobs)
}

// UploadFilesBatch dengan improved concurrency dan monitoring
func (s *sftpService) UploadFilesBatch(jobs []types.UploadSFTPJob) error {
	if len(jobs) == 0 {
		return nil
	}

	log.Printf("[SFTP] Starting optimized batch upload for %d files", len(jobs))

	// Reduced concurrency untuk stability
	const maxConcurrency = 8
	semaphore := make(chan struct{}, maxConcurrency)
	var wg sync.WaitGroup

	resultChan := make(chan UploadResult, len(jobs))
	var successCount, failureCount int32
	startTime := time.Now()

	// Smaller batch size untuk better control
	batchSize := 25
	for i := 0; i < len(jobs); i += batchSize {
		end := i + batchSize
		if end > len(jobs) {
			end = len(jobs)
		}

		currentBatch := jobs[i:end]
		log.Printf("[SFTP] Processing batch %d-%d of %d files", i+1, end, len(jobs))

		// Process batch
		batchWg := sync.WaitGroup{}
		for _, job := range currentBatch {
			batchWg.Add(1)
			wg.Add(1)
			go func(job types.UploadSFTPJob) {
				defer wg.Done()
				defer batchWg.Done()

				// Acquire with longer timeout
				select {
				case semaphore <- struct{}{}:
					defer func() { <-semaphore }()
				case <-time.After(60 * time.Second):
					log.Printf("[SFTP] ❌ Semaphore timeout for %s", job.FileName)
					resultChan <- UploadResult{
						FileName: job.FileName,
						Success:  false,
						Error:    fmt.Errorf("timeout acquiring semaphore"),
					}
					return
				}

				result := s.uploadFileOptimized(job)
				resultChan <- result

				if result.Success {
					successCount++
				} else {
					failureCount++
				}
			}(job)
		}

		// Wait for current batch to complete
		batchWg.Wait()

		// Progress report
		elapsed := time.Since(startTime)
		rate := float64(end) / elapsed.Seconds()
		log.Printf("[SFTP] Batch %d-%d completed. Success: %d, Failed: %d, Rate: %.1f files/sec",
			i+1, end, successCount, failureCount, rate)

		// Delay between batches
		if end < len(jobs) {
			time.Sleep(1 * time.Second)
		}
	}

	// Wait for all to complete
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect final results
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

	if len(errors) > 0 && len(errors) <= 5 {
		for _, err := range errors {
			log.Printf("[SFTP] Upload error: %v", err)
		}
	}

	if failureCount > 0 {
		return fmt.Errorf("batch upload had %d failures out of %d total", failureCount, len(jobs))
	}

	return nil
}

// uploadFileOptimized dengan improved error handling dan logging
func (s *sftpService) uploadFileOptimized(job types.UploadSFTPJob) UploadResult {
	maxRetries := 2
	var uploadErr error

	// Get log entry at the beginning
	existingLog, logErr := s.sftpLogRepo.GetByFileName(job.FileName)
	if logErr != nil || existingLog == nil {
		err := fmt.Errorf("no log entry found for %s: %v", job.FileName, logErr)
		log.Printf("[SFTP] ERROR: %v", err)
		return UploadResult{
			FileName: job.FileName,
			Success:  false,
			Error:    err,
		}
	}

	for attempt := 1; attempt <= maxRetries; attempt++ {
		log.Printf("[SFTP] Upload attempt %d/%d for %s", attempt, maxRetries, job.FileName)

		uploadErr = s.performOptimizedUpload(job)
		if uploadErr == nil {
			// SUCCESS - update with retry
			if updateErr := s.sftpLogRepo.UpdateStatusWithRetry(existingLog.ID, "SUCCESS", nil, 3); updateErr != nil {
				log.Printf("[SFTP] WARN: Failed to update SUCCESS status for %s: %v", job.FileName, updateErr)
				// Don't fail the upload just because status update failed
			} else {
				log.Printf("[SFTP] ✅ SUCCESS: %s", job.FileName)
			}
			return UploadResult{FileName: job.FileName, Success: true, Error: nil}
		}

		// Log detailed error for each attempt
		errorCategory := utils.GetErrorCategory(uploadErr)
		log.Printf("[SFTP] ❌ Attempt %d failed for %s [%s]: %v", attempt, job.FileName, errorCategory, uploadErr)

		if attempt < maxRetries {
			delay := s.getRetryDelay(attempt, uploadErr)
			log.Printf("[SFTP] Retrying %s in %v...", job.FileName, delay)
			time.Sleep(delay)
		}
	}

	// FAILED after all retries - update with retry
	errorMsg := uploadErr.Error()
	if updateErr := s.sftpLogRepo.UpdateStatusWithRetry(existingLog.ID, "FAILED", &errorMsg, 3); updateErr != nil {
		log.Printf("[SFTP] WARN: Failed to update FAILED status for %s: %v", job.FileName, updateErr)
	} else {
		log.Printf("[SFTP] ❌ FINAL FAILURE: %s - %v", job.FileName, uploadErr)
	}

	return UploadResult{FileName: job.FileName, Success: false, Error: uploadErr}
}

// performOptimizedUpload dengan improved validation
func (s *sftpService) performOptimizedUpload(job types.UploadSFTPJob) error {
	// Validate tenant config
	tenantConfig, exists := config.GetTenantByID(job.TenantID)
	if !exists {
		return fmt.Errorf("tenant %s not found", job.TenantID)
	}

	// Validate file exists
	if _, err := os.Stat(job.FilePath); os.IsNotExist(err) {
		return fmt.Errorf("file does not exist: %s", job.FilePath)
	}

	// Perform upload
	err := s.performUploadWithConnection(tenantConfig, job.FilePath, job.RemotePath)
	if err != nil {
		errorCategory := utils.GetErrorCategory(err)
		log.Printf("[SFTP] Upload failed for %s - Category: %s, Error: %v",
			job.FileName, errorCategory, err)
	}

	return err
}

func (s *sftpService) getConnectionWithTimeout(tenantConfig *config.TenantConfig, timeout time.Duration) (*SFTPConnection, error) {
	done := make(chan struct {
		conn *SFTPConnection
		err  error
	}, 1)

	go func() {
		conn, err := s.getConnection(tenantConfig)
		done <- struct {
			conn *SFTPConnection
			err  error
		}{conn, err}
	}()

	select {
	case result := <-done:
		return result.conn, result.err
	case <-time.After(timeout):
		return nil, fmt.Errorf("connection timeout after %v", timeout)
	}
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

		pool, exists = s.connectionPools[tenantID]
		if !exists {
			pool = &SFTPConnectionPool{
				connections: make(map[string]*SFTPConnection),
				maxIdle:     10 * time.Minute,
				maxLifetime: 60 * time.Minute,
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

	sshConfig := &ssh.ClientConfig{
		User:            tenantConfig.SFTP.User,
		Auth:            []ssh.AuthMethod{},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         15 * time.Second,
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

func (s *sftpService) performUploadWithConnection(tenantConfig *config.TenantConfig, localPath, remotePath string) error {
	remotePath = strings.ReplaceAll(remotePath, "\\", "/")

	log.Printf("[SFTP] Uploading %s to %s", localPath, remotePath)

	conn, err := s.getConnection(tenantConfig)
	if err != nil {
		return fmt.Errorf("failed to get SFTP connection: %w", err)
	}

	localFile, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("failed to open local file: %w", err)
	}
	defer localFile.Close()

	remoteDir := filepath.Dir(remotePath)
	if remoteDir != "." && remoteDir != "/" {
		remoteDir = strings.ReplaceAll(remoteDir, "\\", "/")
		log.Printf("[SFTP] Creating remote directory: %s", remoteDir)

		if _, err := conn.sftpClient.Stat(remoteDir); err != nil {
			if err := conn.sftpClient.MkdirAll(remoteDir); err != nil {
				conn.mu.Lock()
				conn.isHealthy = false
				conn.mu.Unlock()
				return fmt.Errorf("failed to create remote directory %s: %w", remoteDir, err)
			}
		}
	}

	remoteFile, err := conn.sftpClient.OpenFile(remotePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC)
	if err != nil {
		log.Printf("[SFTP] OpenFile with TRUNC failed: %v, trying without TRUNC", err)
		remoteFile, err = conn.sftpClient.OpenFile(remotePath, os.O_WRONLY|os.O_CREATE)
		if err != nil {
			conn.mu.Lock()
			conn.isHealthy = false
			conn.mu.Unlock()

			if utils.IsConnectionError(err) {
				return fmt.Errorf("connection error while creating file %s: %w", remotePath, err)
			}
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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
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
	buffer := make([]byte, 256*1024)
	totalBytes := int64(0)
	lastProgressTime := time.Now()

	done := make(chan error, 1)

	go func() {
		for {
			select {
			case <-ctx.Done():
				done <- ctx.Err()
				return
			default:
				if conn, ok := src.(*os.File); ok {
					conn.SetReadDeadline(time.Now().Add(30 * time.Second))
				}

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

					if time.Since(lastProgressTime) > 30*time.Second {
						log.Printf("[SFTP] Upload progress: %d bytes transferred", totalBytes)
						lastProgressTime = time.Now()
					}
				}
			}
		}
	}()

	select {
	case <-ctx.Done():
		return fmt.Errorf("upload timeout after 10 minutes")
	case err := <-done:
		if err != nil {
			return err
		}
		if totalBytes > 1024*1024 {
			log.Printf("[SFTP] Upload completed successfully: %d bytes transferred", totalBytes)
		}
		return nil
	}
}

// UploadFile - existing method dengan improved error handling
func (s *sftpService) UploadFile(job types.UploadSFTPJob) error {
	log.Printf("[SFTP] Starting upload for tenant %s, file %s", job.TenantID, job.FileName)

	_, exists := config.GetTenantByID(job.TenantID)
	if !exists {
		return fmt.Errorf("tenant %s not found or disabled", job.TenantID)
	}

	// Use the optimized upload method
	result := s.uploadFileOptimized(job)

	if !result.Success {
		log.Printf("[SFTP] Upload failed for file %s: %v", job.FileName, result.Error)
		return result.Error
	}

	log.Printf("[SFTP] Upload successful for file %s", job.FileName)
	return nil
}

func (s *sftpService) getRetryDelay(attempt int, err error) time.Duration {
	baseDelay := time.Duration(attempt) * 2 * time.Second

	if utils.IsConnectionError(err) {
		return baseDelay * 2
	}
	if utils.IsTemporaryError(err) {
		return baseDelay / 2
	}

	return baseDelay
}
