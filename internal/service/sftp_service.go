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
	sftpLogRepo     repository.SFTPLogRepository
	localPath       string
	connectionPools map[string]*SFTPConnectionPool
	poolMutex       sync.RWMutex
	cleanupTicker   *time.Ticker
	stopCleanup     chan struct{}
}

type UploadResult struct {
	LogID    string
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

	service.cleanupTicker = time.NewTicker(2 * time.Minute)
	go service.cleanupIdleConnections()

	return service
}

func (s *sftpService) UploadFile(job types.UploadSFTPJob) error {
	log.Printf("[SFTP] Starting upload for file %s (log ID: %s)", job.FileName, job.LogID)

	_, exists := config.GetTenantByID(job.TenantID)
	if !exists {
		return fmt.Errorf("tenant %s not found or disabled", job.TenantID)
	}

	result := s.uploadFileWithLogID(job)

	if !result.Success {
		log.Printf("[SFTP] Upload failed for file %s: %v", job.FileName, result.Error)
		return result.Error
	}

	log.Printf("[SFTP] Upload successful for file %s", job.FileName)
	return nil
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

	// Convert logs to jobs
	validJobs := []types.UploadSFTPJob{}
	missingFiles := 0

	for _, logEntry := range pendingLogs {
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

	log.Printf("[SFTP] File validation: %d valid, %d missing", len(validJobs), missingFiles)

	if len(validJobs) == 0 {
		log.Printf("[SFTP] No valid files to upload for tenant %s", tenantID)
		return nil
	}

	log.Printf("[SFTP] Starting batch upload for %d valid files", len(validJobs))
	return s.UploadFilesBatch(validJobs)
}

func (s *sftpService) UploadFilesBatch(jobs []types.UploadSFTPJob) error {
	if len(jobs) == 0 {
		return nil
	}

	log.Printf("[SFTP] Starting batch upload for %d files", len(jobs))

	const maxConcurrency = 8
	semaphore := make(chan struct{}, maxConcurrency)
	var wg sync.WaitGroup

	resultChan := make(chan UploadResult, len(jobs))
	var successCount, failureCount int32
	startTime := time.Now()

	batchSize := 15
	for i := 0; i < len(jobs); i += batchSize {
		end := i + batchSize
		if end > len(jobs) {
			end = len(jobs)
		}

		currentBatch := jobs[i:end]
		log.Printf("[SFTP] Processing batch %d-%d of %d files", i+1, end, len(jobs))

		batchWg := sync.WaitGroup{}
		for _, job := range currentBatch {
			batchWg.Add(1)
			wg.Add(1)
			go func(job types.UploadSFTPJob) {
				defer wg.Done()
				defer batchWg.Done()

				select {
				case semaphore <- struct{}{}:
					defer func() { <-semaphore }()
				case <-time.After(60 * time.Second):
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
					successCount++
				} else {
					failureCount++
				}
			}(job)
		}

		batchWg.Wait()

		elapsed := time.Since(startTime)
		rate := float64(end) / elapsed.Seconds()
		log.Printf("[SFTP] Batch %d-%d completed. Success: %d, Failed: %d, Rate: %.1f files/sec",
			i+1, end, successCount, failureCount, rate)

		if end < len(jobs) {
			time.Sleep(1 * time.Second)
		}
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

func (s *sftpService) uploadFileWithLogID(job types.UploadSFTPJob) UploadResult {
	maxRetries := 2
	var uploadErr error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		log.Printf("[SFTP] Upload attempt %d/%d for %s (log ID: %s)",
			attempt, maxRetries, job.FileName, job.LogID)

		uploadErr = s.performUpload(job)
		if uploadErr == nil {
			// SUCCESS - update status using LogID
			if updateErr := s.sftpLogRepo.UpdateStatus(job.LogID, "SUCCESS", nil); updateErr != nil {
				log.Printf("[SFTP] WARN: Failed to update SUCCESS status for %s: %v", job.FileName, updateErr)
			} else {
				log.Printf("[SFTP] ✅ SUCCESS: %s (log ID: %s)", job.FileName, job.LogID)
			}
			return UploadResult{LogID: job.LogID, FileName: job.FileName, Success: true, Error: nil}
		}

		errorCategory := utils.GetErrorCategory(uploadErr)
		log.Printf("[SFTP] ❌ Attempt %d failed for %s [%s]: %v",
			attempt, job.FileName, errorCategory, uploadErr)

		if attempt < maxRetries {
			delay := s.getRetryDelay(attempt, uploadErr)
			log.Printf("[SFTP] Retrying %s in %v...", job.FileName, delay)
			time.Sleep(delay)
		}
	}

	// FAILED after all retries - update status using LogID
	errorMsg := uploadErr.Error()
	if updateErr := s.sftpLogRepo.UpdateStatus(job.LogID, "FAILED", &errorMsg); updateErr != nil {
		log.Printf("[SFTP] WARN: Failed to update FAILED status for %s: %v", job.FileName, updateErr)
	} else {
		log.Printf("[SFTP] ❌ FINAL FAILURE: %s - %v (log ID: %s)", job.FileName, uploadErr, job.LogID)
	}

	return UploadResult{LogID: job.LogID, FileName: job.FileName, Success: false, Error: uploadErr}
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
	baseDelay := time.Duration(attempt) * 2 * time.Second

	if utils.IsConnectionError(err) {
		return baseDelay * 2
	}
	if utils.IsTemporaryError(err) {
		return baseDelay / 2
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
