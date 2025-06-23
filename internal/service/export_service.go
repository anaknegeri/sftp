package service

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"jarvist/sftp-service/internal/config"
	"jarvist/sftp-service/internal/domain/entity"
	"jarvist/sftp-service/internal/file"
	"jarvist/sftp-service/internal/queue"
	"jarvist/sftp-service/internal/repository"
	"jarvist/sftp-service/internal/types"
	"jarvist/sftp-service/pkg/utils"

	"github.com/google/uuid"
)

type ExportService interface {
	ExportDaily(tenantID string, date time.Time) error
	Export30Min(tenantID string, date time.Time) error
	ExportAllReport(tenantID string, date time.Time) error
	ExportByLocationID(tenantID, locationID string, date time.Time) error
	Export30MinByLocationID(tenantID, locationID string, triggerTime time.Time) error
	ExportAllReportByLocationID(tenantID, locationID string, date time.Time) error
}

type exportService struct {
	peopleRepo      repository.PeopleCountRepository
	sftpLogRepo     repository.SFTPLogRepository
	csvWriter       file.CSVWriter
	localPath       string
	jobQueue        queue.JobQueue
	processingMutex sync.RWMutex
	processingFiles map[string]time.Time
}

// NewExportService creates a new export service with queue integration
func NewExportService(
	peopleRepo repository.PeopleCountRepository,
	sftpLogRepo repository.SFTPLogRepository,
	csvWriter file.CSVWriter,
	localPath string,
	jobQueue queue.JobQueue,
) ExportService {
	return &exportService{
		peopleRepo:      peopleRepo,
		sftpLogRepo:     sftpLogRepo,
		csvWriter:       csvWriter,
		localPath:       localPath,
		jobQueue:        jobQueue,
		processingFiles: make(map[string]time.Time),
	}
}

// ExportAllReport exports both daily and 30-minute reports for all locations
func (s *exportService) ExportAllReport(tenantID string, date time.Time) error {
	log.Printf("[EXPORT] Starting complete export for tenant %s, date %s", tenantID, date.Format("2006-01-02"))

	locations, err := s.getLocations(tenantID)
	if err != nil {
		return err
	}

	successCount := 0
	for _, location := range locations {
		if err := s.processCompleteReport(tenantID, location, date); err != nil {
			log.Printf("[EXPORT] Failed to process location %s: %v", location.LocationCode, err)
			continue
		}
		successCount++
	}

	log.Printf("[EXPORT] Complete export finished for tenant %s: %d/%d locations processed successfully",
		tenantID, successCount, len(locations))
	return nil
}

// ExportDaily exports only daily reports for all locations
func (s *exportService) ExportDaily(tenantID string, date time.Time) error {
	log.Printf("[DAILY] Starting daily export for tenant %s, date %s", tenantID, date.Format("2006-01-02"))

	locations, err := s.getLocations(tenantID)
	if err != nil {
		return err
	}

	successCount := 0
	for _, location := range locations {
		if err := s.processDailyReport(tenantID, location, date); err != nil {
			log.Printf("[DAILY] Failed to process location %s: %v", location.LocationCode, err)
			continue
		}
		successCount++
	}

	log.Printf("[DAILY] Daily export completed for tenant %s: %d/%d locations processed successfully",
		tenantID, successCount, len(locations))
	return nil
}

// Export30Min exports cumulative 30-minute reports up to the trigger time
func (s *exportService) Export30Min(tenantID string, triggerTime time.Time) error {
	log.Printf("[30MIN] Starting 30-minute cumulative export for tenant %s, triggered at %s",
		tenantID, triggerTime.Format("2006-01-02 15:04"))

	locations, err := s.getLocations(tenantID)
	if err != nil {
		return err
	}

	successCount := 0
	for _, location := range locations {
		if err := s.process30MinReport(tenantID, location, triggerTime); err != nil {
			log.Printf("[30MIN] Failed to process location %s: %v", location.LocationCode, err)
			continue
		}
		successCount++
	}

	log.Printf("[30MIN] 30-minute cumulative export completed for tenant %s: %d/%d locations processed successfully",
		tenantID, successCount, len(locations))
	return nil
}

// ExportByLocationID exports daily report for a specific location
func (s *exportService) ExportByLocationID(tenantID, locationID string, date time.Time) error {
	log.Printf("[DAILY] Starting daily export for tenant %s, location %s, date %s",
		tenantID, locationID, date.Format("2006-01-02"))

	location, err := s.getLocationByID(tenantID, locationID)
	if err != nil {
		return err
	}

	if err := s.processDailyReport(tenantID, location, date); err != nil {
		return fmt.Errorf("failed to process daily report for location %s: %w", location.LocationCode, err)
	}

	log.Printf("[DAILY] Daily export completed successfully for location %s", location.LocationCode)
	return nil
}

// Export30MinByLocationID exports cumulative 30-minute report for a specific location
func (s *exportService) Export30MinByLocationID(tenantID, locationID string, triggerTime time.Time) error {
	log.Printf("[30MIN] Starting 30-minute cumulative export for tenant %s, location %s, triggered at %s",
		tenantID, locationID, triggerTime.Format("2006-01-02 15:04"))

	location, err := s.getLocationByID(tenantID, locationID)
	if err != nil {
		return err
	}

	if err := s.process30MinReport(tenantID, location, triggerTime); err != nil {
		return fmt.Errorf("failed to process 30-minute report for location %s: %w", location.LocationCode, err)
	}

	log.Printf("[30MIN] 30-minute cumulative export completed successfully for location %s", location.LocationCode)
	return nil
}

// ExportAllReportByLocationID exports both daily and all 30-minute reports for a specific location
func (s *exportService) ExportAllReportByLocationID(tenantID, locationID string, date time.Time) error {
	log.Printf("[COMPLETE] Starting complete export for tenant %s, location %s, date %s",
		tenantID, locationID, date.Format("2006-01-02"))

	location, err := s.getLocationByID(tenantID, locationID)
	if err != nil {
		return err
	}

	if err := s.processCompleteReport(tenantID, location, date); err != nil {
		return fmt.Errorf("failed to process complete report for location %s: %w", location.LocationCode, err)
	}

	log.Printf("[COMPLETE] Complete export finished successfully for location %s", location.LocationCode)
	return nil
}

// getLocations retrieves locations for a tenant with error handling
func (s *exportService) getLocations(tenantID string) ([]entity.Location, error) {
	locations, err := s.peopleRepo.GetLocations(tenantID)
	if err != nil {
		log.Printf("[ERROR] Failed to get locations for tenant %s: %v", tenantID, err)
		return nil, fmt.Errorf("failed to get locations: %w", err)
	}

	if len(locations) == 0 {
		log.Printf("[WARNING] No locations found for tenant %s", tenantID)
		return nil, fmt.Errorf("no locations found for tenant %s", tenantID)
	}

	log.Printf("[INFO] Found %d locations for tenant %s", len(locations), tenantID)
	return locations, nil
}

// processDailyReport handles daily report processing for a single location
// After creating CSV file, it saves log to database and queues upload job
func (s *exportService) processDailyReport(tenantID string, location entity.Location, date time.Time) error {
	jakartaTime := date.In(config.GetJakartaTimezone())
	locationProcessKey := fmt.Sprintf("%s_%s_%s_daily", tenantID, location.ID, jakartaTime.Format("20060102"))

	log.Printf("[DAILY] Processing location %s for date %s (Jakarta: %s)",
		location.LocationCode, date.Format("2006-01-02"), jakartaTime.Format("2006-01-02 15:04 MST"))

	// Check if this location+date is currently being processed
	if s.isFileCurrentlyProcessing(locationProcessKey) {
		log.Printf("[DAILY] Location %s for date %s is currently being processed, skipping",
			location.LocationCode, date.Format("2006-01-02"))
		return nil
	}

	if !s.markFileAsProcessing(locationProcessKey) {
		log.Printf("[DAILY] Location %s for date %s started processing by another worker, skipping",
			location.LocationCode, date.Format("2006-01-02"))
		return nil
	}
	defer s.unmarkFileAsProcessing(locationProcessKey)

	// Get reports from database
	reports, err := s.peopleRepo.GetReport(tenantID, location.ID, date)
	if err != nil {
		return fmt.Errorf("failed to get reports for location %s: %w", location.LocationCode, err)
	}

	if len(reports) == 0 {
		log.Printf("[DAILY] No data found for location %s on %s", location.LocationCode, jakartaTime.Format("2006-01-02"))
		return nil
	}

	// Write CSV file
	filePath, err := s.csvWriter.WriteDailyReport(tenantID, location.LocationCode, reports, date)
	if err != nil {
		return fmt.Errorf("failed to write daily CSV for location %s: %w", location.LocationCode, err)
	}

	recordCount := s.countFileRecords(filePath)
	log.Printf("[DAILY] Successfully created file: %s (%d records)", filePath, recordCount)

	// Save file log to database and queue upload job
	if err := s.saveFileAndQueueUpload(tenantID, location.ID, filePath); err != nil {
		log.Printf("[DAILY] Failed to queue upload for %s: %v", filePath, err)
		// Don't return error, file was created successfully
	}

	log.Printf("[DAILY] Daily export completed for location %s", location.LocationCode)
	return nil
}

// process30MinReport handles 30-minute cumulative report processing for a single location
// After creating CSV file, it saves log to database and queues upload job
func (s *exportService) process30MinReport(tenantID string, location entity.Location, triggerTime time.Time) error {
	jakartaTriggerTime := triggerTime.In(config.GetJakartaTimezone())
	currentWindow := jakartaTriggerTime.Truncate(30 * time.Minute)

	locationProcessKey := fmt.Sprintf("%s_%s_%s_30min", tenantID, location.ID, currentWindow.Format("20060102_1504"))

	log.Printf("[30MIN] Processing location %s for trigger time %s (Jakarta: %s)",
		location.LocationCode, triggerTime.Format("2006-01-02 15:04"), jakartaTriggerTime.Format("2006-01-02 15:04 MST"))

	// Check if this location+window is currently being processed
	if s.isFileCurrentlyProcessing(locationProcessKey) {
		log.Printf("[30MIN] Location %s for window %s is currently being processed, skipping", location.LocationCode, currentWindow.Format("15:04"))
		return nil
	}

	if !s.markFileAsProcessing(locationProcessKey) {
		log.Printf("[30MIN] Location %s for window %s started processing by another worker, skipping",
			location.LocationCode, currentWindow.Format("15:04"))
		return nil
	}
	defer s.unmarkFileAsProcessing(locationProcessKey)

	// Ensure today is in Jakarta timezone
	today := time.Date(jakartaTriggerTime.Year(), jakartaTriggerTime.Month(), jakartaTriggerTime.Day(), 0, 0, 0, 0, config.GetJakartaTimezone())

	// Get reports within time range
	allReports, err := s.peopleRepo.GetReportWithTimeRange(tenantID, location.ID, today, triggerTime)
	if err != nil {
		return fmt.Errorf("failed to get reports for location %s: %w", location.LocationCode, err)
	}

	if len(allReports) == 0 {
		log.Printf("[30MIN] No cumulative data found for location %s up to %s",
			location.LocationCode, jakartaTriggerTime.Format("15:04"))
		return nil
	}

	// Write CSV file
	filePath, err := s.csvWriter.Write30MinReport(tenantID, location.LocationCode, allReports, currentWindow)
	if err != nil {
		return fmt.Errorf("failed to write 30-minute cumulative report for location %s: %w", location.LocationCode, err)
	}

	recordCount := s.countFileRecords(filePath)
	log.Printf("[30MIN] Successfully created file: %s (%d records)", filePath, recordCount)

	// Save file log to database and queue upload job
	if err := s.saveFileAndQueueUpload(tenantID, location.ID, filePath); err != nil {
		log.Printf("[30MIN] Failed to queue upload for %s: %v", filePath, err)
		// Don't return error, file was created successfully
	}

	log.Printf("[30MIN] 30-minute export completed for location %s at window %s",
		location.LocationCode, currentWindow.Format("15:04"))
	return nil
}

// processCompleteReport handles both daily and all 30-minute window reports for a single location
func (s *exportService) processCompleteReport(tenantID string, location entity.Location, date time.Time) error {
	log.Printf("[COMPLETE] Processing complete report for location %s on date %s",
		location.LocationCode, date.Format("2006-01-02"))

	reports, err := s.peopleRepo.GetReport(tenantID, location.ID, date)
	if err != nil {
		return fmt.Errorf("failed to get reports for location %s: %w", location.LocationCode, err)
	}

	if len(reports) == 0 {
		log.Printf("[COMPLETE] No data found for location %s on %s", location.LocationCode, date.Format("2006-01-02"))
		return nil
	}

	// Write daily report and queue upload
	dailyFilePath, err := s.csvWriter.WriteDailyReport(tenantID, location.LocationCode, reports, date)
	if err != nil {
		return fmt.Errorf("failed to write daily CSV for location %s: %w", location.LocationCode, err)
	}

	dailyRecordCount := s.countFileRecords(dailyFilePath)
	log.Printf("[COMPLETE] Daily report created: %s (%d records)", dailyFilePath, dailyRecordCount)

	// Save daily file log and queue upload
	if err := s.saveFileAndQueueUpload(tenantID, location.ID, dailyFilePath); err != nil {
		log.Printf("[COMPLETE] Failed to queue upload for daily file %s: %v", dailyFilePath, err)
	}

	// Process 30-minute windows
	return s.process30MinWindows(tenantID, location, reports)
}

// process30MinWindows processes all 30-minute windows for the given reports
func (s *exportService) process30MinWindows(tenantID string, location entity.Location, reports []entity.DailyReport) error {
	windowsMap := s.groupReportsByWindows(reports)
	sortedWindows := s.sortWindows(windowsMap)

	if len(sortedWindows) == 0 {
		log.Printf("[30MIN] No 30-minute windows found for location %s", location.LocationCode)
		return nil
	}

	log.Printf("[30MIN] Processing %d windows for location %s", len(sortedWindows), location.LocationCode)

	successCount := 0
	for i, windowTime := range sortedWindows {
		cumulativeReports := s.filterCumulativeDataUpToWindow(reports, windowTime)
		if len(cumulativeReports) == 0 {
			log.Printf("[30MIN] No cumulative data for window %s at location %s, skipping",
				windowTime.Format("15:04"), location.LocationCode)
			continue
		}

		// Write 30-minute report
		filePath, err := s.csvWriter.Write30MinReport(tenantID, location.LocationCode, cumulativeReports, windowTime)
		if err != nil {
			log.Printf("[30MIN] Failed to write 30-minute report for window %s at location %s: %v",
				windowTime.Format("15:04"), location.LocationCode, err)
			continue
		}

		recordCount := s.countFileRecords(filePath)

		// Save file log and queue upload
		if err := s.saveFileAndQueueUpload(tenantID, location.ID, filePath); err != nil {
			log.Printf("[30MIN] Failed to queue upload for window file %s: %v", filePath, err)
		}

		successCount++

		// Log progress every 5 windows or at the last window
		if i%5 == 0 || i == len(sortedWindows)-1 {
			log.Printf("[30MIN] Progress for location %s: %d/%d windows completed (latest: %s with %d records)",
				location.LocationCode, i+1, len(sortedWindows), windowTime.Format("15:04"), recordCount)
		}
	}

	log.Printf("[30MIN] Completed processing %d/%d windows successfully for location %s",
		successCount, len(sortedWindows), location.LocationCode)
	return nil
}

// getLocationByID retrieves a specific location by ID directly from database
func (s *exportService) getLocationByID(tenantID, locationID string) (entity.Location, error) {
	location, err := s.peopleRepo.GetLocationByID(tenantID, locationID)
	if err != nil {
		log.Printf("[ERROR] Failed to get location %s for tenant %s: %v", locationID, tenantID, err)
		return entity.Location{}, err
	}

	log.Printf("[INFO] Found location %s (%s) for tenant %s", location.LocationCode, locationID, tenantID)
	return location, nil
}

// isFileCurrentlyProcessing checks if file is being processed by another goroutine
func (s *exportService) isFileCurrentlyProcessing(fileName string) bool {
	s.processingMutex.RLock()
	defer s.processingMutex.RUnlock()

	if lastProcessTime, exists := s.processingFiles[fileName]; exists {
		// If processing started less than 5 minutes ago, consider it active
		if time.Since(lastProcessTime) < 5*time.Minute {
			return true
		}
		// Clean up old entries
		delete(s.processingFiles, fileName)
	}
	return false
}

// markFileAsProcessing marks file as currently being processed
func (s *exportService) markFileAsProcessing(fileName string) bool {
	s.processingMutex.Lock()
	defer s.processingMutex.Unlock()

	// Double-check after acquiring write lock
	if lastProcessTime, exists := s.processingFiles[fileName]; exists {
		if time.Since(lastProcessTime) < 5*time.Minute {
			return false // Already being processed
		}
	}

	s.processingFiles[fileName] = time.Now()
	return true
}

// unmarkFileAsProcessing removes file from processing tracker
func (s *exportService) unmarkFileAsProcessing(fileName string) {
	s.processingMutex.Lock()
	defer s.processingMutex.Unlock()
	delete(s.processingFiles, fileName)
}

// saveFileAndQueueUpload saves file information to database with PENDING status
func (s *exportService) saveFileAndQueueUpload(tenantID, locationID, filePath string) error {
	fileName := filepath.Base(filePath)
	fileType := utils.DetermineFileType(fileName)

	// 1. Check if file is currently being processed by another goroutine
	if s.isFileCurrentlyProcessing(fileName) {
		log.Printf("[EXPORT] File %s is currently being processed by another worker, skipping", fileName)
		return nil
	}

	// 2. Mark file as being processed
	if !s.markFileAsProcessing(fileName) {
		log.Printf("[EXPORT] File %s started processing by another worker while checking, skipping", fileName)
		return nil
	}
	defer s.unmarkFileAsProcessing(fileName)

	// 3. Check database for recent uploads with more granular timing
	existingLog, err := s.sftpLogRepo.GetByFileName(fileName)
	if err != nil {
		log.Printf("[EXPORT] Failed to check existing log for %s: %v", fileName, err)
	}

	// 4. Enhanced duplicate prevention logic
	if existingLog != nil {
		timeSinceCreated := time.Since(existingLog.CreatedAt)

		switch existingLog.Status {
		case "SUCCESS":
			// Skip if successfully uploaded recently (30 seconds)
			if timeSinceCreated < 30*time.Second {
				log.Printf("[EXPORT] File %s was successfully uploaded %.1f seconds ago, skipping duplicate",
					fileName, timeSinceCreated.Seconds())
				return nil
			}
			// Allow repush for older successful uploads
			log.Printf("[EXPORT] File %s was uploaded %.1f minutes ago, proceeding with repush",
				fileName, timeSinceCreated.Minutes())

		case "PENDING":
			// Skip if pending upload is very recent (10 seconds)
			if timeSinceCreated < 10*time.Second {
				log.Printf("[EXPORT] File %s has PENDING upload from %.1f seconds ago, skipping duplicate", fileName, timeSinceCreated.Seconds())
				return nil
			}
			// Continue processing if pending for too long (might be stuck)
			log.Printf("[EXPORT] File %s has stale PENDING upload from %.1f minutes ago, retrying",
				fileName, timeSinceCreated.Minutes())

		case "FAILED":
			// Allow retry for failed uploads, but not too frequently
			if timeSinceCreated < 30*time.Second {
				log.Printf("[EXPORT] File %s failed upload %.1f seconds ago, too soon to retry", fileName, timeSinceCreated.Seconds())
				return nil
			}
			log.Printf("[EXPORT] File %s failed upload %.1f minutes ago, proceeding with retry", fileName, timeSinceCreated.Minutes())
		}
	}

	// 5. Get tenant configuration
	tenantConfig, exists := config.GetTenantByID(tenantID)
	if !exists {
		return fmt.Errorf("tenant %s not found or disabled", tenantID)
	}

	// 6. Get file information
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return fmt.Errorf("failed to get file info: %w", err)
	}

	recordCount := utils.CountFileRecords(filePath)
	remotePath := strings.ReplaceAll(filepath.Join(tenantConfig.SFTP.BasePath, fileName), "\\", "/")

	var transferLog *entity.SFTPTransferLog

	// 7. Handle existing PENDING logs smartly
	if existingLog != nil && existingLog.Status == "PENDING" && time.Since(existingLog.CreatedAt) > 10*time.Second {
		// Update existing stale PENDING log
		transferLog = existingLog
		transferLog.FilePath = filePath
		transferLog.RemotePath = remotePath
		transferLog.FileSize = fileInfo.Size()
		transferLog.RecordCount = &recordCount
		transferLog.TransferStartTime = time.Now()

		if err := s.sftpLogRepo.Update(transferLog); err != nil {
			log.Printf("[EXPORT] Failed to update existing PENDING log for %s: %v", fileName, err)
			return fmt.Errorf("failed to update transfer log: %w", err)
		}

		log.Printf("[EXPORT] Updated stale PENDING log for %s (was created %.1f minutes ago)",
			fileName, time.Since(existingLog.CreatedAt).Minutes())
	} else {
		// 8. Create new log entry with deduplication key
		transferLog = &entity.SFTPTransferLog{
			ID:                uuid.New().String(),
			TenantID:          tenantID,
			LocationID:        locationID,
			FileName:          fileName,
			FilePath:          filePath,
			RemotePath:        remotePath,
			Status:            "PENDING",
			FileSize:          fileInfo.Size(),
			TransferStartTime: time.Now(),
			RecordCount:       &recordCount,
			FileType:          fileType,
			CreatedAt:         time.Now(),
		}

		if err := s.sftpLogRepo.Create(transferLog); err != nil {
			// Handle potential duplicate key error gracefully
			if strings.Contains(err.Error(), "duplicate") || strings.Contains(err.Error(), "unique constraint") {
				log.Printf("[EXPORT] Duplicate log entry detected for %s, another process created it", fileName)
				return nil
			}
			log.Printf("[EXPORT] Failed to create transfer log for %s: %v", fileName, err)
			return fmt.Errorf("failed to create transfer log: %w", err)
		}

		logType := "initial"
		if existingLog != nil {
			logType = "repush"
		}
		log.Printf("[EXPORT] File log created (%s): %s (PENDING) with remote path: %s",
			logType, fileName, remotePath)
	}

	// 9. Create upload job with deduplication ID
	uploadJob := types.UploadSFTPJob{
		TenantID:   tenantID,
		FilePath:   filePath,
		FileName:   fileName,
		RemotePath: remotePath,
		FileType:   fileType,
		LocationID: locationID,
		CreatedAt:  time.Now(),
	}

	// 10. Publish upload job to queue with retry
	maxRetries := 3
	for attempt := 1; attempt <= maxRetries; attempt++ {
		if err := s.jobQueue.PublishJob(types.SubjectUploadSFTP, uploadJob); err != nil {
			log.Printf("[EXPORT] Failed to publish upload job for %s (attempt %d/%d): %v",
				fileName, attempt, maxRetries, err)
			if attempt == maxRetries {
				return fmt.Errorf("failed to publish upload job after %d attempts: %w", maxRetries, err)
			}
			time.Sleep(time.Duration(attempt) * time.Second)
			continue
		}
		break
	}

	log.Printf("[EXPORT] Upload job queued: %s (log ID: %s)", fileName, transferLog.ID)
	return nil
}

// groupReportsByWindows groups reports by their respective 30-minute windows
func (s *exportService) groupReportsByWindows(reports []entity.DailyReport) map[time.Time][]entity.DailyReport {
	windows := make(map[time.Time][]entity.DailyReport)

	for _, report := range reports {
		reportTime, err := time.Parse(time.RFC3339, report.Date)
		if err != nil {
			log.Printf("[WARNING] Failed to parse report date %s: %v", report.Date, err)
			continue
		}

		// Create 30-minute window boundary (next 30-minute mark)
		windowStart := reportTime.Truncate(30 * time.Minute).Add(30 * time.Minute)
		windows[windowStart] = append(windows[windowStart], report)
	}

	return windows
}

// sortWindows sorts window times in ascending order
func (s *exportService) sortWindows(windows map[time.Time][]entity.DailyReport) []time.Time {
	var sorted []time.Time
	for windowTime := range windows {
		sorted = append(sorted, windowTime)
	}

	// Use Go's built-in sort for better performance
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Before(sorted[j])
	})

	return sorted
}

// filterCumulativeDataUpToWindow filters reports up to and including the specified window time
// Ensures timezone consistency for proper filtering
func (s *exportService) filterCumulativeDataUpToWindow(allReports []entity.DailyReport, windowTime time.Time) []entity.DailyReport {
	var cumulativeReports []entity.DailyReport

	// Convert windowTime to Jakarta timezone for comparison
	jakartaWindowTime := windowTime.In(config.GetJakartaTimezone())
	for _, report := range allReports {
		reportTime, err := time.Parse(time.RFC3339, report.Date)
		if err != nil {
			log.Printf("[WARNING] Failed to parse report date %s for cumulative filtering: %v", report.Date, err)
			continue
		}

		// Convert reportTime to Jakarta timezone for comparison
		jakartaReportTime := reportTime.In(config.GetJakartaTimezone())

		if jakartaReportTime.Before(jakartaWindowTime) || jakartaReportTime.Equal(jakartaWindowTime) {
			cumulativeReports = append(cumulativeReports, report)
		}
	}

	return cumulativeReports
}

// countFileRecords counts the number of data records in a CSV file (excluding header)
func (s *exportService) countFileRecords(filePath string) int {
	file, err := os.Open(filePath)
	if err != nil {
		log.Printf("[WARNING] Failed to open file %s for record counting: %v", filePath, err)
		return 0
	}
	defer file.Close()

	lineCount := 0
	buffer := make([]byte, 32*1024) // 32KB buffer for efficient reading

	for {
		bytesRead, err := file.Read(buffer)
		if err != nil {
			break
		}

		for i := 0; i < bytesRead; i++ {
			if buffer[i] == '\n' {
				lineCount++
			}
		}
	}

	// Subtract 1 for header line if file has content
	if lineCount > 0 {
		lineCount--
	}

	return lineCount
}
