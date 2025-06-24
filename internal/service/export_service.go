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
	peopleRepo  repository.PeopleCountRepository
	sftpLogRepo repository.SFTPLogRepository
	csvWriter   file.CSVWriter
	localPath   string
	jobQueue    queue.JobQueue

	activeExports    sync.Map
	globalExportLock sync.Map // For tenant-wide operations
	fileGeneration   sync.Map // For preventing concurrent file generation
}

type ExportKey struct {
	TenantID   string
	LocationID string
	Date       string
	Type       string // "daily" or "30min"
	Window     string // for 30min reports: "0830", for daily: ""
}

func (ek ExportKey) String() string {
	if ek.Window != "" {
		return fmt.Sprintf("%s:%s:%s:%s:%s", ek.TenantID, ek.LocationID, ek.Date, ek.Type, ek.Window)
	}
	return fmt.Sprintf("%s:%s:%s:%s", ek.TenantID, ek.LocationID, ek.Date, ek.Type)
}

// Global export key for tenant-wide operations
func (ek ExportKey) GlobalString() string {
	return fmt.Sprintf("global:%s:%s:%s", ek.TenantID, ek.Date, ek.Type)
}

// File generation key to prevent concurrent file creation
func (ek ExportKey) FileGenKey() string {
	if ek.Window != "" {
		return fmt.Sprintf("file:%s:%s:%s:%s:%s", ek.TenantID, ek.LocationID, ek.Date, ek.Type, ek.Window)
	}
	return fmt.Sprintf("file:%s:%s:%s:%s", ek.TenantID, ek.LocationID, ek.Date, ek.Type)
}

func NewExportService(
	peopleRepo repository.PeopleCountRepository,
	sftpLogRepo repository.SFTPLogRepository,
	csvWriter file.CSVWriter,
	localPath string,
	jobQueue queue.JobQueue,
) ExportService {
	service := &exportService{
		peopleRepo:  peopleRepo,
		sftpLogRepo: sftpLogRepo,
		csvWriter:   csvWriter,
		localPath:   localPath,
		jobQueue:    jobQueue,
	}

	go service.cleanupExpiredExports()

	return service
}

func (s *exportService) cleanupExpiredExports() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		cutoff := time.Now().Add(-5 * time.Minute)

		// Cleanup active exports
		s.activeExports.Range(func(key, value interface{}) bool {
			if startTime, ok := value.(time.Time); ok {
				if startTime.Before(cutoff) {
					s.activeExports.Delete(key)
					log.Printf("[EXPORT] Cleaned up expired export lock: %s", key)
				}
			}
			return true
		})

		// Cleanup global locks
		s.globalExportLock.Range(func(key, value interface{}) bool {
			if startTime, ok := value.(time.Time); ok {
				if startTime.Before(cutoff) {
					s.globalExportLock.Delete(key)
					log.Printf("[EXPORT] Cleaned up expired global lock: %s", key)
				}
			}
			return true
		})

		// Cleanup file generation locks
		s.fileGeneration.Range(func(key, value interface{}) bool {
			if startTime, ok := value.(time.Time); ok {
				if startTime.Before(cutoff) {
					s.fileGeneration.Delete(key)
					log.Printf("[EXPORT] Cleaned up expired file generation lock: %s", key)
				}
			}
			return true
		})
	}
}

func (s *exportService) acquireExportLock(key ExportKey) bool {
	keyStr := key.String()

	if _, exists := s.activeExports.Load(keyStr); exists {
		log.Printf("[EXPORT] Export already in progress: %s", keyStr)
		return false
	}

	_, loaded := s.activeExports.LoadOrStore(keyStr, time.Now())
	if loaded {
		log.Printf("[EXPORT] Failed to acquire export lock: %s", keyStr)
		return false
	}

	log.Printf("[EXPORT] Acquired export lock: %s", keyStr)
	return true
}

func (s *exportService) releaseExportLock(key ExportKey) {
	keyStr := key.String()
	s.activeExports.Delete(keyStr)
	log.Printf("[EXPORT] Released export lock: %s", keyStr)
}

func (s *exportService) acquireGlobalExportLock(key ExportKey) bool {
	globalKey := key.GlobalString()

	if _, exists := s.globalExportLock.Load(globalKey); exists {
		log.Printf("[EXPORT] Global export already in progress: %s", globalKey)
		return false
	}

	_, loaded := s.globalExportLock.LoadOrStore(globalKey, time.Now())
	if loaded {
		log.Printf("[EXPORT] Failed to acquire global export lock: %s", globalKey)
		return false
	}

	log.Printf("[EXPORT] Acquired global export lock: %s", globalKey)
	return true
}

func (s *exportService) releaseGlobalExportLock(key ExportKey) {
	globalKey := key.GlobalString()
	s.globalExportLock.Delete(globalKey)
	log.Printf("[EXPORT] Released global export lock: %s", globalKey)
}

func (s *exportService) acquireFileGenerationLock(key ExportKey) bool {
	fileKey := key.FileGenKey()

	if _, exists := s.fileGeneration.Load(fileKey); exists {
		log.Printf("[EXPORT] File generation already in progress: %s", fileKey)
		return false
	}

	_, loaded := s.fileGeneration.LoadOrStore(fileKey, time.Now())
	if loaded {
		log.Printf("[EXPORT] Failed to acquire file generation lock: %s", fileKey)
		return false
	}

	log.Printf("[EXPORT] Acquired file generation lock: %s", fileKey)
	return true
}

func (s *exportService) releaseFileGenerationLock(key ExportKey) {
	fileKey := key.FileGenKey()
	s.fileGeneration.Delete(fileKey)
	log.Printf("[EXPORT] Released file generation lock: %s", fileKey)
}

func (s *exportService) ExportAllReport(tenantID string, date time.Time) error {
	log.Printf("[EXPORT] Starting complete export for tenant %s, date %s", tenantID, date.Format("2006-01-02"))

	// Acquire global lock for tenant-wide operations
	globalKey := ExportKey{
		TenantID: tenantID,
		Date:     date.Format("20060102"),
		Type:     "all",
	}

	if !s.acquireGlobalExportLock(globalKey) {
		return fmt.Errorf("another export operation is already running for tenant %s on date %s", tenantID, date.Format("2006-01-02"))
	}
	defer s.releaseGlobalExportLock(globalKey)

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

func (s *exportService) ExportDaily(tenantID string, date time.Time) error {
	log.Printf("[DAILY] Starting daily export for tenant %s, date %s", tenantID, date.Format("2006-01-02"))

	// Acquire global lock for tenant-wide daily operations
	globalKey := ExportKey{
		TenantID: tenantID,
		Date:     date.Format("20060102"),
		Type:     "daily",
	}

	if !s.acquireGlobalExportLock(globalKey) {
		return fmt.Errorf("another daily export operation is already running for tenant %s on date %s", tenantID, date.Format("2006-01-02"))
	}
	defer s.releaseGlobalExportLock(globalKey)

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

func (s *exportService) Export30Min(tenantID string, triggerTime time.Time) error {
	log.Printf("[30MIN] Starting 30-minute cumulative export for tenant %s, triggered at %s",
		tenantID, triggerTime.Format("2006-01-02 15:04"))

	// Acquire global lock for tenant-wide 30min operations
	globalKey := ExportKey{
		TenantID: tenantID,
		Date:     triggerTime.Format("20060102"),
		Type:     "30min",
	}

	if !s.acquireGlobalExportLock(globalKey) {
		return fmt.Errorf("another 30min export operation is already running for tenant %s", tenantID)
	}
	defer s.releaseGlobalExportLock(globalKey)

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

func (s *exportService) ExportByLocationID(tenantID, locationID string, date time.Time) error {
	log.Printf("[DAILY] Starting daily export for tenant %s, location %s, date %s",
		tenantID, locationID, date.Format("2006-01-02"))

	// Check if tenant-wide export is running
	globalKey := ExportKey{
		TenantID: tenantID,
		Date:     date.Format("20060102"),
		Type:     "daily",
	}

	if _, exists := s.globalExportLock.Load(globalKey.GlobalString()); exists {
		return fmt.Errorf("tenant-wide daily export is already running, please wait")
	}

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

func (s *exportService) Export30MinByLocationID(tenantID, locationID string, triggerTime time.Time) error {
	log.Printf("[30MIN] Starting 30-minute cumulative export for tenant %s, location %s, triggered at %s",
		tenantID, locationID, triggerTime.Format("2006-01-02 15:04"))

	// Check if tenant-wide export is running
	globalKey := ExportKey{
		TenantID: tenantID,
		Date:     triggerTime.Format("20060102"),
		Type:     "30min",
	}

	if _, exists := s.globalExportLock.Load(globalKey.GlobalString()); exists {
		return fmt.Errorf("tenant-wide 30min export is already running, please wait")
	}

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

func (s *exportService) ExportAllReportByLocationID(tenantID, locationID string, date time.Time) error {
	log.Printf("[COMPLETE] Starting complete export for tenant %s, location %s, date %s",
		tenantID, locationID, date.Format("2006-01-02"))

	// Check if tenant-wide export is running
	globalKey := ExportKey{
		TenantID: tenantID,
		Date:     date.Format("20060102"),
		Type:     "all",
	}

	if _, exists := s.globalExportLock.Load(globalKey.GlobalString()); exists {
		return fmt.Errorf("tenant-wide complete export is already running, please wait")
	}

	// Acquire lock for this specific location complete export
	locationKey := ExportKey{
		TenantID:   tenantID,
		LocationID: locationID,
		Date:       date.Format("20060102"),
		Type:       "complete",
	}

	if !s.acquireExportLock(locationKey) {
		return fmt.Errorf("complete export already in progress for location %s", locationID)
	}
	defer s.releaseExportLock(locationKey)

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

func (s *exportService) processDailyReport(tenantID string, location entity.Location, date time.Time) error {
	exportKey := ExportKey{
		TenantID:   tenantID,
		LocationID: location.ID,
		Date:       date.Format("20060102"),
		Type:       "daily",
	}

	if !s.acquireExportLock(exportKey) {
		log.Printf("[DAILY] Skipping duplicate export for location %s on %s", location.LocationCode, date.Format("2006-01-02"))
		return nil
	}
	defer s.releaseExportLock(exportKey)

	// Acquire file generation lock
	if !s.acquireFileGenerationLock(exportKey) {
		log.Printf("[DAILY] File generation already in progress for location %s", location.LocationCode)
		return nil
	}
	defer s.releaseFileGenerationLock(exportKey)

	log.Printf("[DAILY] Processing location %s for date %s", location.LocationCode, date.Format("2006-01-02"))

	reports, err := s.peopleRepo.GetReport(tenantID, location.ID, date)
	if err != nil {
		return fmt.Errorf("failed to get reports for location %s: %w", location.LocationCode, err)
	}

	if len(reports) == 0 {
		log.Printf("[DAILY] No data found for location %s on %s",
			location.LocationCode, date.Format("2006-01-02"))
		return nil
	}

	filePath, err := s.csvWriter.WriteDailyReport(tenantID, location.LocationCode, reports, date)
	if err != nil {
		return fmt.Errorf("failed to write daily CSV for location %s: %w", location.LocationCode, err)
	}

	// Validate file was created properly
	if !s.validateGeneratedFile(filePath) {
		return fmt.Errorf("generated file is invalid: %s", filePath)
	}

	recordCount := s.countFileRecords(filePath)
	log.Printf("[DAILY] Created file: %s (%d records)", filePath, recordCount)

	if err := s.createLogAndQueueUpload(tenantID, location.ID, filePath); err != nil {
		log.Printf("[DAILY] Failed to queue upload for %s: %v", filePath, err)
		return err
	}

	log.Printf("[DAILY] Daily export completed for location %s", location.LocationCode)
	return nil
}

func (s *exportService) process30MinReport(tenantID string, location entity.Location, triggerTime time.Time) error {
	jakartaTriggerTime := triggerTime.In(config.GetJakartaTimezone())
	currentWindow := jakartaTriggerTime.Truncate(30 * time.Minute).Add(30 * time.Minute)

	exportKey := ExportKey{
		TenantID:   tenantID,
		LocationID: location.ID,
		Date:       jakartaTriggerTime.Format("20060102"),
		Type:       "30min",
		Window:     currentWindow.Format("1504"),
	}

	if !s.acquireExportLock(exportKey) {
		log.Printf("[30MIN] Skipping duplicate export for location %s window %s",
			location.LocationCode, currentWindow.Format("15:04"))
		return nil
	}
	defer s.releaseExportLock(exportKey)

	// Acquire file generation lock
	if !s.acquireFileGenerationLock(exportKey) {
		log.Printf("[30MIN] File generation already in progress for location %s window %s",
			location.LocationCode, currentWindow.Format("15:04"))
		return nil
	}
	defer s.releaseFileGenerationLock(exportKey)

	log.Printf("[30MIN] Processing location %s for window %s",
		location.LocationCode, currentWindow.Format("15:04"))

	today := time.Date(jakartaTriggerTime.Year(), jakartaTriggerTime.Month(), jakartaTriggerTime.Day(),
		0, 0, 0, 0, config.GetJakartaTimezone())

	allReports, err := s.peopleRepo.GetReportWithTimeRange(tenantID, location.ID, today, triggerTime)
	if err != nil {
		return fmt.Errorf("failed to get reports for location %s: %w", location.LocationCode, err)
	}

	if len(allReports) == 0 {
		log.Printf("[30MIN] No cumulative data found for location %s up to %s",
			location.LocationCode, jakartaTriggerTime.Format("15:04"))
		return nil
	}

	filePath, err := s.csvWriter.Write30MinReport(tenantID, location.LocationCode, allReports, currentWindow)
	if err != nil {
		return fmt.Errorf("failed to write 30-minute cumulative report for location %s: %w", location.LocationCode, err)
	}

	// Validate file was created properly
	if !s.validateGeneratedFile(filePath) {
		return fmt.Errorf("generated file is invalid: %s", filePath)
	}

	recordCount := s.countFileRecords(filePath)
	log.Printf("[30MIN] Created file: %s (%d records)", filePath, recordCount)

	if err := s.createLogAndQueueUpload(tenantID, location.ID, filePath); err != nil {
		log.Printf("[30MIN] Failed to queue upload for %s: %v", filePath, err)
		return err
	}

	log.Printf("[30MIN] 30-minute export completed for location %s at window %s",
		location.LocationCode, currentWindow.Format("15:04"))
	return nil
}

func (s *exportService) processCompleteReport(tenantID string, location entity.Location, date time.Time) error {
	log.Printf("[COMPLETE] Processing complete report for location %s on date %s",
		location.LocationCode, date.Format("2006-01-02"))

	// Process daily report first
	if err := s.processDailyReport(tenantID, location, date); err != nil {
		return fmt.Errorf("failed to process daily report: %w", err)
	}

	// Get reports for 30min processing
	reports, err := s.peopleRepo.GetReport(tenantID, location.ID, date)
	if err != nil {
		return fmt.Errorf("failed to get reports for location %s: %w", location.LocationCode, err)
	}

	if len(reports) == 0 {
		log.Printf("[COMPLETE] No data found for location %s on %s",
			location.LocationCode, date.Format("2006-01-02"))
		return nil
	}

	return s.process30MinWindows(tenantID, location, reports)
}

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
		// Create export key for this window
		exportKey := ExportKey{
			TenantID:   tenantID,
			LocationID: location.ID,
			Date:       windowTime.Format("20060102"),
			Type:       "30min",
			Window:     windowTime.Format("1504"),
		}

		// Acquire file generation lock for this window
		if !s.acquireFileGenerationLock(exportKey) {
			log.Printf("[30MIN] File generation already in progress for window %s at location %s",
				windowTime.Format("15:04"), location.LocationCode)
			continue
		}

		cumulativeReports := s.filterCumulativeDataUpToWindow(reports, windowTime)
		if len(cumulativeReports) == 0 {
			log.Printf("[30MIN] No cumulative data for window %s at location %s, skipping",
				windowTime.Format("15:04"), location.LocationCode)
			s.releaseFileGenerationLock(exportKey)
			continue
		}

		filePath, err := s.csvWriter.Write30MinReport(tenantID, location.LocationCode, cumulativeReports, windowTime)
		if err != nil {
			log.Printf("[30MIN] Failed to write 30-minute report for window %s at location %s: %v",
				windowTime.Format("15:04"), location.LocationCode, err)
			s.releaseFileGenerationLock(exportKey)
			continue
		}

		// Validate file
		if !s.validateGeneratedFile(filePath) {
			log.Printf("[30MIN] Generated file is invalid for window %s: %s", windowTime.Format("15:04"), filePath)
			s.releaseFileGenerationLock(exportKey)
			continue
		}

		recordCount := s.countFileRecords(filePath)

		if err := s.createLogAndQueueUpload(tenantID, location.ID, filePath); err != nil {
			log.Printf("[30MIN] Failed to queue upload for window file %s: %v", filePath, err)
		}

		s.releaseFileGenerationLock(exportKey)
		successCount++

		if i%5 == 0 || i == len(sortedWindows)-1 {
			log.Printf("[30MIN] Progress for location %s: %d/%d windows completed (latest: %s with %d records)",
				location.LocationCode, i+1, len(sortedWindows), windowTime.Format("15:04"), recordCount)
		}
	}

	log.Printf("[30MIN] Completed processing %d/%d windows successfully for location %s",
		successCount, len(sortedWindows), location.LocationCode)
	return nil
}

// Validate that the generated file is valid
func (s *exportService) validateGeneratedFile(filePath string) bool {
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		log.Printf("[ERROR] Failed to stat file %s: %v", filePath, err)
		return false
	}

	if fileInfo.Size() == 0 {
		log.Printf("[ERROR] Generated file is empty: %s", filePath)
		return false
	}

	// Check if file is readable
	file, err := os.Open(filePath)
	if err != nil {
		log.Printf("[ERROR] Failed to open file %s: %v", filePath, err)
		return false
	}
	defer file.Close()

	// Read first few bytes to ensure file is not corrupted
	buffer := make([]byte, 100)
	_, err = file.Read(buffer)
	if err != nil {
		log.Printf("[ERROR] Failed to read file %s: %v", filePath, err)
		return false
	}

	return true
}

func (s *exportService) getLocationByID(tenantID, locationID string) (entity.Location, error) {
	location, err := s.peopleRepo.GetLocationByID(tenantID, locationID)
	if err != nil {
		log.Printf("[ERROR] Failed to get location %s for tenant %s: %v", locationID, tenantID, err)
		return entity.Location{}, err
	}

	log.Printf("[INFO] Found location %s (%s) for tenant %s", location.LocationCode, locationID, tenantID)
	return location, nil
}

func (s *exportService) createLogAndQueueUpload(tenantID, locationID, filePath string) error {
	fileName := filepath.Base(filePath)
	fileType := utils.DetermineFileType(fileName)

	log.Printf("[EXPORT] Creating log and queuing upload for: %s", fileName)

	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return fmt.Errorf("failed to get file info: %w", err)
	}

	if fileInfo.Size() == 0 {
		return fmt.Errorf("file is empty: %s", filePath)
	}

	tenantConfig, exists := config.GetTenantByID(tenantID)
	if !exists {
		return fmt.Errorf("tenant %s not found or disabled", tenantID)
	}

	recordCount := utils.CountFileRecords(filePath)
	remotePath := strings.ReplaceAll(filepath.Join(tenantConfig.SFTP.BasePath, fileName), "\\", "/")

	// Enhanced duplicate check - check both by filename and by content characteristics
	if s.isDuplicateUpload(tenantID, locationID, fileName, fileInfo.Size(), recordCount) {
		log.Printf("[EXPORT] Skipping duplicate upload: %s (size: %d, records: %d)",
			fileName, fileInfo.Size(), recordCount)
		return nil
	}

	// Create log entry
	transferLog := &entity.SFTPTransferLog{
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
		log.Printf("[EXPORT] Failed to create transfer log for %s: %v", fileName, err)
		return fmt.Errorf("failed to create transfer log: %w", err)
	}

	log.Printf("[EXPORT] Created log entry: %s (ID: %s, size: %d, records: %d)",
		fileName, transferLog.ID, fileInfo.Size(), recordCount)

	// Create upload job with log ID
	uploadJob := types.UploadSFTPJob{
		LogID:      transferLog.ID,
		TenantID:   tenantID,
		FilePath:   filePath,
		FileName:   fileName,
		RemotePath: remotePath,
		FileType:   fileType,
		LocationID: locationID,
		CreatedAt:  time.Now(),
	}

	if err := s.jobQueue.PublishJob(types.SubjectUploadSFTP, uploadJob); err != nil {
		errorMsg := fmt.Sprintf("Failed to queue upload job: %v", err)
		s.sftpLogRepo.UpdateStatus(transferLog.ID, "FAILED", &errorMsg)

		log.Printf("[EXPORT] Failed to queue upload job for %s: %v", fileName, err)
		return fmt.Errorf("failed to publish upload job: %w", err)
	}

	log.Printf("[EXPORT] Upload job queued: %s (log ID: %s)", fileName, transferLog.ID)
	return nil
}

func (s *exportService) isDuplicateUpload(tenantID, locationID, fileName string, fileSize int64, recordCount int) bool {
	// Check recent uploads for this specific file
	recentLogs, err := s.sftpLogRepo.GetRecentByFileName(fileName, 10*time.Minute)
	if err != nil {
		log.Printf("[EXPORT] Failed to check recent uploads: %v", err)
		return false
	}

	for _, recentLog := range recentLogs {
		if recentLog.TenantID == tenantID &&
			recentLog.LocationID == locationID {

			timeDiff := time.Since(recentLog.CreatedAt)

			// If there's a recent upload with same characteristics
			if recentLog.FileSize == fileSize &&
				recentLog.RecordCount != nil &&
				*recentLog.RecordCount == recordCount &&
				timeDiff < 5*time.Minute {

				log.Printf("[EXPORT] Found identical recent upload: %s (%.1f seconds ago, status: %s, size: %d, records: %d)",
					fileName, timeDiff.Seconds(), recentLog.Status, recentLog.FileSize, *recentLog.RecordCount)
				return true
			}

			// If there's a very recent upload (regardless of size) - might be concurrent
			if timeDiff < 30*time.Second {
				log.Printf("[EXPORT] Found very recent upload: %s (%.1f seconds ago, status: %s) - treating as duplicate",
					fileName, timeDiff.Seconds(), recentLog.Status)
				return true
			}

			// If there's a completed upload with non-zero size within time window
			if recentLog.Status == "COMPLETED" &&
				recentLog.FileSize > 0 &&
				timeDiff < 2*time.Minute {

				log.Printf("[EXPORT] Found recent completed upload: %s (%.1f seconds ago, size: %d)",
					fileName, timeDiff.Seconds(), recentLog.FileSize)
				return true
			}
		}
	}

	return false
}

func (s *exportService) groupReportsByWindows(reports []entity.DailyReport) map[time.Time][]entity.DailyReport {
	windows := make(map[time.Time][]entity.DailyReport)

	for _, report := range reports {
		reportTime, err := time.Parse(time.RFC3339, report.Date)
		if err != nil {
			log.Printf("[WARNING] Failed to parse report date %s: %v", report.Date, err)
			continue
		}

		windowStart := reportTime.Truncate(30 * time.Minute).Add(30 * time.Minute)
		windows[windowStart] = append(windows[windowStart], report)
	}

	return windows
}

func (s *exportService) sortWindows(windows map[time.Time][]entity.DailyReport) []time.Time {
	var sorted []time.Time
	for windowTime := range windows {
		sorted = append(sorted, windowTime)
	}

	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Before(sorted[j])
	})

	return sorted
}

func (s *exportService) filterCumulativeDataUpToWindow(allReports []entity.DailyReport, windowTime time.Time) []entity.DailyReport {
	var cumulativeReports []entity.DailyReport

	jakartaWindowTime := windowTime.In(config.GetJakartaTimezone())
	for _, report := range allReports {
		reportTime, err := time.Parse(time.RFC3339, report.Date)
		if err != nil {
			log.Printf("[WARNING] Failed to parse report date %s for cumulative filtering: %v", report.Date, err)
			continue
		}

		jakartaReportTime := reportTime.In(config.GetJakartaTimezone())

		if jakartaReportTime.Before(jakartaWindowTime) || jakartaReportTime.Equal(jakartaWindowTime) {
			cumulativeReports = append(cumulativeReports, report)
		}
	}

	return cumulativeReports
}

func (s *exportService) countFileRecords(filePath string) int {
	file, err := os.Open(filePath)
	if err != nil {
		log.Printf("[WARNING] Failed to open file %s for record counting: %v", filePath, err)
		return 0
	}
	defer file.Close()

	lineCount := 0
	buffer := make([]byte, 32*1024)

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

	// Subtract header line if exists
	if lineCount > 0 {
		lineCount--
	}

	return lineCount
}
