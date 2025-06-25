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
	globalExportLock sync.Map
	fileGeneration   sync.Map
}

type ExportKey struct {
	TenantID   string
	LocationID string
	Date       string
	Type       string
	Window     string
}

func (ek ExportKey) String() string {
	if ek.Window != "" {
		return fmt.Sprintf("%s:%s:%s:%s:%s", ek.TenantID, ek.LocationID, ek.Date, ek.Type, ek.Window)
	}
	return fmt.Sprintf("%s:%s:%s:%s", ek.TenantID, ek.LocationID, ek.Date, ek.Type)
}

func (ek ExportKey) GlobalString() string {
	return fmt.Sprintf("global:%s:%s:%s", ek.TenantID, ek.Date, ek.Type)
}

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
	ticker := time.NewTicker(5 * time.Minute) // Reduced frequency
	defer ticker.Stop()

	for range ticker.C {
		cutoff := time.Now().Add(-10 * time.Minute) // Longer timeout
		cleanedCount := 0

		// Cleanup active exports
		s.activeExports.Range(func(key, value interface{}) bool {
			if startTime, ok := value.(time.Time); ok && startTime.Before(cutoff) {
				s.activeExports.Delete(key)
				cleanedCount++
			}
			return true
		})

		// Cleanup global locks
		s.globalExportLock.Range(func(key, value interface{}) bool {
			if startTime, ok := value.(time.Time); ok && startTime.Before(cutoff) {
				s.globalExportLock.Delete(key)
				cleanedCount++
			}
			return true
		})

		// Cleanup file generation locks
		s.fileGeneration.Range(func(key, value interface{}) bool {
			if startTime, ok := value.(time.Time); ok && startTime.Before(cutoff) {
				s.fileGeneration.Delete(key)
				cleanedCount++
			}
			return true
		})

		if cleanedCount > 0 {
			log.Printf("[EXPORT] Cleaned up %d expired locks", cleanedCount)
		}
	}
}

func (s *exportService) acquireExportLock(key ExportKey) bool {
	keyStr := key.String()
	if _, exists := s.activeExports.Load(keyStr); exists {
		return false
	}
	_, loaded := s.activeExports.LoadOrStore(keyStr, time.Now())
	return !loaded
}

func (s *exportService) releaseExportLock(key ExportKey) {
	s.activeExports.Delete(key.String())
}

func (s *exportService) acquireGlobalExportLock(key ExportKey) bool {
	globalKey := key.GlobalString()
	if _, exists := s.globalExportLock.Load(globalKey); exists {
		return false
	}
	_, loaded := s.globalExportLock.LoadOrStore(globalKey, time.Now())
	return !loaded
}

func (s *exportService) releaseGlobalExportLock(key ExportKey) {
	s.globalExportLock.Delete(key.GlobalString())
}

func (s *exportService) acquireFileGenerationLock(key ExportKey) bool {
	fileKey := key.FileGenKey()
	if _, exists := s.fileGeneration.Load(fileKey); exists {
		return false
	}
	_, loaded := s.fileGeneration.LoadOrStore(fileKey, time.Now())
	return !loaded
}

func (s *exportService) releaseFileGenerationLock(key ExportKey) {
	s.fileGeneration.Delete(key.FileGenKey())
}

func (s *exportService) ExportAllReport(tenantID string, date time.Time) error {
	log.Printf("[EXPORT] Starting complete export for tenant %s, date %s", tenantID, date.Format("2006-01-02"))

	globalKey := ExportKey{
		TenantID: tenantID,
		Date:     date.Format("20060102"),
		Type:     "all",
	}

	if !s.acquireGlobalExportLock(globalKey) {
		return fmt.Errorf("another export operation is already running for tenant %s", tenantID)
	}
	defer s.releaseGlobalExportLock(globalKey)

	locations, err := s.getLocations(tenantID)
	if err != nil {
		return err
	}

	allReports, err := s.peopleRepo.GetAllReportsForTenant(tenantID, date)
	if err != nil {
		return fmt.Errorf("failed to get all reports: %w", err)
	}

	if len(allReports) == 0 {
		log.Printf("[EXPORT] No data found for tenant %s", tenantID)
		return nil
	}

	reportsByLocation := s.groupReportsByLocation(allReports)
	successCount := 0

	for _, location := range locations {
		if locationReports, exists := reportsByLocation[location.ID]; exists && len(locationReports) > 0 {
			if err := s.processCompleteReportWithData(tenantID, location, date, locationReports); err != nil {
				log.Printf("[EXPORT] Failed location %s: %v", location.LocationCode, err)
				continue
			}
			successCount++
		}
	}

	log.Printf("[EXPORT] Complete export finished: %d/%d locations successful", successCount, len(locations))
	return nil
}

func (s *exportService) ExportDaily(tenantID string, date time.Time) error {
	log.Printf("[DAILY] Starting export for tenant %s, date %s", tenantID, date.Format("2006-01-02"))

	globalKey := ExportKey{
		TenantID: tenantID,
		Date:     date.Format("20060102"),
		Type:     "daily",
	}

	if !s.acquireGlobalExportLock(globalKey) {
		return fmt.Errorf("another daily export is already running for tenant %s", tenantID)
	}
	defer s.releaseGlobalExportLock(globalKey)

	locations, err := s.getLocations(tenantID)
	if err != nil {
		return err
	}

	allReports, err := s.peopleRepo.GetAllReportsForTenant(tenantID, date)
	if err != nil {
		return fmt.Errorf("failed to get all reports: %w", err)
	}

	if len(allReports) == 0 {
		log.Printf("[DAILY] No data found for tenant %s", tenantID)
		return nil
	}

	reportsByLocation := s.groupReportsByLocation(allReports)
	successCount := 0

	for _, location := range locations {
		if locationReports, exists := reportsByLocation[location.ID]; exists && len(locationReports) > 0 {
			if err := s.processDailyReportWithData(tenantID, location, date, locationReports); err != nil {
				log.Printf("[DAILY] Failed location %s: %v", location.LocationCode, err)
				continue
			}
			successCount++
		}
	}

	log.Printf("[DAILY] Export completed: %d/%d locations successful", successCount, len(locations))
	return nil
}

func (s *exportService) Export30Min(tenantID string, triggerTime time.Time) error {
	log.Printf("[30MIN] Starting export for tenant %s, time %s", tenantID, triggerTime.Format("15:04"))

	globalKey := ExportKey{
		TenantID: tenantID,
		Date:     triggerTime.Format("20060102"),
		Type:     "30min",
	}

	if !s.acquireGlobalExportLock(globalKey) {
		return fmt.Errorf("another 30min export is already running for tenant %s", tenantID)
	}
	defer s.releaseGlobalExportLock(globalKey)

	locations, err := s.getLocations(tenantID)
	if err != nil {
		return err
	}

	// Use business hours range
	jakartaTriggerTime := triggerTime.In(config.GetJakartaTimezone())
	businessHoursRange := config.BusinessHours(jakartaTriggerTime)

	startTime := businessHoursRange.StartTime
	endTime := triggerTime
	if triggerTime.After(businessHoursRange.EndTime) {
		endTime = businessHoursRange.EndTime
	}

	allReports, err := s.peopleRepo.GetAllReportsForTenantWithTimeRange(tenantID, startTime, endTime)
	if err != nil {
		return fmt.Errorf("failed to get all reports: %w", err)
	}

	if len(allReports) == 0 {
		log.Printf("[30MIN] No data found for tenant %s", tenantID)
		return nil
	}

	reportsByLocation := s.groupReportsByLocation(allReports)
	successCount := 0

	for _, location := range locations {
		if locationReports, exists := reportsByLocation[location.ID]; exists && len(locationReports) > 0 {
			if err := s.process30MinReportWithData(tenantID, location, triggerTime, locationReports); err != nil {
				log.Printf("[30MIN] Failed location %s: %v", location.LocationCode, err)
				continue
			}
			successCount++
		}
	}

	log.Printf("[30MIN] Export completed: %d/%d locations successful", successCount, len(locations))
	return nil
}

func (s *exportService) ExportByLocationID(tenantID, locationID string, date time.Time) error {
	globalKey := ExportKey{
		TenantID: tenantID,
		Date:     date.Format("20060102"),
		Type:     "daily",
	}

	if _, exists := s.globalExportLock.Load(globalKey.GlobalString()); exists {
		return fmt.Errorf("tenant-wide daily export is running, please wait")
	}

	location, err := s.getLocationByID(tenantID, locationID)
	if err != nil {
		return err
	}

	if err := s.processDailyReport(tenantID, location, date); err != nil {
		return fmt.Errorf("failed to process daily report: %w", err)
	}

	log.Printf("[DAILY] Export completed for location %s", location.LocationCode)
	return nil
}

func (s *exportService) Export30MinByLocationID(tenantID, locationID string, triggerTime time.Time) error {
	globalKey := ExportKey{
		TenantID: tenantID,
		Date:     triggerTime.Format("20060102"),
		Type:     "30min",
	}

	if _, exists := s.globalExportLock.Load(globalKey.GlobalString()); exists {
		return fmt.Errorf("tenant-wide 30min export is running, please wait")
	}

	location, err := s.getLocationByID(tenantID, locationID)
	if err != nil {
		return err
	}

	if err := s.process30MinReport(tenantID, location, triggerTime); err != nil {
		return fmt.Errorf("failed to process 30min report: %w", err)
	}

	log.Printf("[30MIN] Export completed for location %s", location.LocationCode)
	return nil
}

func (s *exportService) ExportAllReportByLocationID(tenantID, locationID string, date time.Time) error {
	globalKey := ExportKey{
		TenantID: tenantID,
		Date:     date.Format("20060102"),
		Type:     "all",
	}

	if _, exists := s.globalExportLock.Load(globalKey.GlobalString()); exists {
		return fmt.Errorf("tenant-wide complete export is running, please wait")
	}

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
		return fmt.Errorf("failed to process complete report: %w", err)
	}

	log.Printf("[COMPLETE] Export completed for location %s", location.LocationCode)
	return nil
}

func (s *exportService) getLocations(tenantID string) ([]entity.Location, error) {
	locations, err := s.peopleRepo.GetLocations(tenantID)
	if err != nil {
		return nil, fmt.Errorf("failed to get locations: %w", err)
	}
	if len(locations) == 0 {
		return nil, fmt.Errorf("no locations found for tenant %s", tenantID)
	}
	return locations, nil
}

func (s *exportService) groupReportsByLocation(allReports []entity.DailyReport) map[string][]entity.DailyReport {
	reportsByLocation := make(map[string][]entity.DailyReport)
	for _, report := range allReports {
		reportsByLocation[report.LocationID] = append(reportsByLocation[report.LocationID], report)
	}
	return reportsByLocation
}

func (s *exportService) processDailyReportWithData(tenantID string, location entity.Location, date time.Time, reports []entity.DailyReport) error {
	exportKey := ExportKey{
		TenantID:   tenantID,
		LocationID: location.ID,
		Date:       date.Format("20060102"),
		Type:       "daily",
	}

	if !s.acquireExportLock(exportKey) {
		return nil // Skip duplicate
	}
	defer s.releaseExportLock(exportKey)

	if !s.acquireFileGenerationLock(exportKey) {
		return nil // File generation in progress
	}
	defer s.releaseFileGenerationLock(exportKey)

	return s.generateAndQueueFile(tenantID, location, reports, func() (string, error) {
		return s.csvWriter.WriteDailyReport(tenantID, location.LocationCode, reports, date)
	})
}

func (s *exportService) process30MinReportWithData(tenantID string, location entity.Location, triggerTime time.Time, allReports []entity.DailyReport) error {
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
		return nil // Skip duplicate
	}
	defer s.releaseExportLock(exportKey)

	if !s.acquireFileGenerationLock(exportKey) {
		return nil // File generation in progress
	}
	defer s.releaseFileGenerationLock(exportKey)

	return s.generateAndQueueFile(tenantID, location, allReports, func() (string, error) {
		return s.csvWriter.Write30MinReport(tenantID, location.LocationCode, allReports, currentWindow)
	})
}

func (s *exportService) processCompleteReportWithData(tenantID string, location entity.Location, date time.Time, reports []entity.DailyReport) error {
	// Process daily report first
	if err := s.processDailyReportWithData(tenantID, location, date, reports); err != nil {
		return fmt.Errorf("failed to process daily report: %w", err)
	}

	// Process 30min windows
	return s.process30MinWindows(tenantID, location, reports)
}

func (s *exportService) processDailyReport(tenantID string, location entity.Location, date time.Time) error {
	exportKey := ExportKey{
		TenantID:   tenantID,
		LocationID: location.ID,
		Date:       date.Format("20060102"),
		Type:       "daily",
	}

	if !s.acquireExportLock(exportKey) {
		return nil
	}
	defer s.releaseExportLock(exportKey)

	if !s.acquireFileGenerationLock(exportKey) {
		return nil
	}
	defer s.releaseFileGenerationLock(exportKey)

	reports, err := s.peopleRepo.GetReport(tenantID, location.ID, date)
	if err != nil {
		return fmt.Errorf("failed to get reports: %w", err)
	}

	if len(reports) == 0 {
		return nil
	}

	return s.generateAndQueueFile(tenantID, location, reports, func() (string, error) {
		return s.csvWriter.WriteDailyReport(tenantID, location.LocationCode, reports, date)
	})
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
		return nil
	}
	defer s.releaseExportLock(exportKey)

	if !s.acquireFileGenerationLock(exportKey) {
		return nil
	}
	defer s.releaseFileGenerationLock(exportKey)

	// Use business hours range
	businessHoursRange := config.BusinessHours(jakartaTriggerTime)
	startTime := businessHoursRange.StartTime
	endTime := triggerTime
	if triggerTime.After(businessHoursRange.EndTime) {
		endTime = businessHoursRange.EndTime
	}

	allReports, err := s.peopleRepo.GetReportWithTimeRange(tenantID, location.ID, startTime, endTime)
	if err != nil {
		return fmt.Errorf("failed to get reports: %w", err)
	}

	if len(allReports) == 0 {
		return nil
	}

	return s.generateAndQueueFile(tenantID, location, allReports, func() (string, error) {
		return s.csvWriter.Write30MinReport(tenantID, location.LocationCode, allReports, currentWindow)
	})
}

func (s *exportService) processCompleteReport(tenantID string, location entity.Location, date time.Time) error {
	// Process daily report first
	if err := s.processDailyReport(tenantID, location, date); err != nil {
		return fmt.Errorf("failed to process daily report: %w", err)
	}

	// Get reports for 30min processing
	reports, err := s.peopleRepo.GetReport(tenantID, location.ID, date)
	if err != nil {
		return fmt.Errorf("failed to get reports: %w", err)
	}

	if len(reports) == 0 {
		return nil
	}

	return s.process30MinWindows(tenantID, location, reports)
}

func (s *exportService) process30MinWindows(tenantID string, location entity.Location, reports []entity.DailyReport) error {
	windowsMap := s.groupReportsByWindows(reports)
	sortedWindows := s.sortWindows(windowsMap)

	if len(sortedWindows) == 0 {
		return nil
	}

	successCount := 0
	for _, windowTime := range sortedWindows {
		exportKey := ExportKey{
			TenantID:   tenantID,
			LocationID: location.ID,
			Date:       windowTime.Format("20060102"),
			Type:       "30min",
			Window:     windowTime.Format("1504"),
		}

		if !s.acquireFileGenerationLock(exportKey) {
			continue
		}

		cumulativeReports := s.filterCumulativeDataUpToWindow(reports, windowTime)
		if len(cumulativeReports) == 0 {
			s.releaseFileGenerationLock(exportKey)
			continue
		}

		err := s.generateAndQueueFile(tenantID, location, cumulativeReports, func() (string, error) {
			return s.csvWriter.Write30MinReport(tenantID, location.LocationCode, cumulativeReports, windowTime)
		})

		s.releaseFileGenerationLock(exportKey)

		if err == nil {
			successCount++
		}
	}

	if len(sortedWindows) > 5 {
		log.Printf("[30MIN] Processed %d/%d windows for location %s", successCount, len(sortedWindows), location.LocationCode)
	}
	return nil
}

// Consolidated file generation and queue logic
func (s *exportService) generateAndQueueFile(tenantID string, location entity.Location, reports []entity.DailyReport, fileGenerator func() (string, error)) error {
	filePath, err := fileGenerator()
	if err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	if !s.validateGeneratedFile(filePath) {
		return fmt.Errorf("generated file is invalid: %s", filePath)
	}

	return s.createLogAndQueueUpload(tenantID, location.ID, filePath)
}

func (s *exportService) validateGeneratedFile(filePath string) bool {
	fileInfo, err := os.Stat(filePath)
	if err != nil || fileInfo.Size() == 0 {
		return false
	}

	file, err := os.Open(filePath)
	if err != nil {
		return false
	}
	defer file.Close()

	buffer := make([]byte, 100)
	_, err = file.Read(buffer)
	return err == nil
}

func (s *exportService) getLocationByID(tenantID, locationID string) (entity.Location, error) {
	return s.peopleRepo.GetLocationByID(tenantID, locationID)
}

func (s *exportService) createLogAndQueueUpload(tenantID, locationID, filePath string) error {
	fileName := filepath.Base(filePath)
	fileType := utils.DetermineFileType(fileName)

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

	// Check for duplicates
	if s.isDuplicateUploadWithContext(tenantID, locationID, fileName, fileInfo.Size(), recordCount, "export") {
		log.Printf("[EXPORT] Skipping duplicate: %s", fileName)
		return nil
	}

	if err := s.markPreviousFileAsReplaced(fileName, tenantID, locationID); err != nil {
		log.Printf("[EXPORT] Warning: failed to mark previous file as replaced: %v", err)
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
		return fmt.Errorf("failed to create transfer log: %w", err)
	}

	// Create upload job
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
		return fmt.Errorf("failed to publish upload job: %w", err)
	}

	return nil
}

func (s *exportService) markPreviousFileAsReplaced(fileName, tenantID, locationID string) error {
	recentLogs, err := s.sftpLogRepo.GetRecentByFileName(fileName, 24*time.Hour)
	if err != nil {
		return err
	}

	for _, logEntry := range recentLogs {
		if logEntry.TenantID == tenantID &&
			logEntry.LocationID == locationID &&
			logEntry.Status == "SUCCESS" {
			replacedMsg := fmt.Sprintf("File replaced with updated data at %s", time.Now().Format("2006-01-02 15:04:05"))
			if err := s.sftpLogRepo.UpdateStatus(logEntry.ID, "REPLACED", &replacedMsg); err != nil {
				log.Printf("[EXPORT] Failed to update replaced status for %s: %v", logEntry.FileName, err)
			} else {
				log.Printf("[EXPORT] Marked previous file as REPLACED: %s", logEntry.FileName)
			}
			break
		}
	}

	return nil
}

func (s *exportService) isDuplicateUploadWithContext(tenantID, locationID, fileName string, fileSize int64, recordCount int, context string) bool {
	recentLogs, err := s.sftpLogRepo.GetRecentByFileName(fileName, 10*time.Minute)
	if err != nil {
		return false
	}

	for _, recentLog := range recentLogs {
		if recentLog.TenantID == tenantID && recentLog.LocationID == locationID {
			timeDiff := time.Since(recentLog.CreatedAt)

			if recentLog.Status == "REPLACED" {
				continue
			}

			if recentLog.RecordCount != nil {
				oldRecordCount := *recentLog.RecordCount

				if context == "late_data" {
					if timeDiff < 1*time.Minute &&
						recordCount == oldRecordCount &&
						recentLog.FileSize == fileSize &&
						(recentLog.Status == "SUCCESS" || recentLog.Status == "PENDING") {
						log.Printf("[EXPORT] Blocking recent exact duplicate in late_data context: %s", fileName)
						return true
					}

					if recordCount < oldRecordCount {
						log.Printf("[EXPORT] 🚨 Blocking impossible decrease in late_data: %s (%d → %d)", fileName, oldRecordCount, recordCount)
						return true
					}

					return false
				}

				if recordCount == oldRecordCount &&
					recentLog.FileSize == fileSize &&
					timeDiff < 5*time.Minute &&
					(recentLog.Status == "SUCCESS" || recentLog.Status == "PENDING") {
					log.Printf("[EXPORT] Blocking exact duplicate: %s (same count: %d)", fileName, recordCount)
					return true
				}

				if recordCount < oldRecordCount {
					log.Printf("[EXPORT] 🚨 CRITICAL: Record count decreased from %d to %d for %s - possible data corruption!", oldRecordCount, recordCount, fileName)
					return true
				}

				if recordCount > oldRecordCount && recordCount <= oldRecordCount+3 {
					if timeDiff < 3*time.Minute {
						log.Printf("[EXPORT] Blocking minor increase within time threshold: %s (%d → %d, %v ago)", fileName, oldRecordCount, recordCount, timeDiff)
						return true
					}

					log.Printf("[EXPORT] Allowing minor late data increase: %s (%d → %d records)",
						fileName, oldRecordCount, recordCount)
					return false
				}

				if recordCount > oldRecordCount+3 {
					log.Printf("[EXPORT] Allowing significant data increase: %s (%d → %d records)",
						fileName, oldRecordCount, recordCount)
					return false
				}
			}

			if timeDiff < 30*time.Second &&
				(recentLog.Status == "SUCCESS" || recentLog.Status == "PENDING") {
				log.Printf("[EXPORT] Blocking very recent upload: %s (%v ago)", fileName, timeDiff)
				return true
			}

			if recentLog.Status == "SUCCESS" &&
				recentLog.FileSize > 0 &&
				timeDiff < 2*time.Minute {
				log.Printf("[EXPORT] Blocking recent successful upload: %s (%v ago)", fileName, timeDiff)
				return true
			}
		}
	}
	return false
}

func (s *exportService) groupReportsByWindows(reports []entity.DailyReport) map[time.Time][]entity.DailyReport {
	windows := make(map[time.Time][]entity.DailyReport)
	for _, report := range reports {
		if reportTime, err := time.Parse(time.RFC3339, report.Date); err == nil {
			windowStart := reportTime.Truncate(30 * time.Minute).Add(30 * time.Minute)
			windows[windowStart] = append(windows[windowStart], report)
		}
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
		if reportTime, err := time.Parse(time.RFC3339, report.Date); err == nil {
			jakartaReportTime := reportTime.In(config.GetJakartaTimezone())
			if jakartaReportTime.Before(jakartaWindowTime) || jakartaReportTime.Equal(jakartaWindowTime) {
				cumulativeReports = append(cumulativeReports, report)
			}
		}
	}
	return cumulativeReports
}
