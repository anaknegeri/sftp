package service

import (
	"fmt"
	"jarvist/sftp/internal/config"
	"jarvist/sftp/internal/domain/entity"
	"jarvist/sftp/internal/file"
	"jarvist/sftp/internal/repository"
	"jarvist/sftp/internal/types"
	"jarvist/sftp/pkg/utils"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

type LateDataService interface {
	CheckForLateData(tenantID string, checkDate time.Time, checkType string) error
	SetSFTPService(sftpService SFTPService)
}

type lateDataService struct {
	peopleRepo  repository.PeopleCountRepository
	sftpLogRepo repository.SFTPLogRepository
	csvWriter   file.CSVWriter
	sftpService SFTPService
	localPath   string
}

func NewLateDataService(
	peopleRepo repository.PeopleCountRepository,
	sftpLogRepo repository.SFTPLogRepository,
	csvWriter file.CSVWriter,
	localPath string,
) LateDataService {
	return &lateDataService{
		peopleRepo:  peopleRepo,
		sftpLogRepo: sftpLogRepo,
		csvWriter:   csvWriter,
		localPath:   localPath,
	}
}

func (s *lateDataService) SetSFTPService(sftpService SFTPService) {
	s.sftpService = sftpService
}

// Main method with different check types
func (s *lateDataService) CheckForLateData(tenantID string, checkDate time.Time, checkType string) error {
	log.Printf("[LATE_DATA] Starting %s late data check for tenant %s, date %s",
		checkType, tenantID, checkDate.Format("2006-01-02"))

	locations, err := s.peopleRepo.GetLocations(tenantID)
	if err != nil {
		return fmt.Errorf("failed to get locations: %w", err)
	}

	if len(locations) == 0 {
		log.Printf("[LATE_DATA] No locations found for tenant %s", tenantID)
		return nil
	}

	processedCount := 0
	for _, location := range locations {
		switch checkType {
		case "realtime":
			// For realtime checks - check for very recent late data
			if s.hasRecentLateData(tenantID, location, checkDate) {
				if err := s.regenerateRecentLateData(tenantID, location, checkDate); err != nil {
					log.Printf("[LATE_DATA] Failed to regenerate recent late data for location %s: %v", location.LocationCode, err)
					continue
				}
				processedCount++
			}
		case "historical":
			// For historical checks - check for historical late data
			if s.hasHistoricalLateData(tenantID, location, checkDate) {
				if err := s.regenerateHistoricalLateData(tenantID, location, checkDate); err != nil {
					log.Printf("[LATE_DATA] Failed to regenerate historical late data for location %s: %v", location.LocationCode, err)
					continue
				}
				processedCount++
			}
		default:
			log.Printf("[LATE_DATA] Unknown check type: %s", checkType)
			continue
		}
	}

	if processedCount > 0 {
		log.Printf("[LATE_DATA] %s check completed: %d locations had late data", checkType, processedCount)
	} else {
		log.Printf("[LATE_DATA] No late data found for tenant %s on %s (%s check)",
			tenantID, checkDate.Format("2006-01-02"), checkType)
	}

	return nil
}

// Check for recent late data (within last hour)
func (s *lateDataService) hasRecentLateData(tenantID string, location entity.Location, checkDate time.Time) bool {
	jakartaTime := checkDate.In(config.GetJakartaTimezone())

	// Check if we have new data in the last 1 hour
	oneHourAgo := jakartaTime.Add(-1 * time.Hour)

	reports, err := s.peopleRepo.GetReportWithTimeRange(tenantID, location.ID, oneHourAgo, jakartaTime)
	if err != nil || len(reports) == 0 {
		return false
	}

	// Check if this data was already exported
	lastExportTime := s.getLastExportTimeForWindow(location, jakartaTime)

	// If no export found, or export was before the new data, we have late data
	if lastExportTime.IsZero() {
		log.Printf("[LATE_DATA] No recent export found for location %s, but new data exists", location.LocationCode)
		return true
	}

	// Check if new data arrived after last export
	for _, report := range reports {
		reportTime, err := time.Parse(time.RFC3339, report.Date)
		if err != nil {
			continue
		}
		if reportTime.After(lastExportTime) {
			log.Printf("[LATE_DATA] New data found after last export for location %s", location.LocationCode)
			return true
		}
	}

	return false
}

// Check for historical late data (older data that was never exported)
func (s *lateDataService) hasHistoricalLateData(tenantID string, location entity.Location, checkDate time.Time) bool {
	// Check if we have any data for this date that was never exported
	reports, err := s.peopleRepo.GetReport(tenantID, location.ID, checkDate)
	if err != nil || len(reports) == 0 {
		return false
	}

	// Check if daily report was ever exported for this date
	dailyExportTime := s.getLastDailyExportTime(location, checkDate)

	if dailyExportTime.IsZero() {
		log.Printf("[LATE_DATA] No daily export found for location %s on %s, but data exists",
			location.LocationCode, checkDate.Format("2006-01-02"))
		return true
	}

	// Check if it's been too long since last export (more than 6 hours)
	if time.Since(dailyExportTime) > 6*time.Hour {
		log.Printf("[LATE_DATA] Last export for location %s was %v ago, checking for new data",
			location.LocationCode, time.Since(dailyExportTime))
		return true
	}

	return false
}

// Regenerate only recent late data (smart regeneration)
func (s *lateDataService) regenerateRecentLateData(tenantID string, location entity.Location, checkDate time.Time) error {
	log.Printf("[LATE_DATA] Regenerating recent late data for location %s", location.LocationCode)

	jakartaTime := checkDate.In(config.GetJakartaTimezone())

	// Get all data for today (we need full context for cumulative reports)
	today := time.Date(jakartaTime.Year(), jakartaTime.Month(), jakartaTime.Day(), 0, 0, 0, 0, config.GetJakartaTimezone())
	allReports, err := s.peopleRepo.GetReportWithTimeRange(tenantID, location.ID, today, jakartaTime)
	if err != nil {
		return fmt.Errorf("failed to get all reports: %w", err)
	}

	if len(allReports) == 0 {
		return nil
	}

	// Always regenerate daily report (because it's cumulative for the whole day)
	if err := s.regenerateDailyReport(tenantID, location, today, allReports); err != nil {
		log.Printf("[LATE_DATA] Failed to regenerate daily report: %v", err)
	}

	// Only regenerate 30-minute windows that have new data
	affectedWindows := s.getAffectedWindows(allReports, jakartaTime.Add(-1*time.Hour), jakartaTime)

	for _, windowTime := range affectedWindows {
		if err := s.regenerate30MinWindow(tenantID, location, windowTime, allReports); err != nil {
			log.Printf("[LATE_DATA] Failed to regenerate 30min window %s: %v", windowTime.Format("15:04"), err)
		}
	}

	log.Printf("[LATE_DATA] Recent late data regeneration completed for %s", location.LocationCode)
	return nil
}

// Regenerate historical data (full day regeneration)
func (s *lateDataService) regenerateHistoricalLateData(tenantID string, location entity.Location, checkDate time.Time) error {
	log.Printf("[LATE_DATA] Regenerating historical late data for location %s on %s",
		location.LocationCode, checkDate.Format("2006-01-02"))

	// Get all data for the specific date
	allReports, err := s.peopleRepo.GetReport(tenantID, location.ID, checkDate)
	if err != nil {
		return fmt.Errorf("failed to get all reports: %w", err)
	}

	if len(allReports) == 0 {
		return nil
	}

	// Regenerate daily report
	if err := s.regenerateDailyReport(tenantID, location, checkDate, allReports); err != nil {
		log.Printf("[LATE_DATA] Failed to regenerate daily report: %v", err)
	}

	// For historical data, regenerate ALL 30-minute windows (because we might have missed some)
	allWindows := s.identifyExistingWindows(allReports)

	for _, windowTime := range allWindows {
		if err := s.regenerate30MinWindow(tenantID, location, windowTime, allReports); err != nil {
			log.Printf("[LATE_DATA] Failed to regenerate 30min window %s: %v", windowTime.Format("15:04"), err)
		}
	}

	log.Printf("[LATE_DATA] Historical late data regeneration completed for %s", location.LocationCode)
	return nil
}

// Get affected windows within time range
func (s *lateDataService) getAffectedWindows(allReports []entity.DailyReport, startTime, endTime time.Time) []time.Time {
	windowSet := make(map[time.Time]bool)

	for _, report := range allReports {
		reportTime, err := time.Parse(time.RFC3339, report.Date)
		if err != nil {
			continue
		}

		jakartaReportTime := reportTime.In(config.GetJakartaTimezone())

		// Only include reports within the specified time range
		if jakartaReportTime.After(startTime) && jakartaReportTime.Before(endTime.Add(1*time.Second)) {
			windowStart := jakartaReportTime.Truncate(30 * time.Minute).Add(30 * time.Minute)
			windowSet[windowStart] = true
		}
	}

	var windows []time.Time
	for window := range windowSet {
		windows = append(windows, window)
	}

	sort.Slice(windows, func(i, j int) bool {
		return windows[i].Before(windows[j])
	})

	return windows
}

// Get last export time for specific window
func (s *lateDataService) getLastExportTimeForWindow(location entity.Location, windowTime time.Time) time.Time {
	jakartaTime := windowTime.In(config.GetJakartaTimezone())
	timeStr := jakartaTime.Format("1504")
	dateStr := jakartaTime.Format("20060102")

	fileName := fmt.Sprintf("%s_%s_%s.csv", location.LocationCode, dateStr, timeStr)

	if transferLog, err := s.sftpLogRepo.GetByFileName(fileName); err == nil && transferLog != nil {
		if transferLog.TransferEndTime != nil && transferLog.Status == "SUCCESS" {
			return *transferLog.TransferEndTime
		}
	}

	return time.Time{}
}

// Get last daily export time
func (s *lateDataService) getLastDailyExportTime(location entity.Location, date time.Time) time.Time {
	dateStr := date.Format("20060102")
	fileName := fmt.Sprintf("%s_%s.csv", location.LocationCode, dateStr)

	if transferLog, err := s.sftpLogRepo.GetByFileName(fileName); err == nil && transferLog != nil {
		if transferLog.TransferEndTime != nil && transferLog.Status == "SUCCESS" {
			return *transferLog.TransferEndTime
		}
	}

	return time.Time{}
}

// Single method for daily report regeneration
func (s *lateDataService) regenerateDailyReport(tenantID string, location entity.Location, date time.Time, reports []entity.DailyReport) error {
	filePath, err := s.csvWriter.WriteDailyReport(tenantID, location.LocationCode, reports, date)
	if err != nil {
		return fmt.Errorf("failed to write daily report: %w", err)
	}

	if err := s.uploadFileWithLog(tenantID, location.ID, filePath, entity.FileTypeDaily); err != nil {
		return fmt.Errorf("failed to upload daily report: %w", err)
	}

	log.Printf("[LATE_DATA] Successfully regenerated daily report for %s (%d records)",
		location.LocationCode, len(reports))
	return nil
}

// Single method for 30-min window regeneration
func (s *lateDataService) regenerate30MinWindow(tenantID string, location entity.Location, windowTime time.Time, allReports []entity.DailyReport) error {
	cumulativeReports := s.filterCumulativeDataUpToWindow(allReports, windowTime)

	if len(cumulativeReports) == 0 {
		return nil
	}

	filePath, err := s.csvWriter.Write30MinReport(tenantID, location.LocationCode, cumulativeReports, windowTime)
	if err != nil {
		return fmt.Errorf("failed to write 30min report: %w", err)
	}

	if err := s.uploadFileWithLog(tenantID, location.ID, filePath, entity.FileType30Min); err != nil {
		return fmt.Errorf("failed to upload 30min report: %w", err)
	}

	log.Printf("[LATE_DATA] Successfully regenerated 30min window %s for %s (%d records)",
		windowTime.Format("15:04"), location.LocationCode, len(cumulativeReports))
	return nil
}

func (s *lateDataService) filterCumulativeDataUpToWindow(allReports []entity.DailyReport, windowTime time.Time) []entity.DailyReport {
	var cumulativeReports []entity.DailyReport
	jakartaWindowTime := windowTime.In(config.GetJakartaTimezone())

	for _, report := range allReports {
		reportTime, err := time.Parse(time.RFC3339, report.Date)
		if err != nil {
			continue
		}

		jakartaReportTime := reportTime.In(config.GetJakartaTimezone())
		if jakartaReportTime.Before(jakartaWindowTime) || jakartaReportTime.Equal(jakartaWindowTime) {
			cumulativeReports = append(cumulativeReports, report)
		}
	}

	return cumulativeReports
}

func (s *lateDataService) identifyExistingWindows(allReports []entity.DailyReport) []time.Time {
	windowSet := make(map[time.Time]bool)

	for _, report := range allReports {
		reportTime, err := time.Parse(time.RFC3339, report.Date)
		if err != nil {
			continue
		}

		windowStart := reportTime.Truncate(30 * time.Minute).Add(30 * time.Minute)
		windowSet[windowStart] = true
	}

	var windows []time.Time
	for window := range windowSet {
		windows = append(windows, window)
	}

	sort.Slice(windows, func(i, j int) bool {
		return windows[i].Before(windows[j])
	})

	return windows
}

func (s *lateDataService) uploadFileWithLog(tenantID, locationID, filePath, fileType string) error {
	fileName := filepath.Base(filePath)

	tenantConfig, exists := config.GetTenantByID(tenantID)
	if !exists {
		return fmt.Errorf("tenant %s not found or disabled", tenantID)
	}

	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return fmt.Errorf("failed to get file info: %w", err)
	}

	recordCount := utils.CountFileRecords(filePath)
	remotePath := strings.ReplaceAll(filepath.Join(tenantConfig.SFTP.BasePath, fileName), "\\", "/")

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

	log.Printf("[LATE_DATA] File log created: %s (PENDING) [LATE DATA REGENERATION]", fileName)

	uploadJob := types.UploadSFTPJob{
		TenantID:   tenantID,
		FilePath:   filePath,
		FileName:   fileName,
		RemotePath: remotePath,
		FileType:   fileType,
		LocationID: locationID,
		CreatedAt:  time.Now(),
	}

	// Use SFTP service directly for immediate upload (bypassing queue for late data priority)
	if err := s.sftpService.UploadFile(uploadJob); err != nil {
		now := time.Now()
		transferLog.TransferEndTime = &now
		transferLog.Status = "FAILED"
		errorMsg := err.Error()
		transferLog.ErrorMessage = &errorMsg
		s.sftpLogRepo.Update(transferLog)
		return fmt.Errorf("upload failed: %w", err)
	}

	log.Printf("[LATE_DATA] Successfully uploaded %s (%d records) [LATE DATA REGENERATION]", fileName, recordCount)
	return nil
}
