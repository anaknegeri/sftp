package service

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"jarvist/sftp-service/internal/config"
	"jarvist/sftp-service/internal/domain/entity"
	"jarvist/sftp-service/internal/file"
	"jarvist/sftp-service/internal/repository"
	"jarvist/sftp-service/internal/types"
	"jarvist/sftp-service/pkg/utils"

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

func (s *lateDataService) CheckForLateData(tenantID string, checkDate time.Time, checkType string) error {
	log.Printf("[LATE_DATA] Starting %s check for tenant %s, date %s", checkType, tenantID, checkDate.Format("2006-01-02"))

	if err := s.validateCheckType(checkType); err != nil {
		return err
	}

	locations, err := s.peopleRepo.GetLocations(tenantID)
	if err != nil {
		return fmt.Errorf("failed to get locations: %w", err)
	}

	if len(locations) == 0 {
		log.Printf("[LATE_DATA] No locations found for tenant %s", tenantID)
		return nil
	}

	allReports, err := s.fetchReportsForCheckType(tenantID, checkDate, checkType)
	if err != nil {
		return err
	}

	if len(allReports) == 0 {
		log.Printf("[LATE_DATA] No data found for tenant %s (%s check)", tenantID, checkType)
		return nil
	}

	processedCount := s.processLocationsForLateData(tenantID, checkDate, checkType, locations, allReports)

	if processedCount > 0 {
		log.Printf("[LATE_DATA] %s check completed: %d locations processed", checkType, processedCount)
	} else {
		log.Printf("[LATE_DATA] No late data found for tenant %s", tenantID)
	}

	return nil
}

func (s *lateDataService) fetchReportsForCheckType(tenantID string, checkDate time.Time, checkType string) ([]entity.DailyReport, error) {
	switch strings.TrimSpace(checkType) {
	case "realtime":
		// Use business hours for realtime check
		jakartaTime := checkDate.In(config.GetJakartaTimezone())
		businessHoursRange := config.BusinessHours(jakartaTime)

		// For realtime, check last hour within business hours
		oneHourAgo := jakartaTime.Add(-1 * time.Hour)
		startTime := oneHourAgo
		if oneHourAgo.Before(businessHoursRange.StartTime) {
			startTime = businessHoursRange.StartTime
		}

		endTime := jakartaTime
		if jakartaTime.After(businessHoursRange.EndTime) {
			endTime = businessHoursRange.EndTime
		}

		return s.peopleRepo.GetAllReportsForTenantWithTimeRange(tenantID, startTime, endTime)
	case "historical":
		// Use business hours for historical check
		return s.peopleRepo.GetAllReportsForTenant(tenantID, checkDate)
	default:
		return nil, fmt.Errorf("unknown check type: %s", checkType)
	}
}

func (s *lateDataService) processLocationsForLateData(tenantID string, checkDate time.Time, checkType string, locations []entity.Location, allReports []entity.DailyReport) int {
	reportsByLocation := s.groupReportsByLocation(allReports)
	processedCount := 0

	for _, location := range locations {
		locationReports, exists := reportsByLocation[location.ID]
		if !exists || len(locationReports) == 0 {
			continue
		}

		hasLateData := s.checkHasLateData(location, checkDate, checkType, locationReports)
		if !hasLateData {
			continue
		}

		var err error
		switch strings.TrimSpace(checkType) {
		case "realtime":
			err = s.regenerateRecentLateData(tenantID, location, checkDate, locationReports)
		case "historical":
			err = s.regenerateHistoricalLateData(tenantID, location, checkDate, locationReports)
		}

		if err != nil {
			log.Printf("[LATE_DATA] Failed to regenerate data for location %s: %v", location.LocationCode, err)
			continue
		}
		processedCount++
	}

	return processedCount
}

func (s *lateDataService) checkHasLateData(location entity.Location, checkDate time.Time, checkType string, reports []entity.DailyReport) bool {
	switch strings.TrimSpace(checkType) {
	case "realtime":
		return s.hasRecentLateDataWithReports(location, checkDate, reports)
	case "historical":
		return s.hasHistoricalLateDataWithReports(location, checkDate, reports)
	default:
		return false
	}
}

func (s *lateDataService) regenerateRecentLateData(tenantID string, location entity.Location, checkDate time.Time, locationReports []entity.DailyReport) error {
	if len(locationReports) == 0 || tenantID == "" || location.ID == "" {
		return nil
	}

	log.Printf("[LATE_DATA] Regenerating recent data for location %s (%d records)", location.LocationCode, len(locationReports))

	// Use business hours range for time boundaries
	jakartaTime := checkDate.In(config.GetJakartaTimezone())
	businessHoursRange := config.BusinessHours(jakartaTime)

	// For realtime, get affected windows within business hours
	oneHourAgo := jakartaTime.Add(-1 * time.Hour)
	startTime := oneHourAgo
	if oneHourAgo.Before(businessHoursRange.StartTime) {
		startTime = businessHoursRange.StartTime
	}

	endTime := jakartaTime
	if jakartaTime.After(businessHoursRange.EndTime) {
		endTime = businessHoursRange.EndTime
	}

	// Regenerate daily report (still use business hours date)
	dailySuccess := false
	if err := s.regenerateDailyReport(tenantID, location, businessHoursRange.StartTime, locationReports); err != nil {
		log.Printf("[LATE_DATA] Daily report failed for %s: %v", location.LocationCode, err)
	} else {
		dailySuccess = true
	}

	// Regenerate affected 30-minute windows within business hours
	affectedWindows := s.getAffectedWindows(locationReports, startTime, endTime)
	windowsSuccess := 0

	for _, windowTime := range affectedWindows {
		if err := s.regenerate30MinWindow(tenantID, location, windowTime, locationReports); err != nil {
			log.Printf("[LATE_DATA] Window %s failed for %s: %v", windowTime.Format("15:04"), location.LocationCode, err)
		} else {
			windowsSuccess++
		}
	}

	// Summary
	totalOperations := 1 + len(affectedWindows)
	totalSuccess := 0
	if dailySuccess {
		totalSuccess++
	}
	totalSuccess += windowsSuccess

	log.Printf("[LATE_DATA] Recent regeneration for %s: %d/%d operations successful",
		location.LocationCode, totalSuccess, totalOperations)

	return nil
}

func (s *lateDataService) regenerateHistoricalLateData(tenantID string, location entity.Location, checkDate time.Time, locationReports []entity.DailyReport) error {
	if len(locationReports) == 0 || tenantID == "" || location.ID == "" {
		return nil
	}

	log.Printf("[LATE_DATA] Regenerating historical data for location %s (%d records)", location.LocationCode, len(locationReports))

	// Regenerate daily report
	dailySuccess := false
	if err := s.regenerateDailyReport(tenantID, location, checkDate, locationReports); err != nil {
		log.Printf("[LATE_DATA] Daily report failed for %s: %v", location.LocationCode, err)
	} else {
		dailySuccess = true
	}

	// Regenerate all 30-minute windows
	allWindows := s.identifyExistingWindows(locationReports)
	windowsSuccess := 0

	for i, windowTime := range allWindows {
		if err := s.regenerate30MinWindow(tenantID, location, windowTime, locationReports); err != nil {
			log.Printf("[LATE_DATA] Window %s failed for %s: %v", windowTime.Format("15:04"), location.LocationCode, err)
		} else {
			windowsSuccess++
		}

		// Progress logging for large batches
		if len(allWindows) > 20 && (i+1)%10 == 0 {
			log.Printf("[LATE_DATA] Progress for %s: %d/%d windows completed",
				location.LocationCode, i+1, len(allWindows))
		}
	}

	// Summary
	totalOperations := 1 + len(allWindows)
	totalSuccess := 0
	if dailySuccess {
		totalSuccess++
	}
	totalSuccess += windowsSuccess

	log.Printf("[LATE_DATA] Historical regeneration for %s: %d/%d operations successful",
		location.LocationCode, totalSuccess, totalOperations)

	return nil
}

func (s *lateDataService) hasRecentLateDataWithReports(location entity.Location, checkDate time.Time, reports []entity.DailyReport) bool {
	if len(reports) == 0 {
		return false
	}

	jakartaTime := checkDate.In(config.GetJakartaTimezone())
	lastExportTime := s.getLastExportTimeForWindow(location, jakartaTime)

	if lastExportTime.IsZero() {
		return true
	}

	for _, report := range reports {
		if reportTime, err := time.Parse(time.RFC3339, report.Date); err == nil {
			if reportTime.After(lastExportTime) {
				return true
			}
		}
	}
	return false
}

func (s *lateDataService) hasHistoricalLateDataWithReports(location entity.Location, checkDate time.Time, reports []entity.DailyReport) bool {
	if len(reports) == 0 {
		return false
	}

	dailyExportTime := s.getLastDailyExportTime(location, checkDate)
	if dailyExportTime.IsZero() {
		return true
	}

	return time.Since(dailyExportTime) > 6*time.Hour
}

func (s *lateDataService) regenerateDailyReport(tenantID string, location entity.Location, date time.Time, reports []entity.DailyReport) error {
	filePath, err := s.csvWriter.WriteDailyReport(tenantID, location.LocationCode, reports, date)
	if err != nil {
		return fmt.Errorf("failed to write daily report: %w", err)
	}

	return s.createLogAndUpload(tenantID, location.ID, filePath, entity.FileTypeDaily)
}

func (s *lateDataService) regenerate30MinWindow(tenantID string, location entity.Location, windowTime time.Time, allReports []entity.DailyReport) error {
	cumulativeReports := s.filterCumulativeDataUpToWindow(allReports, windowTime)
	if len(cumulativeReports) == 0 {
		return nil
	}

	filePath, err := s.csvWriter.Write30MinReport(tenantID, location.LocationCode, cumulativeReports, windowTime)
	if err != nil {
		return fmt.Errorf("failed to write 30min report: %w", err)
	}

	return s.createLogAndUpload(tenantID, location.ID, filePath, entity.FileType30Min)
}

func (s *lateDataService) createLogAndUpload(tenantID, locationID, filePath, fileType string) error {
	fileName := filepath.Base(filePath)

	tenantConfig, exists := config.GetTenantByID(tenantID)
	if !exists {
		return fmt.Errorf("tenant %s not found", tenantID)
	}

	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return fmt.Errorf("failed to get file info: %w", err)
	}

	recordCount := utils.CountFileRecords(filePath)
	remotePath := strings.ReplaceAll(filepath.Join(tenantConfig.SFTP.BasePath, fileName), "\\", "/")

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

	// Direct upload for late data (priority)
	if err := s.sftpService.UploadFile(uploadJob); err != nil {
		errorMsg := err.Error()
		s.sftpLogRepo.UpdateStatus(transferLog.ID, "FAILED", &errorMsg)
		return fmt.Errorf("upload failed: %w", err)
	}

	return nil
}

func (s *lateDataService) validateCheckType(checkType string) error {
	validTypes := []string{"realtime", "historical"}
	trimmedType := strings.TrimSpace(checkType)

	for _, valid := range validTypes {
		if trimmedType == valid {
			return nil
		}
	}

	return fmt.Errorf("invalid check type '%s', must be one of: %v", checkType, validTypes)
}

func (s *lateDataService) getLastExportTimeForWindow(location entity.Location, windowTime time.Time) time.Time {
	jakartaTime := windowTime.In(config.GetJakartaTimezone())
	fileName := fmt.Sprintf("%s_%s_%s.csv",
		location.LocationCode,
		jakartaTime.Format("20060102"),
		jakartaTime.Format("1504"))

	if transferLog, err := s.sftpLogRepo.GetByFileName(fileName); err == nil && transferLog != nil {
		if transferLog.TransferEndTime != nil && transferLog.Status == "SUCCESS" {
			return *transferLog.TransferEndTime
		}
	}
	return time.Time{}
}

func (s *lateDataService) getLastDailyExportTime(location entity.Location, date time.Time) time.Time {
	fileName := fmt.Sprintf("%s_%s.csv", location.LocationCode, date.Format("20060102"))

	if transferLog, err := s.sftpLogRepo.GetByFileName(fileName); err == nil && transferLog != nil {
		if transferLog.TransferEndTime != nil && transferLog.Status == "SUCCESS" {
			return *transferLog.TransferEndTime
		}
	}
	return time.Time{}
}

func (s *lateDataService) getAffectedWindows(allReports []entity.DailyReport, startTime, endTime time.Time) []time.Time {
	windowSet := make(map[time.Time]bool)

	for _, report := range allReports {
		if reportTime, err := time.Parse(time.RFC3339, report.Date); err == nil {
			jakartaReportTime := reportTime.In(config.GetJakartaTimezone())
			if jakartaReportTime.After(startTime) && jakartaReportTime.Before(endTime.Add(time.Second)) {
				windowStart := jakartaReportTime.Truncate(30 * time.Minute).Add(30 * time.Minute)
				windowSet[windowStart] = true
			}
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

func (s *lateDataService) identifyExistingWindows(allReports []entity.DailyReport) []time.Time {
	windowSet := make(map[time.Time]bool)

	for _, report := range allReports {
		if reportTime, err := time.Parse(time.RFC3339, report.Date); err == nil {
			windowStart := reportTime.Truncate(30 * time.Minute).Add(30 * time.Minute)
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

func (s *lateDataService) filterCumulativeDataUpToWindow(allReports []entity.DailyReport, windowTime time.Time) []entity.DailyReport {
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

func (s *lateDataService) groupReportsByLocation(allReports []entity.DailyReport) map[string][]entity.DailyReport {
	reportsByLocation := make(map[string][]entity.DailyReport)
	for _, report := range allReports {
		reportsByLocation[report.LocationID] = append(reportsByLocation[report.LocationID], report)
	}
	return reportsByLocation
}
