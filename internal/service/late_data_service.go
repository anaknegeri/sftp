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
	sftpService SFTPService,
	localPath string,
) LateDataService {
	return &lateDataService{
		peopleRepo:  peopleRepo,
		sftpLogRepo: sftpLogRepo,
		csvWriter:   csvWriter,
		sftpService: sftpService,
		localPath:   localPath,
	}
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

	log.Printf("[LATE_DATA] %s check completed: %d/%d locations processed", checkType, processedCount, len(locations))
	return nil
}

func (s *lateDataService) fetchReportsForCheckType(tenantID string, checkDate time.Time, checkType string) ([]entity.DailyReport, error) {
	switch strings.TrimSpace(checkType) {
	case "realtime":
		jakartaTime := checkDate.In(config.GetJakartaTimezone())
		businessHoursRange := config.BusinessHours(jakartaTime)

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

	jakartaTime := checkDate.In(config.GetJakartaTimezone())
	isDifferentDay := s.isLateDataForDifferentDay(locationReports, jakartaTime)

	if isDifferentDay {
		log.Printf("[LATE_DATA] Cross-day late data for %s - regenerating daily reports only", location.LocationCode)
		return s.regenerateCrossDayLateData(tenantID, location, locationReports)
	}

	log.Printf("[LATE_DATA] Same-day late data for %s - regenerating daily + windows", location.LocationCode)

	businessHoursRange := config.BusinessHours(jakartaTime)
	allDayReports, err := s.peopleRepo.GetReport(tenantID, location.ID, checkDate)
	if err != nil {
		return fmt.Errorf("failed to get all day reports: %w", err)
	}

	if len(allDayReports) == 0 {
		return nil
	}

	existingWindowsMap, err := s.getExistingWindowsBatch(location, checkDate)
	if err != nil {
		log.Printf("[LATE_DATA] Warning: failed to get existing windows: %v", err)
		existingWindowsMap = make(map[time.Time]*entity.SFTPTransferLog)
	}

	dailySuccess := s.regenerateDailyReportSafe(tenantID, location, businessHoursRange.StartTime, allDayReports)

	allDataWindows := s.identifyExistingWindows(allDayReports)
	var existingWindows []time.Time
	for windowTime := range existingWindowsMap {
		existingWindows = append(existingWindows, windowTime)
	}

	allWindowsToRegenerate := s.mergeAndSortWindows(existingWindows, allDataWindows)

	windowsSuccess := s.regenerateWindowsBatch(tenantID, location, allWindowsToRegenerate, allDayReports, existingWindowsMap)

	totalSuccess := 0
	if dailySuccess {
		totalSuccess++
	}
	totalSuccess += windowsSuccess

	log.Printf("[LATE_DATA] Regeneration complete for %s: %d/%d operations successful",
		location.LocationCode, totalSuccess, 1+len(allWindowsToRegenerate))

	return nil
}

func (s *lateDataService) getExistingWindowsBatch(location entity.Location, date time.Time) (map[time.Time]*entity.SFTPTransferLog, error) {
	jakartaDate := date.In(config.GetJakartaTimezone())
	dateStr := jakartaDate.Format("20060102")

	filePattern := fmt.Sprintf("%s_%s_", location.LocationCode, dateStr)

	existingWindowsMap := make(map[time.Time]*entity.SFTPTransferLog)

	since := 24 * time.Hour
	allRecentLogs, err := s.sftpLogRepo.GetRecentByFileName(filePattern, since)
	if err != nil {
		return existingWindowsMap, err
	}

	for _, transferLog := range allRecentLogs {
		if transferLog.TenantID == location.TenantID &&
			transferLog.LocationID == location.ID &&
			strings.HasPrefix(transferLog.FileName, filePattern) &&
			(transferLog.Status == "SUCCESS" || transferLog.Status == "PENDING") {

			if windowTime := s.parseWindowTimeFromFileName(transferLog.FileName); !windowTime.IsZero() {
				existingWindowsMap[windowTime] = transferLog
			}
		}
	}

	return existingWindowsMap, nil
}

func (s *lateDataService) parseWindowTimeFromFileName(fileName string) time.Time {

	parts := strings.Split(fileName, "_")
	if len(parts) < 3 {
		return time.Time{}
	}

	dateStr := parts[1]                             // YYYYMMDD
	timeStr := strings.TrimSuffix(parts[2], ".csv") // HHMM

	if len(dateStr) != 8 || len(timeStr) != 4 {
		return time.Time{}
	}

	fullTimeStr := dateStr + timeStr // YYYYMMDDHHMM
	if windowTime, err := time.ParseInLocation("200601021504", fullTimeStr, config.GetJakartaTimezone()); err == nil {
		return windowTime
	}

	return time.Time{}
}

func (s *lateDataService) regenerateWindowsBatch(tenantID string, location entity.Location, windows []time.Time, allDayReports []entity.DailyReport, existingWindowsMap map[time.Time]*entity.SFTPTransferLog) int {
	if len(windows) == 0 {
		return 0
	}

	successCount := 0
	batchSize := 10

	for i := 0; i < len(windows); i += batchSize {
		end := i + batchSize
		if end > len(windows) {
			end = len(windows)
		}

		batchWindows := windows[i:end]
		batchSuccess := 0

		for _, windowTime := range batchWindows {
			cumulativeReports := s.filterCumulativeDataUpToWindow(allDayReports, windowTime)
			if len(cumulativeReports) == 0 {
				continue
			}

			var existingLog *entity.SFTPTransferLog
			if log, exists := existingWindowsMap[windowTime]; exists {
				existingLog = log
			}

			if err := s.regenerate30MinWindow(tenantID, location, windowTime, cumulativeReports, existingLog); err == nil {
				batchSuccess++
			}
		}

		successCount += batchSuccess

		if len(windows) > 20 && i+batchSize < len(windows) {
			log.Printf("[LATE_DATA] Progress for %s: %d/%d windows processed",
				location.LocationCode, end, len(windows))
		}
	}

	return successCount
}

func (s *lateDataService) regenerate30MinWindow(tenantID string, location entity.Location, windowTime time.Time, allReports []entity.DailyReport, existingLog *entity.SFTPTransferLog) error {
	if len(allReports) == 0 {
		return nil
	}

	cumulativeReports := s.filterCumulativeDataUpToWindow(allReports, windowTime)
	if len(cumulativeReports) == 0 {
		return nil
	}

	filePath, err := s.csvWriter.Write30MinReport(tenantID, location.LocationCode, cumulativeReports, windowTime)
	if err != nil {
		return fmt.Errorf("failed to write 30min report: %w", err)
	}

	if existingLog != nil && existingLog.Status == "SUCCESS" {
		if err := s.markFileAsReplaced(existingLog.ID); err == nil {
			// Success - no need to query again
		}
	} else {
		fileName := filepath.Base(filePath)
		s.markPreviousFileAsReplaced(fileName)
	}

	return s.createLogAndUpload(tenantID, location.ID, filePath, entity.FileType30Min)
}

func (s *lateDataService) markFileAsReplaced(logID string) error {
	replacedMsg := fmt.Sprintf("File replaced with updated data at %s", time.Now().Format("2006-01-02 15:04:05"))
	return s.sftpLogRepo.UpdateStatus(logID, "REPLACED", &replacedMsg)
}

func (s *lateDataService) regenerateDailyReportSafe(tenantID string, location entity.Location, date time.Time, reports []entity.DailyReport) bool {
	if err := s.regenerateDailyReport(tenantID, location, date, reports); err != nil {
		return false
	}
	return true
}

func (s *lateDataService) isLateDataForDifferentDay(reports []entity.DailyReport, checkDate time.Time) bool {
	if len(reports) == 0 {
		return false
	}

	jakartaCheckDate := checkDate.In(config.GetJakartaTimezone())
	checkDateOnly := time.Date(jakartaCheckDate.Year(), jakartaCheckDate.Month(), jakartaCheckDate.Day(), 0, 0, 0, 0, config.GetJakartaTimezone())

	for _, report := range reports {
		if reportTime, err := time.Parse(time.RFC3339, report.Date); err == nil {
			jakartaReportTime := reportTime.In(config.GetJakartaTimezone())
			reportDateOnly := time.Date(jakartaReportTime.Year(), jakartaReportTime.Month(), jakartaReportTime.Day(), 0, 0, 0, 0, config.GetJakartaTimezone())

			if !reportDateOnly.Equal(checkDateOnly) {
				return true
			}
		}
	}
	return false
}

func (s *lateDataService) regenerateCrossDayLateData(tenantID string, location entity.Location, locationReports []entity.DailyReport) error {
	reportsByDate := s.groupReportsByDate(locationReports)
	successCount := 0

	for dateStr := range reportsByDate {
		if date, err := time.Parse("2006-01-02", dateStr); err == nil {
			jakartaDate := date.In(config.GetJakartaTimezone())

			allReportsForDate, err := s.peopleRepo.GetReport(tenantID, location.ID, jakartaDate)
			if err != nil {
				continue
			}

			if s.regenerateDailyReportSafe(tenantID, location, jakartaDate, allReportsForDate) {
				successCount++
			}
		}
	}

	log.Printf("[LATE_DATA] Cross-day regeneration for %s: %d/%d dates successful",
		location.LocationCode, successCount, len(reportsByDate))
	return nil
}

func (s *lateDataService) groupReportsByDate(reports []entity.DailyReport) map[string][]entity.DailyReport {
	reportsByDate := make(map[string][]entity.DailyReport)

	for _, report := range reports {
		if reportTime, err := time.Parse(time.RFC3339, report.Date); err == nil {
			jakartaTime := reportTime.In(config.GetJakartaTimezone())
			dateStr := jakartaTime.Format("2006-01-02")
			reportsByDate[dateStr] = append(reportsByDate[dateStr], report)
		}
	}

	return reportsByDate
}

func (s *lateDataService) mergeAndSortWindows(windows1, windows2 []time.Time) []time.Time {
	windowSet := make(map[time.Time]bool)

	for _, w := range windows1 {
		windowSet[w] = true
	}
	for _, w := range windows2 {
		windowSet[w] = true
	}

	var merged []time.Time
	for window := range windowSet {
		merged = append(merged, window)
	}

	sort.Slice(merged, func(i, j int) bool {
		return merged[i].Before(merged[j])
	})

	return merged
}

func (s *lateDataService) markPreviousFileAsReplaced(fileName string) error {
	recentLogs, err := s.sftpLogRepo.GetRecentByFileName(fileName, 24*time.Hour)
	if err != nil {
		return err
	}

	for _, logEntry := range recentLogs {
		if logEntry.Status == "SUCCESS" {
			return s.markFileAsReplaced(logEntry.ID)
		}
	}
	return nil
}

func (s *lateDataService) regenerateHistoricalLateData(tenantID string, location entity.Location, checkDate time.Time, locationReports []entity.DailyReport) error {
	if len(locationReports) == 0 || tenantID == "" || location.ID == "" {
		return nil
	}

	// Regenerate daily report
	dailySuccess := s.regenerateDailyReportSafe(tenantID, location, checkDate, locationReports)

	// Batch process windows
	allWindows := s.identifyExistingWindows(locationReports)
	windowsSuccess := s.regenerateWindowsBatch(tenantID, location, allWindows, locationReports, nil)

	// Summary
	totalSuccess := 0
	if dailySuccess {
		totalSuccess++
	}
	totalSuccess += windowsSuccess

	log.Printf("[LATE_DATA] Historical regeneration for %s: %d/%d operations successful",
		location.LocationCode, totalSuccess, 1+len(allWindows))

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

	fileName := filepath.Base(filePath)
	s.markPreviousFileAsReplaced(fileName) // Ignore error for non-critical operation

	return s.createLogAndUpload(tenantID, location.ID, filePath, entity.FileTypeDaily)
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

	if s.isDuplicateUploadForLateData(tenantID, locationID, fileName, fileInfo.Size(), recordCount) {
		return nil
	}

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
	fileName := fmt.Sprintf("%s_%s_%s.csv", location.LocationCode, jakartaTime.Format("20060102"), jakartaTime.Format("1504"))

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

func (s *lateDataService) isDuplicateUploadForLateData(tenantID, locationID, fileName string, fileSize int64, recordCount int) bool {
	recentLogs, err := s.sftpLogRepo.GetRecentByFileName(fileName, 5*time.Minute)
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

				// Exact duplicate
				if recordCount == oldRecordCount &&
					recentLog.FileSize == fileSize &&
					timeDiff < 2*time.Minute &&
					(recentLog.Status == "SUCCESS" || recentLog.Status == "PENDING") {
					return true
				}

				// Impossible decrease
				if recordCount < oldRecordCount {
					log.Printf("[LATE_DATA] 🚨 Record count decreased %d→%d for %s", oldRecordCount, recordCount, fileName)
					return true
				}

				// Allow increases (legitimate late data)
				if recordCount > oldRecordCount {
					return false
				}
			}

			// Very recent duplicate
			if timeDiff < 2*time.Minute &&
				recentLog.FileSize == fileSize &&
				(recentLog.Status == "SUCCESS" || recentLog.Status == "PENDING") {
				return true
			}
		}
	}
	return false
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
