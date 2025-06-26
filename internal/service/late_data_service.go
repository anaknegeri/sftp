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
	log.Printf("[LATE_DATA] Starting %s check for tenant %s", checkType, tenantID)

	if err := s.validate(checkType); err != nil {
		return err
	}

	locations, err := s.peopleRepo.GetLocations(tenantID)
	if err != nil {
		return fmt.Errorf("failed to get locations: %w", err)
	}

	if len(locations) == 0 {
		return nil
	}

	affectedLocations, err := s.detect(tenantID, checkDate, checkType)
	if err != nil {
		return fmt.Errorf("failed to detect late data: %w", err)
	}

	if len(affectedLocations) == 0 {
		return nil
	}

	processedCount := 0
	for _, location := range locations {
		if !s.contains(affectedLocations, location.ID) {
			continue
		}

		if err := s.process(tenantID, location, checkDate, checkType); err != nil {
			log.Printf("[LATE_DATA] Failed to process %s: %v", location.LocationCode, err)
			continue
		}
		processedCount++
	}

	if processedCount > 0 {
		if err := s.regenerateCombined(tenantID, checkDate); err != nil {
			log.Printf("[LATE_DATA] Failed to regenerate combined: %v", err)
		}
	}

	log.Printf("[LATE_DATA] Completed: %d/%d locations processed", processedCount, len(affectedLocations))
	return nil
}

func (s *lateDataService) detect(tenantID string, checkDate time.Time, checkType string) ([]string, error) {
	scheduleConfig := config.GetLateDataScheduleConfig()
	jakartaTime := checkDate.In(config.GetJakartaTimezone())

	var insertedSince, timestampBefore time.Time

	switch strings.TrimSpace(checkType) {
	case "realtime":
		lookBack := time.Duration(scheduleConfig.ThirtyMinLookBackMinute) * time.Microsecond
		insertedSince = jakartaTime.Add(-lookBack)
		timestampBefore = jakartaTime.Add(-30 * time.Minute)
	case "historical":
		insertedSince = jakartaTime.Add(-24 * time.Hour)
		businessHours := config.BusinessHours(jakartaTime)
		timestampBefore = businessHours.EndTime
	default:
		return nil, fmt.Errorf("unknown check type: %s", checkType)
	}

	reports, err := s.peopleRepo.GetLateData(tenantID, insertedSince, timestampBefore)
	if err != nil {
		return nil, err
	}

	locationSet := make(map[string]bool)
	for _, report := range reports {
		locationSet[report.LocationID] = true
	}

	var locations []string
	for locationID := range locationSet {
		locations = append(locations, locationID)
	}

	return locations, nil
}

func (s *lateDataService) process(tenantID string, location entity.Location, checkDate time.Time, checkType string) error {
	switch strings.TrimSpace(checkType) {
	case "realtime":
		return s.processRealtime(tenantID, location, checkDate)
	case "historical":
		return s.processHistorical(tenantID, location, checkDate)
	default:
		return fmt.Errorf("unknown check type: %s", checkType)
	}
}

func (s *lateDataService) processRealtime(tenantID string, location entity.Location, checkDate time.Time) error {
	jakartaTime := checkDate.In(config.GetJakartaTimezone())
	businessHours := config.BusinessHours(jakartaTime)

	reports, err := s.peopleRepo.GetReportWithTimeRange(tenantID, location.ID, businessHours.StartTime, jakartaTime)
	if err != nil {
		return err
	}

	if len(reports) == 0 {
		return nil
	}

	s.regenDaily(tenantID, location, jakartaTime, reports)
	s.regenWindows(tenantID, location, jakartaTime, reports)

	return nil
}

func (s *lateDataService) processHistorical(tenantID string, location entity.Location, checkDate time.Time) error {
	jakartaDate := checkDate.In(config.GetJakartaTimezone())

	reports, err := s.peopleRepo.GetReport(tenantID, location.ID, jakartaDate)
	if err != nil {
		return err
	}

	if len(reports) == 0 {
		return nil
	}

	s.regenDaily(tenantID, location, jakartaDate, reports)
	return nil
}

func (s *lateDataService) regenDaily(tenantID string, location entity.Location, date time.Time, reports []entity.DailyReport) {
	filePath, err := s.csvWriter.WriteDailyReport(tenantID, location.LocationCode, reports, date)
	if err != nil {
		return
	}

	fileName := filepath.Base(filePath)
	s.markReplaced(fileName)
	s.upload(tenantID, location.ID, filePath, entity.FileTypeDaily)
}

func (s *lateDataService) regenWindows(tenantID string, location entity.Location, currentTime time.Time, allReports []entity.DailyReport) {
	windowsMap := s.groupByWindows(allReports)

	var windows []time.Time
	for windowTime := range windowsMap {
		if windowTime.Before(currentTime) || windowTime.Equal(currentTime) {
			windows = append(windows, windowTime)
		}
	}

	sort.Slice(windows, func(i, j int) bool {
		return windows[i].Before(windows[j])
	})

	for _, windowTime := range windows {
		cumulativeReports := s.filterUpToWindow(allReports, windowTime)
		if len(cumulativeReports) == 0 {
			continue
		}

		filePath, err := s.csvWriter.Write30MinReport(tenantID, location.LocationCode, cumulativeReports, windowTime)
		if err != nil {
			continue
		}

		fileName := filepath.Base(filePath)
		s.markReplaced(fileName)
		s.upload(tenantID, location.ID, filePath, entity.FileType30Min)
	}
}

func (s *lateDataService) regenerateCombined(tenantID string, checkDate time.Time) error {
	jakartaDate := checkDate.In(config.GetJakartaTimezone())
	businessHours := config.BusinessHours(jakartaDate)

	completeData, err := s.peopleRepo.GetAllReportsForTenantWithTimeRange(tenantID, businessHours.StartTime, businessHours.EndTime)
	if err != nil {
		return err
	}

	if len(completeData) == 0 {
		return nil
	}

	reportsByDate := s.groupByDate(completeData)

	for dateStr, reportsForDate := range reportsByDate {
		reportDate, err := time.Parse("2006-01-02", dateStr)
		if err != nil {
			continue
		}

		fileName := fmt.Sprintf("*%s.csv", reportDate.Format("20060102"))
		s.markReplaced(fileName)

		filePath, err := s.csvWriter.WriteCombinedReport(tenantID, "*", reportsForDate, reportDate)
		if err != nil {
			continue
		}

		s.upload(tenantID, "ALL", filePath, entity.FileType30Min)
	}

	return nil
}

func (s *lateDataService) upload(tenantID, locationID, filePath, fileType string) error {
	fileName := filepath.Base(filePath)
	tenantConfig, exists := config.GetTenantByID(tenantID)
	if !exists {
		return fmt.Errorf("tenant not found")
	}

	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return err
	}

	recordCount := utils.CountFileRecords(filePath)

	var remotePath string
	isCombined := strings.HasPrefix(fileName, "*") || locationID == "ALL"

	if isCombined {
		remotePath = strings.ReplaceAll(filepath.Join(tenantConfig.SFTP.CombinedPath, fileName), "\\", "/")
	} else {
		remotePath = strings.ReplaceAll(filepath.Join(tenantConfig.SFTP.BasePath, fileName), "\\", "/")
	}

	if s.isDuplicate(tenantID, locationID, fileName, fileInfo.Size(), recordCount) {
		return nil
	}

	transferLog := &entity.SFTPTransferLog{
		ID:                uuid.New().String(),
		TenantID:          tenantID,
		FileName:          fileName,
		FilePath:          filePath,
		RemotePath:        remotePath,
		Status:            "PENDING",
		FileSize:          fileInfo.Size(),
		TransferStartTime: time.Now(),
		RecordCount:       &recordCount,
		FileType:          fileType,
		CreatedAt:         time.Now(),
		Environment:       config.GetEnvironment(),
	}

	if locationID != "ALL" {
		transferLog.LocationID = &locationID
	}

	if err := s.sftpLogRepo.Create(transferLog); err != nil {
		return err
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
		return err
	}

	return nil
}

func (s *lateDataService) groupByWindows(reports []entity.DailyReport) map[time.Time][]entity.DailyReport {
	windows := make(map[time.Time][]entity.DailyReport)
	for _, report := range reports {
		if reportTime, err := time.Parse(time.RFC3339, report.Date); err == nil {
			windowStart := reportTime.Truncate(30 * time.Minute).Add(30 * time.Minute)
			windows[windowStart] = append(windows[windowStart], report)
		}
	}
	return windows
}

func (s *lateDataService) groupByDate(reports []entity.DailyReport) map[string][]entity.DailyReport {
	reportsByDate := make(map[string][]entity.DailyReport)

	for _, report := range reports {
		if reportTime, err := time.Parse(time.RFC3339, report.Date); err == nil {
			jakartaTime := reportTime.In(config.GetJakartaTimezone())
			businessDate := s.getBusinessDate(jakartaTime)
			dateStr := businessDate.Format("2006-01-02")
			reportsByDate[dateStr] = append(reportsByDate[dateStr], report)
		}
	}

	return reportsByDate
}

func (s *lateDataService) getBusinessDate(jakartaTime time.Time) time.Time {
	if jakartaTime.Hour() < 8 {
		return jakartaTime.AddDate(0, 0, -1)
	}
	return jakartaTime
}

func (s *lateDataService) filterUpToWindow(allReports []entity.DailyReport, windowTime time.Time) []entity.DailyReport {
	var filtered []entity.DailyReport
	jakartaWindowTime := windowTime.In(config.GetJakartaTimezone())

	for _, report := range allReports {
		if reportTime, err := time.Parse(time.RFC3339, report.Date); err == nil {
			jakartaReportTime := reportTime.In(config.GetJakartaTimezone())
			if jakartaReportTime.Before(jakartaWindowTime) || jakartaReportTime.Equal(jakartaWindowTime) {
				filtered = append(filtered, report)
			}
		}
	}

	return filtered
}

func (s *lateDataService) markReplaced(fileName string) {
	recentLogs, err := s.sftpLogRepo.GetRecentByFileName(fileName, 24*time.Hour)
	if err != nil {
		return
	}

	for _, logEntry := range recentLogs {
		if logEntry.Status == "SUCCESS" {
			replacedMsg := fmt.Sprintf("Replaced at %s", time.Now().Format("2006-01-02 15:04:05"))
			s.sftpLogRepo.UpdateStatus(logEntry.ID, "REPLACED", &replacedMsg)
			return
		}
	}
}

func (s *lateDataService) isDuplicate(tenantID, locationID, fileName string, fileSize int64, recordCount int) bool {
	recentLogs, err := s.sftpLogRepo.GetRecentByFileName(fileName, 2*time.Minute)
	if err != nil {
		return false
	}

	for _, recentLog := range recentLogs {
		if recentLog.TenantID == tenantID {
			var logLocationID string
			if recentLog.LocationID != nil {
				logLocationID = *recentLog.LocationID
			} else {
				logLocationID = "ALL"
			}

			if logLocationID == locationID {
				if recentLog.Status == "REPLACED" {
					continue
				}

				if recentLog.RecordCount != nil {
					oldCount := *recentLog.RecordCount
					if recordCount == oldCount && recentLog.FileSize == fileSize &&
						(recentLog.Status == "SUCCESS" || recentLog.Status == "PENDING") {
						return true
					}
					if recordCount < oldCount {
						return true
					}
				}

				if recentLog.FileSize == fileSize &&
					(recentLog.Status == "SUCCESS" || recentLog.Status == "PENDING") {
					return true
				}
			}
		}
	}
	return false
}

func (s *lateDataService) validate(checkType string) error {
	validTypes := []string{"realtime", "historical"}
	trimmed := strings.TrimSpace(checkType)

	for _, valid := range validTypes {
		if trimmed == valid {
			return nil
		}
	}

	return fmt.Errorf("invalid check type: %s", checkType)
}

func (s *lateDataService) contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
