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
	log.Printf("[LATE_DATA] Starting %s late data check for tenant %s, date %s",
		checkType, tenantID, checkDate.Format("2006-01-02"))

	if err := s.validateCheckType(checkType); err != nil {
		log.Printf("[LATE_DATA] Invalid check type received: '%s'", checkType)
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

	// BULK: Get all data at once instead of per-location
	var allReports []entity.DailyReport

	switch strings.TrimSpace(checkType) {
	case "realtime":
		jakartaTime := checkDate.In(config.GetJakartaTimezone())
		oneHourAgo := jakartaTime.Add(-1 * time.Hour)

		log.Printf("[LATE_DATA] Fetching all realtime data for tenant %s (bulk query)", tenantID)
		allReports, err = s.peopleRepo.GetAllReportsForTenantWithTimeRange(tenantID, oneHourAgo, jakartaTime)
		if err != nil {
			return fmt.Errorf("failed to get realtime reports: %w", err)
		}

	case "historical":
		log.Printf("[LATE_DATA] Fetching all historical data for tenant %s (bulk query)", tenantID)
		allReports, err = s.peopleRepo.GetAllReportsForTenant(tenantID, checkDate)
		if err != nil {
			return fmt.Errorf("failed to get historical reports: %w", err)
		}

	default:
		log.Printf("[LATE_DATA] Unknown check type: '%s'", checkType)
		return fmt.Errorf("unknown check type: %s", checkType)
	}

	if len(allReports) == 0 {
		log.Printf("[LATE_DATA] No data found for tenant %s (%s check)", tenantID, checkType)
		return nil
	}

	log.Printf("[LATE_DATA] Fetched %d total records for analysis", len(allReports))

	// Group reports by location
	reportsByLocation := s.groupReportsByLocation(allReports)

	processedCount := 0
	for _, location := range locations {
		locationReports, exists := reportsByLocation[location.ID]
		if !exists || len(locationReports) == 0 {
			continue
		}

		var hasLateData bool
		switch strings.TrimSpace(checkType) {
		case "realtime":
			hasLateData = s.hasRecentLateDataWithReports(location, checkDate, locationReports)
		case "historical":
			hasLateData = s.hasHistoricalLateDataWithReports(location, checkDate, locationReports)
		}

		if hasLateData {
			switch strings.TrimSpace(checkType) {
			case "realtime":
				if err := s.regenerateRecentLateDataWithReports(tenantID, location, checkDate, locationReports); err != nil {
					log.Printf("[LATE_DATA] Failed to regenerate recent late data for location %s: %v", location.LocationCode, err)
					continue
				}
			case "historical":
				if err := s.regenerateHistoricalLateDataWithReports(tenantID, location, checkDate, locationReports); err != nil {
					log.Printf("[LATE_DATA] Failed to regenerate historical late data for location %s: %v", location.LocationCode, err)
					continue
				}
			}
			processedCount++
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

// regenerateRecentLateData regenerates recent late data for a specific location
// This method fetches data and then calls the version with reports
func (s *lateDataService) regenerateRecentLateData(tenantID string, location entity.Location, checkDate time.Time) error {
	log.Printf("[LATE_DATA] Regenerating recent late data for location %s", location.LocationCode)

	jakartaTime := checkDate.In(config.GetJakartaTimezone())
	today := time.Date(jakartaTime.Year(), jakartaTime.Month(), jakartaTime.Day(), 0, 0, 0, 0, config.GetJakartaTimezone())

	// BULK: Get all reports for tenant with time range, then filter for this location
	allReports, err := s.peopleRepo.GetAllReportsForTenantWithTimeRange(tenantID, today, jakartaTime)
	if err != nil {
		return fmt.Errorf("failed to get all reports: %w", err)
	}

	// Filter reports for this specific location
	var locationReports []entity.DailyReport
	for _, report := range allReports {
		if report.LocationID == location.ID {
			locationReports = append(locationReports, report)
		}
	}

	if len(locationReports) == 0 {
		log.Printf("[LATE_DATA] No recent data found for location %s", location.LocationCode)
		return nil
	}

	log.Printf("[LATE_DATA] Found %d recent records for location %s", len(locationReports), location.LocationCode)

	// Call the version with pre-fetched reports
	return s.regenerateRecentLateDataWithReports(tenantID, location, checkDate, locationReports)
}

// regenerateRecentLateDataWithReports regenerates recent late data using pre-fetched reports
// for better performance and reliability
func (s *lateDataService) regenerateRecentLateDataWithReports(tenantID string, location entity.Location, checkDate time.Time, locationReports []entity.DailyReport) error {
	startTime := time.Now()
	log.Printf("[LATE_DATA] Starting recent late data regeneration for location %s (%d records)",
		location.LocationCode, len(locationReports))

	// Input validation
	if len(locationReports) == 0 {
		log.Printf("[LATE_DATA] No reports to process for location %s", location.LocationCode)
		return nil
	}

	if tenantID == "" {
		return fmt.Errorf("tenantID cannot be empty")
	}

	if location.ID == "" || location.LocationCode == "" {
		return fmt.Errorf("invalid location data: ID=%s, Code=%s", location.ID, location.LocationCode)
	}

	// Calculate time boundaries
	jakartaTime := checkDate.In(config.GetJakartaTimezone())
	today := time.Date(jakartaTime.Year(), jakartaTime.Month(), jakartaTime.Day(), 0, 0, 0, 0, config.GetJakartaTimezone())
	oneHourAgo := jakartaTime.Add(-1 * time.Hour)

	log.Printf("[LATE_DATA] Time range: %s to %s (recent window: %s to %s)",
		today.Format("2006-01-02 15:04"), jakartaTime.Format("2006-01-02 15:04"),
		oneHourAgo.Format("15:04"), jakartaTime.Format("15:04"))

	// Initialize tracking variables
	var (
		dailySuccess   = false
		windowsSuccess = 0
		windowsTotal   = 0
		errors         []string
	)

	// Step 1: Always regenerate daily report (cumulative for the whole day)
	log.Printf("[LATE_DATA] Step 1/2: Regenerating daily cumulative report for location %s", location.LocationCode)

	dailyStartTime := time.Now()
	if err := s.regenerateDailyReport(tenantID, location, today, locationReports); err != nil {
		errorMsg := fmt.Sprintf("daily report failed: %v", err)
		errors = append(errors, errorMsg)
		log.Printf("[LATE_DATA] ❌ Failed to regenerate daily report for %s: %v", location.LocationCode, err)
	} else {
		dailySuccess = true
		dailyDuration := time.Since(dailyStartTime)
		log.Printf("[LATE_DATA] ✅ Successfully regenerated daily report for %s (took %v)",
			location.LocationCode, dailyDuration)
	}

	// Step 2: Only regenerate 30-minute windows that have new data (recent window)
	log.Printf("[LATE_DATA] Step 2/2: Identifying affected 30-minute windows in recent timeframe for location %s",
		location.LocationCode)

	windowsStartTime := time.Now()
	affectedWindows := s.getAffectedWindows(locationReports, oneHourAgo, jakartaTime)
	windowsTotal = len(affectedWindows)

	if len(affectedWindows) == 0 {
		log.Printf("[LATE_DATA] No affected 30-minute windows found in recent timeframe for location %s",
			location.LocationCode)
	} else {
		log.Printf("[LATE_DATA] Found %d affected 30-minute windows to regenerate for location %s",
			len(affectedWindows), location.LocationCode)

		// Process each affected window
		for i, windowTime := range affectedWindows {
			windowProcessStartTime := time.Now()

			log.Printf("[LATE_DATA] Processing affected window %d/%d: %s for location %s",
				i+1, len(affectedWindows), windowTime.Format("15:04"), location.LocationCode)

			if err := s.regenerate30MinWindow(tenantID, location, windowTime, locationReports); err != nil {
				errorMsg := fmt.Sprintf("window %s failed: %v", windowTime.Format("15:04"), err)
				errors = append(errors, errorMsg)

				log.Printf("[LATE_DATA] ❌ Failed to regenerate 30min window %s for %s: %v",
					windowTime.Format("15:04"), location.LocationCode, err)
			} else {
				windowsSuccess++
				windowDuration := time.Since(windowProcessStartTime)

				log.Printf("[LATE_DATA] ✅ Successfully regenerated 30min window %s for %s (took %v)",
					windowTime.Format("15:04"), location.LocationCode, windowDuration)
			}

			// Progress logging if many windows
			if len(affectedWindows) > 5 && (i+1)%3 == 0 {
				log.Printf("[LATE_DATA] Progress for %s: %d/%d affected windows completed (%.1f%%)",
					location.LocationCode, i+1, len(affectedWindows),
					float64(i+1)/float64(len(affectedWindows))*100)
			}
		}

		windowsTotalDuration := time.Since(windowsStartTime)
		log.Printf("[LATE_DATA] Affected windows regeneration summary for %s: %d/%d successful (took %v)",
			location.LocationCode, windowsSuccess, windowsTotal, windowsTotalDuration)
	}

	// Final summary and result determination
	totalDuration := time.Since(startTime)
	totalOperations := 1 + windowsTotal // daily + affected windows
	totalSuccess := 0
	if dailySuccess {
		totalSuccess++
	}
	totalSuccess += windowsSuccess
	totalErrors := len(errors)

	log.Printf("[LATE_DATA] Recent late data regeneration completed for %s:", location.LocationCode)
	log.Printf("[LATE_DATA] - Duration: %v", totalDuration)
	log.Printf("[LATE_DATA] - Daily report: %s", func() string {
		if dailySuccess {
			return "✅ SUCCESS"
		}
		return "❌ FAILED"
	}())
	log.Printf("[LATE_DATA] - Affected windows: %d/%d successful", windowsSuccess, windowsTotal)
	log.Printf("[LATE_DATA] - Overall: %d/%d operations successful", totalSuccess, totalOperations)

	// Determine final result
	if totalErrors > 0 {
		if totalSuccess == 0 {
			// Complete failure
			log.Printf("[LATE_DATA] ❌ COMPLETE FAILURE: All recent operations failed for location %s", location.LocationCode)
			return fmt.Errorf("all recent regeneration operations failed for location %s: %v",
				location.LocationCode, strings.Join(errors, "; "))
		} else {
			// Partial success
			log.Printf("[LATE_DATA] ⚠️  PARTIAL SUCCESS: Some recent operations failed for location %s", location.LocationCode)
			log.Printf("[LATE_DATA] Errors encountered: %v", strings.Join(errors, "; "))
			// Don't return error for partial success
		}
	} else {
		// Complete success
		log.Printf("[LATE_DATA] ✅ COMPLETE SUCCESS: All recent operations completed successfully for location %s", location.LocationCode)
	}

	return nil
}
func (s *lateDataService) hasHistoricalLateDataWithReports(location entity.Location, checkDate time.Time, reports []entity.DailyReport) bool {
	if len(reports) == 0 {
		return false
	}

	dailyExportTime := s.getLastDailyExportTime(location, checkDate)

	if dailyExportTime.IsZero() {
		log.Printf("[LATE_DATA] No daily export found for location %s on %s, but data exists",
			location.LocationCode, checkDate.Format("2006-01-02"))
		return true
	}

	if time.Since(dailyExportTime) > 6*time.Hour {
		log.Printf("[LATE_DATA] Last export for location %s was %v ago, checking for new data",
			location.LocationCode, time.Since(dailyExportTime))
		return true
	}

	return false
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

func (s *lateDataService) hasRecentLateData(tenantID string, location entity.Location, checkDate time.Time) bool {
	jakartaTime := checkDate.In(config.GetJakartaTimezone())
	oneHourAgo := jakartaTime.Add(-1 * time.Hour)

	reports, err := s.peopleRepo.GetReportWithTimeRange(tenantID, location.ID, oneHourAgo, jakartaTime)
	if err != nil || len(reports) == 0 {
		return false
	}

	lastExportTime := s.getLastExportTimeForWindow(location, jakartaTime)

	if lastExportTime.IsZero() {
		log.Printf("[LATE_DATA] No recent export found for location %s, but new data exists", location.LocationCode)
		return true
	}

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

func (s *lateDataService) hasHistoricalLateData(tenantID string, location entity.Location, checkDate time.Time) bool {
	reports, err := s.peopleRepo.GetReport(tenantID, location.ID, checkDate)
	if err != nil || len(reports) == 0 {
		return false
	}

	dailyExportTime := s.getLastDailyExportTime(location, checkDate)

	if dailyExportTime.IsZero() {
		log.Printf("[LATE_DATA] No daily export found for location %s on %s, but data exists",
			location.LocationCode, checkDate.Format("2006-01-02"))
		return true
	}

	if time.Since(dailyExportTime) > 6*time.Hour {
		log.Printf("[LATE_DATA] Last export for location %s was %v ago, checking for new data",
			location.LocationCode, time.Since(dailyExportTime))
		return true
	}

	return false
}

func (s *lateDataService) regenerateHistoricalLateDataWithReports(tenantID string, location entity.Location, checkDate time.Time, locationReports []entity.DailyReport) error {
	startTime := time.Now()
	log.Printf("[LATE_DATA] Starting historical late data regeneration for location %s (%d records)",
		location.LocationCode, len(locationReports))

	// Input validation
	if len(locationReports) == 0 {
		log.Printf("[LATE_DATA] No reports to process for location %s on %s",
			location.LocationCode, checkDate.Format("2006-01-02"))
		return nil
	}

	if tenantID == "" {
		return fmt.Errorf("tenantID cannot be empty")
	}

	if location.ID == "" || location.LocationCode == "" {
		return fmt.Errorf("invalid location data: ID=%s, Code=%s", location.ID, location.LocationCode)
	}

	// Initialize counters for tracking
	var (
		dailySuccess     = false
		windowsProcessed = 0
		windowsSuccess   = 0
		windowsErrors    = 0
		errors           []string
	)

	// Step 1: Regenerate daily report
	log.Printf("[LATE_DATA] Step 1/2: Regenerating daily report for location %s", location.LocationCode)

	dailyStartTime := time.Now()
	if err := s.regenerateDailyReport(tenantID, location, checkDate, locationReports); err != nil {
		errorMsg := fmt.Sprintf("daily report failed: %v", err)
		errors = append(errors, errorMsg)
		log.Printf("[LATE_DATA] ❌ Failed to regenerate daily report for %s: %v", location.LocationCode, err)
	} else {
		dailySuccess = true
		dailyDuration := time.Since(dailyStartTime)
		log.Printf("[LATE_DATA] ✅ Successfully regenerated daily report for %s (took %v)",
			location.LocationCode, dailyDuration)
	}

	// Step 2: Regenerate all 30-minute windows
	log.Printf("[LATE_DATA] Step 2/2: Identifying and regenerating 30-minute windows for location %s", location.LocationCode)

	allWindows := s.identifyExistingWindows(locationReports)
	windowsProcessed = len(allWindows)

	if len(allWindows) == 0 {
		log.Printf("[LATE_DATA] No 30-minute windows found for location %s on %s",
			location.LocationCode, checkDate.Format("2006-01-02"))
	} else {
		log.Printf("[LATE_DATA] Found %d 30-minute windows to regenerate for location %s",
			len(allWindows), location.LocationCode)

		// Process each window
		windowStartTime := time.Now()
		for i, windowTime := range allWindows {
			windowProcessStartTime := time.Now()

			log.Printf("[LATE_DATA] Processing window %d/%d: %s for location %s",
				i+1, len(allWindows), windowTime.Format("15:04"), location.LocationCode)

			if err := s.regenerate30MinWindow(tenantID, location, windowTime, locationReports); err != nil {
				windowsErrors++
				errorMsg := fmt.Sprintf("window %s failed: %v", windowTime.Format("15:04"), err)
				errors = append(errors, errorMsg)

				log.Printf("[LATE_DATA] ❌ Failed to regenerate 30min window %s for %s: %v",
					windowTime.Format("15:04"), location.LocationCode, err)
			} else {
				windowsSuccess++
				windowDuration := time.Since(windowProcessStartTime)

				log.Printf("[LATE_DATA] ✅ Successfully regenerated 30min window %s for %s (took %v)",
					windowTime.Format("15:04"), location.LocationCode, windowDuration)
			}

			// Progress logging for large number of windows
			if len(allWindows) > 10 && (i+1)%5 == 0 {
				elapsed := time.Since(windowStartTime)
				remaining := len(allWindows) - (i + 1)
				avgTimePerWindow := elapsed / time.Duration(i+1)
				estimatedRemaining := time.Duration(remaining) * avgTimePerWindow

				log.Printf("[LATE_DATA] Progress for %s: %d/%d windows completed (%.1f%%), estimated %v remaining",
					location.LocationCode, i+1, len(allWindows),
					float64(i+1)/float64(len(allWindows))*100, estimatedRemaining)
			}
		}

		windowsTotalDuration := time.Since(windowStartTime)
		log.Printf("[LATE_DATA] 30-minute windows regeneration summary for %s: %d/%d successful, %d errors (took %v)",
			location.LocationCode, windowsSuccess, windowsProcessed, windowsErrors, windowsTotalDuration)
	}

	// Final summary and result determination
	totalDuration := time.Since(startTime)
	totalOperations := 1 + windowsProcessed // daily + windows
	totalSuccess := 0
	if dailySuccess {
		totalSuccess++
	}
	totalSuccess += windowsSuccess
	totalErrors := len(errors)

	log.Printf("[LATE_DATA] Historical regeneration completed for %s:", location.LocationCode)
	log.Printf("[LATE_DATA] - Duration: %v", totalDuration)
	log.Printf("[LATE_DATA] - Daily report: %s", func() string {
		if dailySuccess {
			return "✅ SUCCESS"
		}
		return "❌ FAILED"
	}())
	log.Printf("[LATE_DATA] - 30-min windows: %d/%d successful", windowsSuccess, windowsProcessed)
	log.Printf("[LATE_DATA] - Overall: %d/%d operations successful", totalSuccess, totalOperations)

	// Determine final result
	if totalErrors > 0 {
		if totalSuccess == 0 {
			// Complete failure
			log.Printf("[LATE_DATA] ❌ COMPLETE FAILURE: All operations failed for location %s", location.LocationCode)
			return fmt.Errorf("all regeneration operations failed for location %s: %v",
				location.LocationCode, strings.Join(errors, "; "))
		} else {
			// Partial success
			log.Printf("[LATE_DATA] ⚠️  PARTIAL SUCCESS: Some operations failed for location %s", location.LocationCode)
			log.Printf("[LATE_DATA] Errors encountered: %v", strings.Join(errors, "; "))
			// Don't return error for partial success - let the process continue
		}
	} else {
		// Complete success
		log.Printf("[LATE_DATA] ✅ COMPLETE SUCCESS: All operations completed successfully for location %s", location.LocationCode)
	}

	return nil
}

func (s *lateDataService) regenerateHistoricalLateData(tenantID string, location entity.Location, checkDate time.Time) error {
	log.Printf("[LATE_DATA] Regenerating historical late data for location %s on %s",
		location.LocationCode, checkDate.Format("2006-01-02"))

	// BULK: Get all reports for tenant, then filter for this location
	allReports, err := s.peopleRepo.GetAllReportsForTenant(tenantID, checkDate)
	if err != nil {
		return fmt.Errorf("failed to get all reports for tenant: %w", err)
	}

	// Filter reports for this specific location
	var locationReports []entity.DailyReport
	for _, report := range allReports {
		if report.LocationID == location.ID {
			locationReports = append(locationReports, report)
		}
	}

	if len(locationReports) == 0 {
		log.Printf("[LATE_DATA] No recent data found for location %s", location.LocationCode)
		return nil
	}

	log.Printf("[LATE_DATA] Found %d recent records for location %s", len(locationReports), location.LocationCode)

	jakartaTime := checkDate.In(config.GetJakartaTimezone())
	today := time.Date(jakartaTime.Year(), jakartaTime.Month(), jakartaTime.Day(), 0, 0, 0, 0, config.GetJakartaTimezone())

	// Always regenerate daily report (cumulative for the whole day)
	if err := s.regenerateDailyReport(tenantID, location, today, locationReports); err != nil {
		log.Printf("[LATE_DATA] Failed to regenerate daily report: %v", err)
	}

	// Only regenerate 30-minute windows that have new data
	affectedWindows := s.getAffectedWindows(locationReports, jakartaTime.Add(-1*time.Hour), jakartaTime)

	for _, windowTime := range affectedWindows {
		if err := s.regenerate30MinWindow(tenantID, location, windowTime, locationReports); err != nil {
			log.Printf("[LATE_DATA] Failed to regenerate 30min window %s: %v", windowTime.Format("15:04"), err)
		}
	}

	log.Printf("[LATE_DATA] Recent late data regeneration completed for %s", location.LocationCode)
	return nil
}

func (s *lateDataService) getAffectedWindows(allReports []entity.DailyReport, startTime, endTime time.Time) []time.Time {
	windowSet := make(map[time.Time]bool)

	for _, report := range allReports {
		reportTime, err := time.Parse(time.RFC3339, report.Date)
		if err != nil {
			continue
		}

		jakartaReportTime := reportTime.In(config.GetJakartaTimezone())

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

func (s *lateDataService) regenerateDailyReport(tenantID string, location entity.Location, date time.Time, reports []entity.DailyReport) error {
	filePath, err := s.csvWriter.WriteDailyReport(tenantID, location.LocationCode, reports, date)
	if err != nil {
		return fmt.Errorf("failed to write daily report: %w", err)
	}

	if err := s.createLogAndUpload(tenantID, location.ID, filePath, entity.FileTypeDaily); err != nil {
		return fmt.Errorf("failed to upload daily report: %w", err)
	}

	log.Printf("[LATE_DATA] Successfully regenerated daily report for %s (%d records)",
		location.LocationCode, len(reports))
	return nil
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

	if err := s.createLogAndUpload(tenantID, location.ID, filePath, entity.FileType30Min); err != nil {
		return fmt.Errorf("failed to upload 30min report: %w", err)
	}

	log.Printf("[LATE_DATA] Successfully regenerated 30min window %s for %s (%d records)",
		windowTime.Format("15:04"), location.LocationCode, len(cumulativeReports))
	return nil
}

func (s *lateDataService) createLogAndUpload(tenantID, locationID, filePath, fileType string) error {
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

	log.Printf("[LATE_DATA] Created log entry: %s (ID: %s) [LATE DATA REGENERATION]", fileName, transferLog.ID)

	// Create upload job with LogID
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

	// Use SFTP service directly for immediate upload (priority for late data)
	if err := s.sftpService.UploadFile(uploadJob); err != nil {
		errorMsg := err.Error()
		s.sftpLogRepo.UpdateStatus(transferLog.ID, "FAILED", &errorMsg)
		return fmt.Errorf("upload failed: %w", err)
	}

	log.Printf("[LATE_DATA] Successfully uploaded %s (%d records) [LATE DATA REGENERATION]", fileName, recordCount)
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

func (s *lateDataService) groupReportsByLocation(allReports []entity.DailyReport) map[string][]entity.DailyReport {
	reportsByLocation := make(map[string][]entity.DailyReport)

	for _, report := range allReports {
		reportsByLocation[report.LocationID] = append(reportsByLocation[report.LocationID], report)
	}

	return reportsByLocation
}

func (s *lateDataService) hasRecentLateDataWithReports(location entity.Location, checkDate time.Time, reports []entity.DailyReport) bool {
	if len(reports) == 0 {
		return false
	}

	jakartaTime := checkDate.In(config.GetJakartaTimezone())
	lastExportTime := s.getLastExportTimeForWindow(location, jakartaTime)

	if lastExportTime.IsZero() {
		log.Printf("[LATE_DATA] No recent export found for location %s, but new data exists", location.LocationCode)
		return true
	}

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
