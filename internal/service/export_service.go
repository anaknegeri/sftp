package service

import (
	"fmt"
	"jarvist/sftp/internal/domain/entity"
	"jarvist/sftp/internal/file"
	"jarvist/sftp/internal/repository"
	"log"
	"os"
	"sort"
	"time"
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
}

func NewExportService(
	peopleRepo repository.PeopleCountRepository,
	sftpLogRepo repository.SFTPLogRepository,
	csvWriter file.CSVWriter,
	localPath string,
) ExportService {
	return &exportService{
		peopleRepo:  peopleRepo,
		sftpLogRepo: sftpLogRepo,
		csvWriter:   csvWriter,
		localPath:   localPath,
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
func (s *exportService) processDailyReport(tenantID string, location entity.Location, date time.Time) error {
	log.Printf("[DAILY] Processing location %s for date %s", location.LocationCode, date.Format("2006-01-02"))

	reports, err := s.peopleRepo.GetReport(tenantID, location.ID, date)
	if err != nil {
		return fmt.Errorf("failed to get reports for location %s: %w", location.LocationCode, err)
	}

	if len(reports) == 0 {
		log.Printf("[DAILY] No data found for location %s on %s", location.LocationCode, date.Format("2006-01-02"))
		return nil
	}

	filePath, err := s.csvWriter.WriteDailyReport(tenantID, location.LocationCode, reports, date)
	if err != nil {
		return fmt.Errorf("failed to write daily CSV for location %s: %w", location.LocationCode, err)
	}

	recordCount := s.countFileRecords(filePath)
	log.Printf("[DAILY] Successfully exported %d records for location %s", recordCount, location.LocationCode)
	return nil
}

// process30MinReport handles 30-minute cumulative report processing for a single location
func (s *exportService) process30MinReport(tenantID string, location entity.Location, triggerTime time.Time) error {
	log.Printf("[30MIN] Processing location %s for trigger time %s",
		location.LocationCode, triggerTime.Format("2006-01-02 15:04"))

	today := time.Date(triggerTime.Year(), triggerTime.Month(), triggerTime.Day(), 0, 0, 0, 0, triggerTime.Location())

	allReports, err := s.peopleRepo.GetReportWithTimeRange(tenantID, location.ID, today, triggerTime)
	if err != nil {
		return fmt.Errorf("failed to get reports for location %s: %w", location.LocationCode, err)
	}

	if len(allReports) == 0 {
		log.Printf("[30MIN] No cumulative data found for location %s up to %s",
			location.LocationCode, triggerTime.Format("15:04"))
		return nil
	}

	currentWindow := triggerTime.Truncate(30 * time.Minute)
	filePath, err := s.csvWriter.Write30MinReport(tenantID, location.LocationCode, allReports, currentWindow)
	if err != nil {
		return fmt.Errorf("failed to write 30-minute cumulative report for location %s: %w", location.LocationCode, err)
	}

	recordCount := s.countFileRecords(filePath)
	log.Printf("[30MIN] Successfully exported %d cumulative records for location %s at window %s",
		recordCount, location.LocationCode, currentWindow.Format("15:04"))
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

	// Write daily report
	dailyFilePath, err := s.csvWriter.WriteDailyReport(tenantID, location.LocationCode, reports, date)
	if err != nil {
		return fmt.Errorf("failed to write daily CSV for location %s: %w", location.LocationCode, err)
	}

	dailyRecordCount := s.countFileRecords(dailyFilePath)
	log.Printf("[COMPLETE] Daily report exported: %d records for location %s", dailyRecordCount, location.LocationCode)

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

		filePath, err := s.csvWriter.Write30MinReport(tenantID, location.LocationCode, cumulativeReports, windowTime)
		if err != nil {
			log.Printf("[30MIN] Failed to write 30-minute report for window %s at location %s: %v",
				windowTime.Format("15:04"), location.LocationCode, err)
			continue
		}

		recordCount := s.countFileRecords(filePath)
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

// validateLocationExists checks if location exists without fetching all data
func (s *exportService) validateLocationExists(tenantID, locationID string) error {
	exists, err := s.peopleRepo.LocationExists(tenantID, locationID)
	if err != nil {
		log.Printf("[ERROR] Failed to validate location %s for tenant %s: %v", locationID, tenantID, err)
		return fmt.Errorf("failed to validate location: %w", err)
	}

	if !exists {
		log.Printf("[ERROR] Location %s not found for tenant %s", locationID, tenantID)
		return fmt.Errorf("location %s not found for tenant %s", locationID, tenantID)
	}

	log.Printf("[INFO] Location %s validated for tenant %s", locationID, tenantID)
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
func (s *exportService) filterCumulativeDataUpToWindow(allReports []entity.DailyReport, windowTime time.Time) []entity.DailyReport {
	var cumulativeReports []entity.DailyReport

	for _, report := range allReports {
		reportTime, err := time.Parse(time.RFC3339, report.Date)
		if err != nil {
			log.Printf("[WARNING] Failed to parse report date %s for cumulative filtering: %v", report.Date, err)
			continue
		}

		if reportTime.Before(windowTime) || reportTime.Equal(windowTime) {
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
