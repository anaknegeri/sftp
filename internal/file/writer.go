package file

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"jarvist/sftp-service/internal/domain/entity"
)

type CSVWriter interface {
	Write30MinReport(tenantID, locationCode string, reports []entity.DailyReport, date time.Time) (string, error)
	WriteDailyReport(tenantID, locationCode string, reports []entity.DailyReport, date time.Time) (string, error)
	WriteCombinedReport(tenantID, locationCode string, reports []entity.DailyReport, date time.Time) (string, error) // NEW
}

type csvWriter struct {
	basePath string
}

func NewCSVWriter(basePath string) CSVWriter {
	return &csvWriter{basePath: basePath}
}

var jakartaLocation *time.Location

func init() {
	var err error
	jakartaLocation, err = time.LoadLocation("Asia/Jakarta")
	if err != nil {
		panic("failed to load Asia/Jakarta timezone: " + err.Error())
	}
}

func (w *csvWriter) Write30MinReport(tenantID, locationCode string, reports []entity.DailyReport, date time.Time) (string, error) {
	jakartaDate := date.In(jakartaLocation)
	fileName := fmt.Sprintf("%s_%s_%s.csv", locationCode, jakartaDate.Format("20060102"), jakartaDate.Format("1504"))
	return w.writeReport(tenantID, fileName, reports)
}

func (w *csvWriter) WriteDailyReport(tenantID, locationCode string, reports []entity.DailyReport, date time.Time) (string, error) {
	jakartaDate := date.In(jakartaLocation)
	fileName := fmt.Sprintf("%s_%s.csv", locationCode, jakartaDate.Format("20060102"))
	return w.writeReport(tenantID, fileName, reports)
}

func (w *csvWriter) writeReport(tenantID, fileName string, reports []entity.DailyReport) (string, error) {
	log.Printf("[CSV_WRITER] Starting to write %d reports to file %s", len(reports), fileName)

	if len(reports) == 0 {
		return "", fmt.Errorf("no data to export")
	}

	tenantDir := filepath.Join(w.basePath, tenantID)
	if err := os.MkdirAll(tenantDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create tenant directory: %w", err)
	}

	filePath := filepath.Join(tenantDir, fileName)
	file, err := os.Create(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	csvWriter := csv.NewWriter(file)
	defer csvWriter.Flush()

	header := []string{"Store_ID", "Device_ID", "Date", "Hour", "Total enters", "Total exited"}
	if err := csvWriter.Write(header); err != nil {
		return "", fmt.Errorf("failed to write header: %w", err)
	}

	successCount := 0
	errorCount := 0

	for i, report := range reports {
		t, err := w.parsePostgreSQLTimestamp(report.Date)
		if err != nil {
			errorCount++
			log.Printf("[CSV_WRITER] ERROR: Failed to parse date for record %d: '%s' - %v", i+1, report.Date, err)
			continue
		}

		row := []string{
			report.LocationCode,
			report.DeviceName,
			t.Format("20060102"),
			t.Format("150405"),
			strconv.FormatInt(report.TotalIn, 10),
			strconv.FormatInt(report.TotalOut, 10),
		}

		if err := csvWriter.Write(row); err != nil {
			errorCount++
			log.Printf("[CSV_WRITER] ERROR: Failed to write record %d: %v", i+1, err)
			continue
		}

		successCount++
	}

	log.Printf("[CSV_WRITER] Completed writing %s: %d success, %d errors out of %d total",
		fileName, successCount, errorCount, len(reports))

	if successCount == 0 {
		return "", fmt.Errorf("no valid records written to file - all %d records had errors", len(reports))
	}

	return filePath, nil
}

func (w *csvWriter) parsePostgreSQLTimestamp(dateStr string) (time.Time, error) {
	if dateStr == "" {
		return time.Time{}, fmt.Errorf("empty date string")
	}

	formats := []string{
		"2006-01-02 15:04:05+07",    // "2025-06-24 08:30:00+07" (exact match)
		"2006-01-02 15:04:05-07",    // Negative timezone
		"2006-01-02 15:04:05Z",      // UTC
		"2006-01-02 15:04:05",       // Tanpa timezone
		"2006-01-02T15:04:05+07:00", // RFC3339 format
		"2006-01-02T15:04:05Z",      // RFC3339 UTC
		time.RFC3339,                // Standard RFC3339
		time.RFC3339Nano,            // RFC3339 with nanoseconds
	}

	var lastErr error
	for _, format := range formats {
		if t, err := time.Parse(format, dateStr); err == nil {
			if format == "2006-01-02 15:04:05" {
				return t.In(jakartaLocation), nil
			}
			return t, nil
		} else {
			lastErr = err
		}
	}

	return time.Time{}, fmt.Errorf("failed to parse PostgreSQL timestamp '%s' with any known format: %w", dateStr, lastErr)
}

func (w *csvWriter) WriteCombinedReport(tenantID, locationCode string, reports []entity.DailyReport, date time.Time) (string, error) {
	jakartaDate := date.In(jakartaLocation)
	fileName := fmt.Sprintf("%s.csv", jakartaDate.Format("20060102"))

	log.Printf("[CSV_WRITER] Starting to write combined report with %d reports to file %s", len(reports), fileName)

	if len(reports) == 0 {
		return "", fmt.Errorf("no data to export for combined report")
	}

	tenantDir := filepath.Join(w.basePath, tenantID)
	if err := os.MkdirAll(tenantDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create tenant directory: %w", err)
	}

	filePath := filepath.Join(tenantDir, fileName)
	file, err := os.Create(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to create combined file: %w", err)
	}
	defer file.Close()

	csvWriter := csv.NewWriter(file)
	defer csvWriter.Flush()

	// Same header format as individual reports
	header := []string{"Store_ID", "Device_ID", "Date", "Hour", "Total enters", "Total exited"}
	if err := csvWriter.Write(header); err != nil {
		return "", fmt.Errorf("failed to write header: %w", err)
	}

	// Sort reports by location code first, then by time for better organization
	sortedReports := make([]entity.DailyReport, len(reports))
	copy(sortedReports, reports)

	sort.Slice(sortedReports, func(i, j int) bool {
		if sortedReports[i].LocationCode != sortedReports[j].LocationCode {
			return sortedReports[i].LocationCode < sortedReports[j].LocationCode
		}
		return sortedReports[i].Date < sortedReports[j].Date
	})

	successCount := 0
	errorCount := 0

	for i, report := range sortedReports {
		t, err := w.parsePostgreSQLTimestamp(report.Date)
		if err != nil {
			errorCount++
			log.Printf("[CSV_WRITER] ERROR: Failed to parse date for combined record %d: '%s' - %v", i+1, report.Date, err)
			continue
		}

		row := []string{
			report.LocationCode,
			report.DeviceName,
			t.Format("20060102"),
			t.Format("150405"),
			strconv.FormatInt(report.TotalIn, 10),
			strconv.FormatInt(report.TotalOut, 10),
		}

		if err := csvWriter.Write(row); err != nil {
			errorCount++
			log.Printf("[CSV_WRITER] ERROR: Failed to write combined record %d: %v", i+1, err)
			continue
		}

		successCount++
	}

	log.Printf("[CSV_WRITER] Completed writing combined report %s: %d success, %d errors out of %d total",
		fileName, successCount, errorCount, len(sortedReports))

	if successCount == 0 {
		return "", fmt.Errorf("no valid records written to combined file - all %d records had errors", len(sortedReports))
	}

	return filePath, nil
}
