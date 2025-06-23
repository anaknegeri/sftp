package file

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"jarvist/sftp-service/internal/domain/entity"
)

type CSVWriter interface {
	Write30MinReport(tenantID, locationCode string, reports []entity.DailyReport, date time.Time) (string, error)
	WriteDailyReport(tenantID, locationCode string, reports []entity.DailyReport, date time.Time) (string, error)
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

	for _, report := range reports {
		t, err := time.Parse(time.RFC3339, report.Date)
		if err != nil {
			return "", fmt.Errorf("failed to parse date: %w", err)
		}

		row := []string{
			report.LocationCode,
			report.DeviceName,
			t.Format("20060102"),
			t.Format("1504"),
			strconv.FormatInt(report.TotalIn, 10),
			strconv.FormatInt(report.TotalOut, 10),
		}

		if err := csvWriter.Write(row); err != nil {
			return "", fmt.Errorf("failed to write row: %w", err)
		}
	}

	return filePath, nil
}
