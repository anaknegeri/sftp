package repository

import (
	"context"
	"fmt"
	"time"

	"jarvist/sftp-service/internal/config"
	"jarvist/sftp-service/internal/domain/entity"

	"gorm.io/gorm"
)

type PeopleCountRepository interface {
	GetLocations(tenantID string) ([]entity.Location, error)
	GetLocationByID(tenantID, locationID string) (entity.Location, error)
	LocationExists(tenantID, locationID string) (bool, error)
	GetReport(tenantID, locationID string, date time.Time) ([]entity.DailyReport, error)
	GetReportWithTimeRange(tenantID, locationID string, startTime, endTime time.Time) ([]entity.DailyReport, error)
}

type peopleCountRepository struct {
	db *gorm.DB
}

func NewPeopleCountRepository(db *gorm.DB) PeopleCountRepository {
	return &peopleCountRepository{db: db}
}

func (r *peopleCountRepository) GetLocations(tenantID string) ([]entity.Location, error) {
	var locations []entity.Location

	result := r.db.Where("tenant_id = ? AND status = ?", tenantID, "active").
		Order("location_code ASC").
		Find(&locations)

	if result.Error != nil {
		return nil, fmt.Errorf("failed to get locations: %w", result.Error)
	}

	return locations, nil
}

func (r *peopleCountRepository) GetLocationByID(tenantID, locationID string) (entity.Location, error) {
	var location entity.Location

	result := r.db.Where("tenant_id = ? AND id = ? AND status = ?", tenantID, locationID, "active").
		First(&location)

	if result.Error != nil {
		if result.Error == gorm.ErrRecordNotFound {
			return entity.Location{}, fmt.Errorf("location %s not found for tenant %s", locationID, tenantID)
		}
		return entity.Location{}, fmt.Errorf("failed to get location by ID: %w", result.Error)
	}

	return location, nil
}

func (r *peopleCountRepository) LocationExists(tenantID, locationID string) (bool, error) {
	var count int64

	result := r.db.Model(&entity.Location{}).
		Where("tenant_id = ? AND id = ? AND status = ?", tenantID, locationID, "active").
		Count(&count)

	if result.Error != nil {
		return false, fmt.Errorf("failed to check location existence: %w", result.Error)
	}

	return count > 0, nil
}

func (r *peopleCountRepository) GetReport(tenantID, locationID string, date time.Time) ([]entity.DailyReport, error) {
	timeRange := NewDailyTimeRange(date)
	return r.executeReportQuery(tenantID, locationID, timeRange.StartTime, timeRange.EndTime)
}

func (r *peopleCountRepository) GetReportWithTimeRange(tenantID, locationID string, startTime, endTime time.Time) ([]entity.DailyReport, error) {
	return r.executeReportQuery(tenantID, locationID, startTime, endTime)
}

func (r *peopleCountRepository) executeReportQuery(tenantID, locationID string, startTime, endTime time.Time) ([]entity.DailyReport, error) {
	var reports []entity.DailyReport

	query := `
		SELECT
		  time_bucket,
		  pc.tenant_id,
		  pc.location_id,
		  pc.location_code,
		  pc.location_name,
		  pc.device_id,
		  pc.device_name,
		  SUM(pc.count_in) AS total_in,
		  SUM(pc.count_out) AS total_out
		FROM (
		  SELECT
		    people_counts.*,
		    locations.location_code,
		    locations.name AS location_name,
		    devices.device_name,
		    date_trunc('hour', people_counts.timestamp) +
		    INTERVAL '30 minutes' * FLOOR(EXTRACT(MINUTE FROM people_counts.timestamp) / 30) AS time_bucket
		  FROM people_counts
		  JOIN locations ON people_counts.location_id = locations.id
		  JOIN devices ON people_counts.device_id = devices.id
		  WHERE people_counts.tenant_id = ?
		    AND people_counts.location_id = ?
		    AND people_counts.timestamp >= ?
		    AND people_counts.timestamp <= ?
		) pc
		GROUP BY time_bucket, pc.tenant_id, pc.location_id, pc.location_code, pc.location_name, pc.device_id, pc.device_name
		ORDER BY time_bucket, pc.device_name
	`

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	rows, err := r.db.WithContext(ctx).Raw(query, tenantID, locationID, startTime, endTime).Rows()
	if err != nil {
		return nil, fmt.Errorf("failed to query reports: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var report entity.DailyReport
		var dateStr string

		if err := rows.Scan(
			&dateStr,
			&report.TenantID,
			&report.LocationID,
			&report.LocationCode,
			&report.LocationName,
			&report.DeviceID,
			&report.DeviceName,
			&report.TotalIn,
			&report.TotalOut,
		); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		report.Date = dateStr
		reports = append(reports, report)
	}

	return reports, nil
}

type TimeRange struct {
	StartTime time.Time
	EndTime   time.Time
}

func NewDailyTimeRange(date time.Time) TimeRange {
	jakartaDate := date.In(config.GetJakartaTimezone())
	start := time.Date(jakartaDate.Year(), jakartaDate.Month(), jakartaDate.Day(), 0, 0, 0, 0, config.GetJakartaTimezone())
	end := start.Add(24*time.Hour - time.Second)
	return TimeRange{StartTime: start, EndTime: end}
}
