package repository

import (
	"context"
	"fmt"
	"log"
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
	GetAllReportsForTenant(tenantID string, date time.Time) ([]entity.DailyReport, error)
	GetAllReportsForTenantWithTimeRange(tenantID string, startTime, endTime time.Time) ([]entity.DailyReport, error)
}

type peopleCountRepository struct {
	db  *gorm.DB
	cfg *config.Config
}

func NewPeopleCountRepository(db *gorm.DB, cfg *config.Config) PeopleCountRepository {
	return &peopleCountRepository{
		db:  db,
		cfg: cfg,
	}
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
	timeRange := config.BusinessHours(date)
	return r.executeReportQuery(tenantID, locationID, timeRange.StartTime, timeRange.EndTime)
}

func (r *peopleCountRepository) GetReportWithTimeRange(tenantID, locationID string, startTime, endTime time.Time) ([]entity.DailyReport, error) {
	return r.executeReportQuery(tenantID, locationID, startTime, endTime)
}

func (r *peopleCountRepository) GetAllReportsForTenant(tenantID string, date time.Time) ([]entity.DailyReport, error) {
	timeRange := config.BusinessHours(date)
	return r.executeAllReportsQuery(tenantID, timeRange.StartTime, timeRange.EndTime)
}

func (r *peopleCountRepository) GetAllReportsForTenantWithTimeRange(tenantID string, startTime, endTime time.Time) ([]entity.DailyReport, error) {
	return r.executeAllReportsQuery(tenantID, startTime, endTime)
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

	// Use default timeout for single location queries
	timeout := config.DefaultQueryTimeout
	if r.cfg != nil {
		timeout = r.cfg.Database.DefaultTimeout
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	log.Printf("[REPO] Executing single location query with %v timeout", timeout)

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

func (r *peopleCountRepository) executeAllReportsQuery(tenantID string, startTime, endTime time.Time) ([]entity.DailyReport, error) {
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
		    AND people_counts.timestamp >= ?
		    AND people_counts.timestamp <= ?
		    AND locations.status = 'active'
		) pc
		GROUP BY time_bucket, pc.tenant_id, pc.location_id, pc.location_code, pc.location_name, pc.device_id, pc.device_name
		ORDER BY pc.location_code, time_bucket, pc.device_name
	`

	// Use bulk timeout for all-tenant queries
	timeout := config.BulkQueryTimeout
	if r.cfg != nil {
		timeout = r.cfg.Database.BulkTimeout
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	log.Printf("[REPO] Executing bulk query for tenant %s with %v timeout", tenantID, timeout)
	queryStart := time.Now()

	rows, err := r.db.WithContext(ctx).Raw(query, tenantID, startTime, endTime).Rows()
	if err != nil {
		queryDuration := time.Since(queryStart)
		log.Printf("[REPO] Bulk query failed after %v: %v", queryDuration, err)

		// Check if it's a timeout error
		if ctx.Err() == context.DeadlineExceeded {
			return nil, fmt.Errorf("bulk query timeout after %v for tenant %s: %w", timeout, tenantID, err)
		}

		return nil, fmt.Errorf("failed to query all reports: %w", err)
	}
	defer rows.Close()

	recordCount := 0
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
		recordCount++

		// Log progress for very large datasets
		if recordCount%10000 == 0 {
			log.Printf("[REPO] Bulk query progress: %d records processed", recordCount)
		}
	}

	queryDuration := time.Since(queryStart)
	log.Printf("[REPO] Bulk query completed in %v: %d records for tenant %s",
		queryDuration, recordCount, tenantID)

	// Warn if query took more than expected time
	if queryDuration > timeout/2 {
		log.Printf("[REPO] WARNING: Bulk query took %v (%.1f%% of timeout %v) - consider optimization",
			queryDuration, float64(queryDuration)/float64(timeout)*100, timeout)
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
