// ✅ ENHANCED: Add these methods to your SFTPLogRepository interface and implementation

package repository

import (
	"fmt"
	"log"
	"time"

	"jarvist/sftp-service/internal/domain/entity"

	"gorm.io/gorm"
)

type SFTPLogRepository interface {
	Create(log *entity.SFTPTransferLog) error
	Update(log *entity.SFTPTransferLog) error
	GetByID(id string) (*entity.SFTPTransferLog, error)
	GetByFileName(fileName string) (*entity.SFTPTransferLog, error)
	GetPendingUploads(tenantID string) ([]*entity.SFTPTransferLog, error)
	UpdateStatus(id, status string, errorMessage *string) error
	GetRecentByFileName(fileName string, since time.Duration) ([]*entity.SFTPTransferLog, error)

	GetRecentByFilePattern(tenantID, locationID, filePattern string, since time.Duration) ([]*entity.SFTPTransferLog, error)
	GetExistingWindowsForLocation(tenantID, locationID, dateStr string, since time.Duration) ([]*entity.SFTPTransferLog, error)
	BatchUpdateStatus(ids []string, status string, errorMessage *string) error

	GetPendingCount(tenantID string) (int64, error)
	GetSuccessCount(tenantID string) (int64, error)
	GetFailedCount(tenantID string) (int64, error)
}

type sftpLogRepository struct {
	db *gorm.DB
}

func NewSFTPLogRepository(db *gorm.DB) SFTPLogRepository {
	return &sftpLogRepository{db: db}
}

func (r *sftpLogRepository) GetRecentByFilePattern(tenantID, locationID, filePattern string, since time.Duration) ([]*entity.SFTPTransferLog, error) {
	var logs []*entity.SFTPTransferLog
	cutoffTime := time.Now().Add(-since)

	query := r.db.Where("tenant_id = ? AND location_id = ? AND file_name LIKE ? AND created_at > ?",
		tenantID, locationID, filePattern+"%", cutoffTime).
		Order("created_at DESC")

	result := query.Find(&logs)
	if result.Error != nil {
		log.Printf("[REPO] ERROR querying pattern %s: %v", filePattern, result.Error)
		return nil, result.Error
	}

	log.Printf("[REPO] Found %d files matching pattern %s for tenant %s", len(logs), filePattern, tenantID)
	return logs, nil
}

func (r *sftpLogRepository) GetExistingWindowsForLocation(tenantID, locationID, dateStr string, since time.Duration) ([]*entity.SFTPTransferLog, error) {
	var logs []*entity.SFTPTransferLog
	cutoffTime := time.Now().Add(-since)

	pattern := fmt.Sprintf("%%_%s_%%.csv", dateStr)

	query := r.db.Where(`tenant_id = ? AND location_id = ? AND file_name LIKE ?
		AND created_at > ? AND status IN ('SUCCESS', 'PENDING')
		AND file_type = '30MIN'`,
		tenantID, locationID, pattern, cutoffTime).
		Order("file_name ASC")

	result := query.Find(&logs)
	if result.Error != nil {
		log.Printf("[REPO] ERROR querying existing windows for %s: %v", locationID, result.Error)
		return nil, result.Error
	}

	log.Printf("[REPO] Found %d existing 30min windows for location %s on date %s",
		len(logs), locationID, dateStr)
	return logs, nil
}

func (r *sftpLogRepository) BatchUpdateStatus(ids []string, status string, errorMessage *string) error {
	if len(ids) == 0 {
		return nil
	}

	updateData := map[string]interface{}{
		"status":            status,
		"transfer_end_time": time.Now(),
	}

	if errorMessage != nil {
		updateData["error_message"] = *errorMessage
	} else {
		updateData["error_message"] = nil
	}

	result := r.db.Model(&entity.SFTPTransferLog{}).
		Where("id IN ?", ids).
		Updates(updateData)

	if result.Error != nil {
		log.Printf("[REPO] ❌ Batch update failed for %d records: %v", len(ids), result.Error)
		return result.Error
	}

	log.Printf("[REPO] ✅ Batch updated %d records to status %s", result.RowsAffected, status)
	return nil
}

func (r *sftpLogRepository) GetRecentByFileName(fileName string, since time.Duration) ([]*entity.SFTPTransferLog, error) {
	var logs []*entity.SFTPTransferLog
	cutoffTime := time.Now().Add(-since)

	result := r.db.Where("file_name = ? AND created_at > ?", fileName, cutoffTime).
		Order("created_at DESC").
		Limit(10).
		Find(&logs)

	if result.Error != nil {
		log.Printf("[REPO] ERROR querying recent files %s: %v", fileName, result.Error)
		return nil, result.Error
	}

	if len(logs) > 3 {
		log.Printf("[REPO] Found %d recent uploads for %s", len(logs), fileName)
	}

	return logs, nil
}

func (r *sftpLogRepository) Create(sftpLog *entity.SFTPTransferLog) error {
	if err := r.db.Create(sftpLog).Error; err != nil {
		log.Printf("[REPO] Failed to create log entry for %s: %v", sftpLog.FileName, err)
		return err
	}

	return nil
}

func (r *sftpLogRepository) Update(sftpLog *entity.SFTPTransferLog) error {
	if err := r.db.Save(sftpLog).Error; err != nil {
		log.Printf("[REPO] ❌  Failed to update log entry %s: %v", sftpLog.ID, err)
		return err
	}

	log.Printf("[REPO] ✅ Successfully to update log entry %s: %v", sftpLog.ID, sftpLog.Status)
	return nil
}

func (r *sftpLogRepository) GetByID(id string) (*entity.SFTPTransferLog, error) {
	var logEntry entity.SFTPTransferLog
	result := r.db.Where("id = ?", id).First(&logEntry)

	if result.Error != nil {
		if result.Error == gorm.ErrRecordNotFound {
			return nil, nil
		}
		log.Printf("[REPO] ERROR querying ID %s: %v", id, result.Error)
		return nil, result.Error
	}

	return &logEntry, nil
}

func (r *sftpLogRepository) GetByFileName(fileName string) (*entity.SFTPTransferLog, error) {
	var logEntry entity.SFTPTransferLog
	result := r.db.Where("file_name = ?", fileName).
		Order("created_at DESC").
		First(&logEntry)

	if result.Error != nil {
		if result.Error == gorm.ErrRecordNotFound {
			return nil, nil
		}
		log.Printf("[REPO] ERROR querying file %s: %v", fileName, result.Error)
		return nil, result.Error
	}

	return &logEntry, nil
}

func (r *sftpLogRepository) GetPendingUploads(tenantID string) ([]*entity.SFTPTransferLog, error) {
	var logs []*entity.SFTPTransferLog

	query := r.db.Where("tenant_id = ? AND status = ?", tenantID, "PENDING").
		Order("created_at ASC")

	if tenantID == "" {
		query = r.db.Where("status = ?", "PENDING").Order("created_at ASC")
	}

	result := query.Find(&logs)
	if result.Error != nil {
		log.Printf("[REPO] GetPendingUploads failed: %v", result.Error)
		return nil, result.Error
	}

	if len(logs) > 0 {
		log.Printf("[REPO] Found %d pending files for tenant %s", len(logs), tenantID)
	}

	return logs, nil
}

func (r *sftpLogRepository) UpdateStatus(id, status string, errorMessage *string) error {
	updateData := map[string]interface{}{
		"status":            status,
		"transfer_end_time": time.Now(),
	}

	if errorMessage != nil {
		updateData["error_message"] = *errorMessage
	} else {
		updateData["error_message"] = nil
	}

	result := r.db.Model(&entity.SFTPTransferLog{}).
		Where("id = ?", id).
		Updates(updateData)

	if result.Error != nil {
		log.Printf("[REPO] Update failed for ID %s: %v", id, result.Error)
		return result.Error
	}

	if result.RowsAffected == 0 {
		return fmt.Errorf("no rows affected - record not found")
	}

	if status == "FAILED" || status == "REPLACED" {
		log.Printf("[REPO] Updated ID %s to status %s", id, status)
	}
	return nil
}

// Optional monitoring methods
func (r *sftpLogRepository) GetPendingCount(tenantID string) (int64, error) {
	var count int64
	err := r.db.Model(&entity.SFTPTransferLog{}).
		Where("tenant_id = ? AND status = ?", tenantID, "PENDING").
		Count(&count).Error
	return count, err
}

func (r *sftpLogRepository) GetSuccessCount(tenantID string) (int64, error) {
	var count int64
	err := r.db.Model(&entity.SFTPTransferLog{}).
		Where("tenant_id = ? AND status = ?", tenantID, "SUCCESS").
		Count(&count).Error
	return count, err
}

func (r *sftpLogRepository) GetFailedCount(tenantID string) (int64, error) {
	var count int64
	err := r.db.Model(&entity.SFTPTransferLog{}).
		Where("tenant_id = ? AND status = ?", tenantID, "FAILED").
		Count(&count).Error
	return count, err
}
