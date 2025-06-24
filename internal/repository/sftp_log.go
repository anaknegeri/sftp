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
	GetByFileName(fileName string) (*entity.SFTPTransferLog, error)
	GetPendingUploads(tenantID string) ([]*entity.SFTPTransferLog, error)
	GetPendingUploadsByStatus(status string) ([]*entity.SFTPTransferLog, error)
	UpdateStatus(id, status string, errorMessage *string) error
	// New methods untuk fix
	UpdateStatusWithRetry(id, status string, errorMessage *string, maxRetries int) error
	GetStuckPendingFiles(tenantID string) ([]*entity.SFTPTransferLog, error)
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

func (r *sftpLogRepository) Create(log *entity.SFTPTransferLog) error {
	return r.db.Create(log).Error
}

func (r *sftpLogRepository) Update(log *entity.SFTPTransferLog) error {
	return r.db.Save(log).Error
}

func (r *sftpLogRepository) GetByFileName(fileName string) (*entity.SFTPTransferLog, error) {
	log.Printf("[REPO] Looking for file: %s", fileName)

	var logEntry entity.SFTPTransferLog
	result := r.db.Where("file_name = ?", fileName).
		Order("created_at DESC").
		First(&logEntry)

	if result.Error != nil {
		if result.Error == gorm.ErrRecordNotFound {
			log.Printf("[REPO] No record found for file: %s", fileName)
			return nil, nil
		}
		log.Printf("[REPO] ERROR querying file %s: %v", fileName, result.Error)
		return nil, result.Error
	}

	log.Printf("[REPO] Found record for %s: ID=%s, Status=%s, Created=%v",
		fileName, logEntry.ID, logEntry.Status, logEntry.CreatedAt)
	return &logEntry, nil
}

func (r *sftpLogRepository) GetPendingUploads(tenantID string) ([]*entity.SFTPTransferLog, error) {
	log.Printf("[REPO] GetPendingUploads called for tenant: %s", tenantID)

	var logs []*entity.SFTPTransferLog

	query := r.db.Where("tenant_id = ? AND status = ?", tenantID, "PENDING").
		Order("created_at ASC")

	if tenantID == "" {
		query = r.db.Where("status = ?", "PENDING").Order("created_at ASC")
	}

	result := query.Find(&logs)
	if result.Error != nil {
		log.Printf("[REPO] ❌ GetPendingUploads failed: %v", result.Error)
		return nil, result.Error
	}

	log.Printf("[REPO] ✅ GetPendingUploads found %d pending files for tenant %s", len(logs), tenantID)

	// Debug: log sample files
	if len(logs) > 0 {
		log.Printf("[REPO] Sample pending files:")
		for i, logEntry := range logs {
			if i >= 5 {
				break
			}
			log.Printf("[REPO] - %s (created: %v, status: %s)",
				logEntry.FileName, logEntry.CreatedAt, logEntry.Status)
		}
	}

	return logs, nil
}

func (r *sftpLogRepository) GetPendingUploadsByStatus(status string) ([]*entity.SFTPTransferLog, error) {
	var logs []*entity.SFTPTransferLog

	result := r.db.Where("status = ?", status).
		Order("created_at ASC").
		Find(&logs)

	if result.Error != nil {
		return nil, result.Error
	}

	return logs, nil
}

func (r *sftpLogRepository) UpdateStatus(id, status string, errorMessage *string) error {
	log.Printf("[REPO] UpdateStatus called for ID %s to status %s", id, status)

	// Check if record exists
	var existingLog entity.SFTPTransferLog
	if err := r.db.Where("id = ?", id).First(&existingLog).Error; err != nil {
		log.Printf("[REPO] ❌ Record not found for ID %s: %v", id, err)
		return fmt.Errorf("record not found: %w", err)
	}

	log.Printf("[REPO] Found record: FileName=%s, CurrentStatus=%s",
		existingLog.FileName, existingLog.Status)

	updateData := map[string]interface{}{
		"status":            status,
		"transfer_end_time": time.Now(),
	}

	if errorMessage != nil {
		updateData["error_message"] = *errorMessage
		log.Printf("[REPO] Setting error_message: %s", *errorMessage)
	} else {
		updateData["error_message"] = nil
		log.Printf("[REPO] Clearing error_message")
	}

	result := r.db.Model(&entity.SFTPTransferLog{}).
		Where("id = ?", id).
		Updates(updateData)

	log.Printf("[REPO] Update result: RowsAffected=%d, Error=%v", result.RowsAffected, result.Error)

	if result.Error != nil {
		log.Printf("[REPO] ❌ Update failed: %v", result.Error)
		return result.Error
	}

	if result.RowsAffected == 0 {
		log.Printf("[REPO] ❌ No rows affected for ID %s", id)
		return fmt.Errorf("no rows affected")
	}

	log.Printf("[REPO] ✅ Successfully updated ID %s to status %s", id, status)
	return nil
}

// NEW: UpdateStatusWithRetry - update status dengan retry mechanism
func (r *sftpLogRepository) UpdateStatusWithRetry(id, status string, errorMessage *string, maxRetries int) error {
	log.Printf("[REPO] UpdateStatusWithRetry called for ID %s to status %s (max retries: %d)", id, status, maxRetries)

	for attempt := 1; attempt <= maxRetries; attempt++ {
		// Start transaction
		tx := r.db.Begin()
		if tx.Error != nil {
			log.Printf("[REPO] ❌ Failed to start transaction (attempt %d): %v", attempt, tx.Error)
			if attempt < maxRetries {
				time.Sleep(time.Duration(attempt) * 200 * time.Millisecond)
				continue
			}
			return tx.Error
		}

		// Check if record exists
		var existingLog entity.SFTPTransferLog
		if err := tx.Where("id = ?", id).First(&existingLog).Error; err != nil {
			tx.Rollback()
			log.Printf("[REPO] ❌ Record not found for ID %s (attempt %d): %v", id, attempt, err)
			if attempt < maxRetries {
				time.Sleep(time.Duration(attempt) * 200 * time.Millisecond)
				continue
			}
			return fmt.Errorf("record not found: %w", err)
		}

		log.Printf("[REPO] Found record (attempt %d): FileName=%s, CurrentStatus=%s",
			attempt, existingLog.FileName, existingLog.Status)

		updateData := map[string]interface{}{
			"status":            status,
			"transfer_end_time": time.Now(),
		}

		if errorMessage != nil {
			updateData["error_message"] = *errorMessage
		} else {
			updateData["error_message"] = nil
		}

		// Perform update
		result := tx.Model(&entity.SFTPTransferLog{}).
			Where("id = ?", id).
			Updates(updateData)

		if result.Error != nil {
			tx.Rollback()
			log.Printf("[REPO] ❌ Update failed (attempt %d): %v", attempt, result.Error)
			if attempt < maxRetries {
				time.Sleep(time.Duration(attempt) * 200 * time.Millisecond)
				continue
			}
			return result.Error
		}

		if result.RowsAffected == 0 {
			tx.Rollback()
			log.Printf("[REPO] ❌ No rows affected for ID %s (attempt %d)", id, attempt)
			if attempt < maxRetries {
				time.Sleep(time.Duration(attempt) * 200 * time.Millisecond)
				continue
			}
			return fmt.Errorf("no rows affected")
		}

		// Commit transaction
		if err := tx.Commit().Error; err != nil {
			log.Printf("[REPO] ❌ Failed to commit transaction (attempt %d): %v", attempt, err)
			if attempt < maxRetries {
				time.Sleep(time.Duration(attempt) * 200 * time.Millisecond)
				continue
			}
			return err
		}

		log.Printf("[REPO] ✅ Successfully updated ID %s to status %s (attempt %d, affected: %d rows)",
			id, status, attempt, result.RowsAffected)
		return nil
	}

	return fmt.Errorf("failed to update status after %d attempts", maxRetries)
}

// NEW: GetStuckPendingFiles - get files yang pernah diproses tapi masih PENDING
func (r *sftpLogRepository) GetStuckPendingFiles(tenantID string) ([]*entity.SFTPTransferLog, error) {
	log.Printf("[REPO] GetStuckPendingFiles called for tenant: %s", tenantID)

	var logs []*entity.SFTPTransferLog

	// Files yang transfer_start_time != created_at tapi status masih PENDING
	result := r.db.Where(
		"tenant_id = ? AND status = ? AND transfer_start_time != created_at",
		tenantID, "PENDING").
		Order("transfer_start_time ASC").
		Find(&logs)

	if result.Error != nil {
		log.Printf("[REPO] ❌ GetStuckPendingFiles failed: %v", result.Error)
		return nil, result.Error
	}

	log.Printf("[REPO] ✅ Found %d stuck PENDING files for tenant %s", len(logs), tenantID)

	// Debug: log sample stuck files
	if len(logs) > 0 {
		log.Printf("[REPO] Sample stuck PENDING files:")
		for i, logEntry := range logs {
			if i >= 3 {
				break
			}
			processingTime := logEntry.TransferStartTime.Sub(logEntry.CreatedAt)
			log.Printf("[REPO] - %s (created: %v, started: %v, processing_delay: %v)",
				logEntry.FileName, logEntry.CreatedAt, logEntry.TransferStartTime, processingTime)
		}
	}

	return logs, nil
}

// NEW: GetPendingCount - count PENDING files
func (r *sftpLogRepository) GetPendingCount(tenantID string) (int64, error) {
	var count int64
	err := r.db.Model(&entity.SFTPTransferLog{}).
		Where("tenant_id = ? AND status = ?", tenantID, "PENDING").
		Count(&count).Error
	return count, err
}

// NEW: GetSuccessCount - count SUCCESS files
func (r *sftpLogRepository) GetSuccessCount(tenantID string) (int64, error) {
	var count int64
	err := r.db.Model(&entity.SFTPTransferLog{}).
		Where("tenant_id = ? AND status = ?", tenantID, "SUCCESS").
		Count(&count).Error
	return count, err
}

// NEW: GetFailedCount - count FAILED files
func (r *sftpLogRepository) GetFailedCount(tenantID string) (int64, error) {
	var count int64
	err := r.db.Model(&entity.SFTPTransferLog{}).
		Where("tenant_id = ? AND status = ?", tenantID, "FAILED").
		Count(&count).Error
	return count, err
}
