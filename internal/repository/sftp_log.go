// File: internal/repository/sftp_log.go - Simplified version
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

	// Optional methods for admin/monitoring
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

func (r *sftpLogRepository) Create(sftpLog *entity.SFTPTransferLog) error {
	if err := r.db.Create(sftpLog).Error; err != nil {
		log.Printf("[REPO] Failed to create log entry for %s: %v", sftpLog.FileName, err)
		return err
	}

	log.Printf("[REPO] ✅ Created log entry: %s (ID: %s)", sftpLog.FileName, sftpLog.ID)
	return nil
}

func (r *sftpLogRepository) Update(sftpLog *entity.SFTPTransferLog) error {
	if err := r.db.Save(sftpLog).Error; err != nil {
		log.Printf("[REPO] Failed to update log entry %s: %v", sftpLog.ID, err)
		return err
	}

	log.Printf("[REPO] ✅ Updated log entry: %s (ID: %s)", sftpLog.FileName, sftpLog.ID)
	return nil
}

// SIMPLIFIED: GetByID for direct log access using LogID
func (r *sftpLogRepository) GetByID(id string) (*entity.SFTPTransferLog, error) {
	log.Printf("[REPO] Looking for log by ID: %s", id)

	var logEntry entity.SFTPTransferLog
	result := r.db.Where("id = ?", id).First(&logEntry)

	if result.Error != nil {
		if result.Error == gorm.ErrRecordNotFound {
			log.Printf("[REPO] No record found for ID: %s", id)
			return nil, nil
		}
		log.Printf("[REPO] ERROR querying ID %s: %v", id, result.Error)
		return nil, result.Error
	}

	log.Printf("[REPO] Found record for ID %s: FileName=%s, Status=%s",
		id, logEntry.FileName, logEntry.Status)
	return &logEntry, nil
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

	if len(logs) > 0 && len(logs) <= 5 {
		log.Printf("[REPO] Sample pending files:")
		for _, logEntry := range logs {
			log.Printf("[REPO] - %s (ID: %s, created: %v)",
				logEntry.FileName, logEntry.ID, logEntry.CreatedAt)
		}
	}

	return logs, nil
}

// SIMPLIFIED: UpdateStatus using ID directly
func (r *sftpLogRepository) UpdateStatus(id, status string, errorMessage *string) error {
	log.Printf("[REPO] UpdateStatus called for ID %s to status %s", id, status)

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

	if result.Error != nil {
		log.Printf("[REPO] ❌ Update failed for ID %s: %v", id, result.Error)
		return result.Error
	}

	if result.RowsAffected == 0 {
		log.Printf("[REPO] ❌ No rows affected for ID %s - record may not exist", id)
		return fmt.Errorf("no rows affected - record not found")
	}

	log.Printf("[REPO] ✅ Successfully updated ID %s to status %s (affected: %d rows)",
		id, status, result.RowsAffected)
	return nil
}

// OPTIONAL: Admin/monitoring methods
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

func (r *sftpLogRepository) GetRecentByFileName(fileName string, since time.Duration) ([]*entity.SFTPTransferLog, error) {
	log.Printf("[REPO] Looking for recent uploads of file: %s (within %v)", fileName, since)

	var logs []*entity.SFTPTransferLog
	cutoffTime := time.Now().Add(-since)

	result := r.db.Where("file_name = ? AND created_at > ?", fileName, cutoffTime).
		Order("created_at DESC").
		Find(&logs)

	if result.Error != nil {
		log.Printf("[REPO] ERROR querying recent files %s: %v", fileName, result.Error)
		return nil, result.Error
	}

	log.Printf("[REPO] Found %d recent uploads for %s", len(logs), fileName)

	if len(logs) > 0 {
		for i, logEntry := range logs {
			if i >= 3 {
				break
			}
			timeDiff := time.Since(logEntry.CreatedAt)
			log.Printf("[REPO] - %s: size=%d, records=%v, status=%s (%.1f seconds ago)",
				logEntry.ID, logEntry.FileSize,
				func() int {
					if logEntry.RecordCount != nil {
						return *logEntry.RecordCount
					} else {
						return 0
					}
				}(),
				logEntry.Status, timeDiff.Seconds())
		}
	}

	return logs, nil
}
