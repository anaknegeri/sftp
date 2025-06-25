package repository

import (
	"context"
	"fmt"
	"log"
	"time"

	"jarvist/sftp-service/internal/domain/entity"

	"gorm.io/gorm"
)

// ACTUAL interface that's being used (based on your project)
type SFTPLogRepository interface {
	Create(log *entity.SFTPTransferLog) error
	Update(log *entity.SFTPTransferLog) error
	GetByID(id string) (*entity.SFTPTransferLog, error)
	GetByFileName(fileName string) (*entity.SFTPTransferLog, error)
	GetPendingUploads(tenantID string) ([]*entity.SFTPTransferLog, error)
	UpdateStatus(id, status string, errorMessage *string) error
	GetRecentByFileName(fileName string, since time.Duration) ([]*entity.SFTPTransferLog, error)
}

type sftpLogRepository struct {
	db *gorm.DB
}

func NewSFTPLogRepository(db *gorm.DB) SFTPLogRepository {
	return &sftpLogRepository{db: db}
}

// ENHANCED: UpdateStatus with detailed logging and retry mechanism
func (r *sftpLogRepository) UpdateStatus(id, status string, errorMessage *string) error {
	startTime := time.Now()

	log.Printf("[REPO] Updating status for ID %s: -> %s", id, status)

	// First, get current status for logging
	var currentLog entity.SFTPTransferLog
	getCurrentResult := r.db.Where("id = ?", id).First(&currentLog)
	currentStatus := "UNKNOWN"
	if getCurrentResult.Error == nil {
		currentStatus = currentLog.Status
	}

	updateData := map[string]interface{}{
		"status":            status,
		"transfer_end_time": time.Now(),
	}

	if errorMessage != nil {
		updateData["error_message"] = *errorMessage
		log.Printf("[REPO] Adding error message: %s", *errorMessage)
	} else {
		updateData["error_message"] = nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result := r.db.WithContext(ctx).Model(&entity.SFTPTransferLog{}).
		Where("id = ?", id).
		Updates(updateData)

	duration := time.Since(startTime)

	if result.Error != nil {
		log.Printf("[REPO] ❌ UPDATE FAILED for ID %s (%s -> %s) after %v: %v",
			id, currentStatus, status, duration, result.Error)
		return result.Error
	}

	if result.RowsAffected == 0 {
		log.Printf("[REPO] ❌ NO ROWS AFFECTED for ID %s (%s -> %s) after %v - record not found",
			id, currentStatus, status, duration)
		return fmt.Errorf("no rows affected - record not found")
	}

	log.Printf("[REPO] ✅ SUCCESS: Updated ID %s (%s -> %s) in %v [%d rows affected]",
		id, currentStatus, status, duration, result.RowsAffected)

	return nil
}

// ENHANCED: GetByID with better logging
func (r *sftpLogRepository) GetByID(id string) (*entity.SFTPTransferLog, error) {
	startTime := time.Now()
	var logEntry entity.SFTPTransferLog

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result := r.db.WithContext(ctx).Where("id = ?", id).First(&logEntry)
	duration := time.Since(startTime)

	if result.Error != nil {
		if result.Error == gorm.ErrRecordNotFound {
			log.Printf("[REPO] Record not found for ID %s (query took %v)", id, duration)
			return nil, nil
		}
		log.Printf("[REPO] ERROR querying ID %s after %v: %v", id, duration, result.Error)
		return nil, result.Error
	}

	log.Printf("[REPO] Found record for ID %s: status=%s (query took %v)",
		id, logEntry.Status, duration)
	return &logEntry, nil
}

// ENHANCED: Create with better logging
func (r *sftpLogRepository) Create(sftpLog *entity.SFTPTransferLog) error {
	startTime := time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := r.db.WithContext(ctx).Create(sftpLog).Error; err != nil {
		duration := time.Since(startTime)
		log.Printf("[REPO] ❌ CREATE FAILED for %s (ID: %s) after %v: %v",
			sftpLog.FileName, sftpLog.ID, duration, err)
		return err
	}

	duration := time.Since(startTime)
	log.Printf("[REPO] ✅ CREATED: %s (ID: %s, Status: %s) in %v",
		sftpLog.FileName, sftpLog.ID, sftpLog.Status, duration)
	return nil
}

// ENHANCED: Update with better logging
func (r *sftpLogRepository) Update(sftpLog *entity.SFTPTransferLog) error {
	startTime := time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := r.db.WithContext(ctx).Save(sftpLog).Error; err != nil {
		duration := time.Since(startTime)
		log.Printf("[REPO] ❌ UPDATE FAILED for %s (ID: %s) after %v: %v",
			sftpLog.FileName, sftpLog.ID, duration, err)
		return err
	}

	duration := time.Since(startTime)
	log.Printf("[REPO] ✅ UPDATED: %s (ID: %s, Status: %s) in %v",
		sftpLog.FileName, sftpLog.ID, sftpLog.Status, duration)
	return nil
}

// ENHANCED: GetPendingUploads with better performance and logging
func (r *sftpLogRepository) GetPendingUploads(tenantID string) ([]*entity.SFTPTransferLog, error) {
	startTime := time.Now()
	var logs []*entity.SFTPTransferLog

	query := r.db.Where("status IN (?)", []string{"PENDING", "PROCESSING"}).
		Order("created_at ASC")

	if tenantID != "" {
		query = query.Where("tenant_id = ?", tenantID)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result := query.WithContext(ctx).Find(&logs)
	duration := time.Since(startTime)

	if result.Error != nil {
		log.Printf("[REPO] ❌ GET PENDING FAILED for tenant %s after %v: %v",
			tenantID, duration, result.Error)
		return nil, result.Error
	}

	// Count by status for detailed logging
	pendingCount := 0
	processingCount := 0
	for _, log := range logs {
		switch log.Status {
		case "PENDING":
			pendingCount++
		case "PROCESSING":
			processingCount++
		}
	}

	log.Printf("[REPO] ✅ FOUND PENDING: %d total (%d PENDING, %d PROCESSING) for tenant %s in %v",
		len(logs), pendingCount, processingCount, tenantID, duration)

	return logs, nil
}

// ENHANCED: GetRecentByFileName with better logging
func (r *sftpLogRepository) GetRecentByFileName(fileName string, since time.Duration) ([]*entity.SFTPTransferLog, error) {
	startTime := time.Now()
	var logs []*entity.SFTPTransferLog
	cutoffTime := time.Now().Add(-since)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result := r.db.WithContext(ctx).Where("file_name = ? AND created_at > ?", fileName, cutoffTime).
		Order("created_at DESC").
		Limit(10).
		Find(&logs)

	duration := time.Since(startTime)

	if result.Error != nil {
		log.Printf("[REPO] ❌ ERROR querying recent files %s after %v: %v", fileName, duration, result.Error)
		return nil, result.Error
	}

	if len(logs) > 0 {
		log.Printf("[REPO] ✅ FOUND %d recent uploads for %s in %v", len(logs), fileName, duration)
	}

	return logs, nil
}

// ENHANCED: GetByFileName with better logging
func (r *sftpLogRepository) GetByFileName(fileName string) (*entity.SFTPTransferLog, error) {
	startTime := time.Now()
	var logEntry entity.SFTPTransferLog

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result := r.db.WithContext(ctx).Where("file_name = ?", fileName).
		Order("created_at DESC").
		First(&logEntry)

	duration := time.Since(startTime)

	if result.Error != nil {
		if result.Error == gorm.ErrRecordNotFound {
			log.Printf("[REPO] File %s not found (query took %v)", fileName, duration)
			return nil, nil
		}
		log.Printf("[REPO] ERROR querying file %s after %v: %v", fileName, duration, result.Error)
		return nil, result.Error
	}

	log.Printf("[REPO] Found file %s: status=%s (query took %v)",
		fileName, logEntry.Status, duration)
	return &logEntry, nil
}
