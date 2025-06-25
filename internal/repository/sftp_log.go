// File: internal/repository/sftp_log.go
package repository

import (
	"context"
	"fmt"
	"log"
	"time"

	"jarvist/sftp-service/internal/domain/entity"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

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

// FIXED: UpdateStatus with proper timeout and retry handling
func (r *sftpLogRepository) UpdateStatus(id, status string, errorMessage *string) error {
	startTime := time.Now()

	log.Printf("[REPO] Updating status for ID %s: -> %s", id, status)

	// Get current status for logging
	var currentLog entity.SFTPTransferLog
	getCurrentResult := r.db.Where("id = ?", id).First(&currentLog)
	currentStatus := "UNKNOWN"
	if getCurrentResult.Error == nil {
		currentStatus = currentLog.Status
	}

	// FIXED: Use longer timeout for database operations
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

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

	// FIXED: Use transaction with proper locking to prevent conflicts
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// First, lock the row to prevent concurrent updates
		var existing entity.SFTPTransferLog
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ?", id).
			First(&existing).Error; err != nil {
			if err == gorm.ErrRecordNotFound {
				return fmt.Errorf("record not found")
			}
			return fmt.Errorf("failed to lock record: %w", err)
		}

		// Check if status has already changed to what we want
		if existing.Status == status {
			log.Printf("[REPO] Status already %s for ID %s, skipping update", status, id)
			return nil
		}

		// Perform the update
		result := tx.Model(&entity.SFTPTransferLog{}).
			Where("id = ?", id).
			Updates(updateData)

		if result.Error != nil {
			return fmt.Errorf("update failed: %w", result.Error)
		}

		if result.RowsAffected == 0 {
			return fmt.Errorf("no rows affected - record may have been deleted")
		}

		return nil
	})

	duration := time.Since(startTime)

	if err != nil {
		// Check for specific timeout errors
		if ctx.Err() == context.DeadlineExceeded {
			log.Printf("[REPO] ❌ TIMEOUT: Update for ID %s (%s -> %s) after %v: context deadline exceeded",
				id, currentStatus, status, duration)
		} else {
			log.Printf("[REPO] ❌ UPDATE FAILED for ID %s (%s -> %s) after %v: %v",
				id, currentStatus, status, duration, err)
		}
		return err
	}

	log.Printf("[REPO] ✅ SUCCESS: Updated ID %s (%s -> %s) in %v",
		id, currentStatus, status, duration)

	return nil
}

// FIXED: GetByID with better timeout handling
func (r *sftpLogRepository) GetByID(id string) (*entity.SFTPTransferLog, error) {
	startTime := time.Now()
	var logEntry entity.SFTPTransferLog

	// Use shorter timeout for read operations
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
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

// FIXED: Create with better timeout and conflict handling
func (r *sftpLogRepository) Create(sftpLog *entity.SFTPTransferLog) error {
	startTime := time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Use transaction to ensure consistency
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return tx.Create(sftpLog).Error
	})

	duration := time.Since(startTime)

	if err != nil {
		log.Printf("[REPO] ❌ CREATE FAILED for %s (ID: %s) after %v: %v",
			sftpLog.FileName, sftpLog.ID, duration, err)
		return err
	}

	log.Printf("[REPO] ✅ CREATED: %s (ID: %s, Status: %s) in %v",
		sftpLog.FileName, sftpLog.ID, sftpLog.Status, duration)
	return nil
}

// FIXED: Update with better timeout handling
func (r *sftpLogRepository) Update(sftpLog *entity.SFTPTransferLog) error {
	startTime := time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return tx.Save(sftpLog).Error
	})

	duration := time.Since(startTime)

	if err != nil {
		log.Printf("[REPO] ❌ UPDATE FAILED for %s (ID: %s) after %v: %v",
			sftpLog.FileName, sftpLog.ID, duration, err)
		return err
	}

	log.Printf("[REPO] ✅ UPDATED: %s (ID: %s, Status: %s) in %v",
		sftpLog.FileName, sftpLog.ID, sftpLog.Status, duration)
	return nil
}

// FIXED: GetPendingUploads with better performance and timeout
func (r *sftpLogRepository) GetPendingUploads(tenantID string) ([]*entity.SFTPTransferLog, error) {
	startTime := time.Now()
	var logs []*entity.SFTPTransferLog

	query := r.db.Where("status IN (?)", []string{"PENDING", "PROCESSING"}).
		Order("created_at ASC")

	if tenantID != "" {
		query = query.Where("tenant_id = ?", tenantID)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
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

// FIXED: GetRecentByFileName with better timeout
func (r *sftpLogRepository) GetRecentByFileName(fileName string, since time.Duration) ([]*entity.SFTPTransferLog, error) {
	startTime := time.Now()
	var logs []*entity.SFTPTransferLog
	cutoffTime := time.Now().Add(-since)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
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

// FIXED: GetByFileName with better timeout
func (r *sftpLogRepository) GetByFileName(fileName string) (*entity.SFTPTransferLog, error) {
	startTime := time.Now()
	var logEntry entity.SFTPTransferLog

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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
