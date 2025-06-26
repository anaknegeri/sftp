package repository

import (
	"context"
	"fmt"
	"log"
	"time"

	"jarvist/sftp-service/internal/config"
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

func (r *sftpLogRepository) UpdateStatus(id, status string, errorMessage *string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	environment := config.GetEnvironment()

	updateData := map[string]interface{}{
		"status":            status,
		"transfer_end_time": time.Now(),
	}

	if errorMessage != nil {
		updateData["error_message"] = *errorMessage
	} else {
		updateData["error_message"] = nil
	}

	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var existing entity.SFTPTransferLog
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ? AND environment = ?", id, environment).
			First(&existing).Error; err != nil {
			if err == gorm.ErrRecordNotFound {
				return fmt.Errorf("record not found")
			}
			return fmt.Errorf("failed to lock record: %w", err)
		}

		if existing.Status == status {
			return nil // Already updated
		}

		result := tx.Model(&entity.SFTPTransferLog{}).
			Where("id = ? AND environment = ?", id, environment).
			Updates(updateData)

		if result.Error != nil {
			return fmt.Errorf("update failed: %w", result.Error)
		}

		if result.RowsAffected == 0 {
			return fmt.Errorf("no rows affected")
		}

		return nil
	})

	if err != nil {
		log.Printf("[REPO] UpdateStatus failed: %s -> %s: %v", id, status, err)
		return err
	}

	return nil
}

func (r *sftpLogRepository) GetByID(id string) (*entity.SFTPTransferLog, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var logEntry entity.SFTPTransferLog
	environment := config.GetEnvironment()

	result := r.db.WithContext(ctx).Where("id = ? AND environment = ?", id, environment).First(&logEntry)
	if result.Error != nil {
		if result.Error == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, result.Error
	}

	return &logEntry, nil
}

func (r *sftpLogRepository) Create(sftpLog *entity.SFTPTransferLog) error {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Auto-set environment
	if sftpLog.Environment == "" {
		sftpLog.Environment = config.GetEnvironment()
	}

	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return tx.Create(sftpLog).Error
	})

	if err != nil {
		log.Printf("[REPO] Create failed: %s: %v", sftpLog.FileName, err)
		return err
	}

	return nil
}

func (r *sftpLogRepository) Update(sftpLog *entity.SFTPTransferLog) error {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	environment := config.GetEnvironment()
	if sftpLog.Environment == "" {
		sftpLog.Environment = environment
	}

	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		result := tx.Model(&entity.SFTPTransferLog{}).
			Where("id = ? AND environment = ?", sftpLog.ID, environment).
			Updates(sftpLog)

		if result.RowsAffected == 0 {
			return fmt.Errorf("record not found in environment")
		}

		return result.Error
	})

	if err != nil {
		log.Printf("[REPO] Update failed: %s: %v", sftpLog.FileName, err)
		return err
	}

	return nil
}

func (r *sftpLogRepository) GetPendingUploads(tenantID string) ([]*entity.SFTPTransferLog, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	var logs []*entity.SFTPTransferLog
	environment := config.GetEnvironment()

	query := r.db.Where("status IN (?) AND environment = ?", []string{"PENDING", "PROCESSING"}, environment).
		Order("created_at ASC")

	if tenantID != "" {
		query = query.Where("tenant_id = ?", tenantID)
	}

	result := query.WithContext(ctx).Find(&logs)
	if result.Error != nil {
		log.Printf("[REPO] GetPendingUploads failed: %v", result.Error)
		return nil, result.Error
	}

	return logs, nil
}

func (r *sftpLogRepository) GetRecentByFileName(fileName string, since time.Duration) ([]*entity.SFTPTransferLog, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var logs []*entity.SFTPTransferLog
	cutoffTime := time.Now().Add(-since)
	environment := config.GetEnvironment()

	result := r.db.WithContext(ctx).
		Where("file_name = ? AND created_at > ? AND environment = ?", fileName, cutoffTime, environment).
		Order("created_at DESC").
		Limit(10).
		Find(&logs)

	if result.Error != nil {
		return nil, result.Error
	}

	return logs, nil
}

func (r *sftpLogRepository) GetByFileName(fileName string) (*entity.SFTPTransferLog, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var logEntry entity.SFTPTransferLog
	environment := config.GetEnvironment()

	result := r.db.WithContext(ctx).
		Where("file_name = ? AND environment = ?", fileName, environment).
		Order("created_at DESC").
		First(&logEntry)

	if result.Error != nil {
		if result.Error == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, result.Error
	}

	return &logEntry, nil
}
