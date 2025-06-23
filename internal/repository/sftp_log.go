package repository

import (
	"jarvist/sftp-service/internal/domain/entity"
	"time"

	"gorm.io/gorm"
)

type SFTPLogRepository interface {
	Create(log *entity.SFTPTransferLog) error
	Update(log *entity.SFTPTransferLog) error
	GetByFileName(fileName string) (*entity.SFTPTransferLog, error)
	GetPendingUploads(tenantID string) ([]*entity.SFTPTransferLog, error)
	GetPendingUploadsByStatus(status string) ([]*entity.SFTPTransferLog, error)
	UpdateStatus(id, status string, errorMessage *string) error
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
	var log entity.SFTPTransferLog
	result := r.db.Where("file_name = ?", fileName).Order("created_at DESC").First(&log)

	if result.Error != nil {
		if result.Error == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, result.Error
	}

	return &log, nil
}

func (r *sftpLogRepository) GetPendingUploads(tenantID string) ([]*entity.SFTPTransferLog, error) {
	var logs []*entity.SFTPTransferLog

	query := r.db.Where("tenant_id = ? AND status = ?", tenantID, "PENDING").
		Order("created_at ASC") // Process older files first

	if tenantID == "" {
		query = r.db.Where("status = ?", "PENDING").Order("created_at ASC")
	}

	result := query.Find(&logs)
	if result.Error != nil {
		return nil, result.Error
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

	return result.Error
}
