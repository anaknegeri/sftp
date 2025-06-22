package repository

import (
	"jarvist/sftp/internal/domain/entity"

	"gorm.io/gorm"
)

type SFTPLogRepository interface {
	Create(log *entity.SFTPTransferLog) error
	Update(log *entity.SFTPTransferLog) error
	GetByFileName(fileName string) (*entity.SFTPTransferLog, error)
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
