package entity

import "time"

const (
	FileType30Min = "30MIN"
	FileTypeDaily = "DAILY"
)

const (
	StatusPending  = "PENDING"
	StatusSuccess  = "SUCCESS"
	StatusFailed   = "FAILED"
	StatusReplaced = "REPLACED"
)

type SFTPTransferLog struct {
	ID                string     `gorm:"type:uuid;primaryKey"`
	TenantID          string     `gorm:"type:varchar(50);not null"`
	LocationID        *string    `gorm:"type:uuid"`
	FileName          string     `gorm:"type:varchar(255);not null"`
	FilePath          string     `gorm:"type:varchar(512);not null"`
	RemotePath        string     `gorm:"type:varchar(512);not null"`
	Status            string     `gorm:"type:varchar(50);not null"`
	FileSize          int64      `gorm:"type:bigint;not null"`
	TransferStartTime time.Time  `gorm:"type:timestamp;not null"`
	TransferEndTime   *time.Time `gorm:"type:timestamp"`
	ErrorMessage      *string    `gorm:"type:text"`
	RecordCount       *int       `gorm:"type:int"`
	FileType          string     `gorm:"type:varchar(50);not null"`
	CreatedAt         time.Time  `gorm:"type:timestamp;default:current_timestamp"`
	Environment       string     `gorm:"type:varchar(50);not null"`
}

func (log *SFTPTransferLog) IsSuccessful() bool {
	return log.Status == StatusSuccess
}

func (log *SFTPTransferLog) IsPending() bool {
	return log.Status == StatusPending
}

func (log *SFTPTransferLog) IsFailed() bool {
	return log.Status == StatusFailed
}

func (log *SFTPTransferLog) IsReplaced() bool {
	return log.Status == StatusReplaced
}

func (log *SFTPTransferLog) IsActive() bool {
	return log.Status == StatusSuccess || log.Status == StatusPending
}
