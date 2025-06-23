package types

import (
	"time"
)

const (
	SubjectGenerateReport = "job.sftp.generate.report"
	SubjectUploadSFTP     = "job.sftp.upload.sftp"
	SubjectLateDataCheck  = "job.sftp.late.data.check"
)

// Job type constants
const (
	JobTypeGenerateReport = "generate_report"
	JobTypeUploadSFTP     = "upload_sftp"
	JobTypeDailyExport    = "daily_export"
	JobType30MinExport    = "30min_export"
	JobTypeRepush         = "repush"
	JobTypeLateDataCheck  = "late_data_check"
)

type GenerateReportJob struct {
	TenantID  string    `json:"tenant_id"`
	Date      time.Time `json:"date"`
	JobType   string    `json:"job_type"` // "daily" or "30min"
	CreatedAt time.Time `json:"created_at"`
}

type UploadSFTPJob struct {
	TenantID   string    `json:"tenant_id"`
	FilePath   string    `json:"file_path"`
	FileName   string    `json:"file_name"`
	RemotePath string    `json:"remote_path"`
	FileType   string    `json:"file_type"`
	LocationID string    `json:"location_id,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
}

// NEW: Late data check job with optimized structure
type LateDataCheckJob struct {
	TenantID  string    `json:"tenant_id"`
	Date      time.Time `json:"date"`
	CheckType string    `json:"check_type"` // "realtime" or "historical"
	CreatedAt time.Time `json:"created_at"`
}

// Job payload untuk NATS messaging
type JobPayload struct {
	ID       string    `json:"id"`
	Type     string    `json:"type"`
	TenantID string    `json:"tenant_id"`
	Data     string    `json:"data"`
	Created  time.Time `json:"created"`
}
