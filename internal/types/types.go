package types

import "time"

const (
	SubjectGenerateReport = "job.sftp.generate.report"
	SubjectUploadSFTP     = "job.sftp.upload.sftp"
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
