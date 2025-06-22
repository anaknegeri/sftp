package job

import (
	"encoding/json"
	"fmt"
	"jarvist/sftp/internal/queue"
	"jarvist/sftp/internal/service"
	"jarvist/sftp/internal/types"
	"log"
)

type JobProcessor struct {
	queue         queue.JobQueue
	exportService service.ExportService
	sftpService   service.SFTPService
}

func NewJobProcessor(
	queue queue.JobQueue,
	exportService service.ExportService,
	sftpService service.SFTPService,
) *JobProcessor {
	return &JobProcessor{
		queue:         queue,
		exportService: exportService,
		sftpService:   sftpService,
	}
}

func (p *JobProcessor) Start() error {
	log.Println("[JOB] Starting job processor...")

	// Subscribe to generate report jobs
	if err := p.queue.SubscribeJob(types.SubjectGenerateReport, p.handleGenerateReport); err != nil {
		return fmt.Errorf("failed to subscribe to generate report jobs: %w", err)
	}

	// Subscribe to SFTP upload jobs
	if err := p.queue.SubscribeJob(types.SubjectUploadSFTP, p.handleUploadSFTP); err != nil {
		return fmt.Errorf("failed to subscribe to SFTP upload jobs: %w", err)
	}

	log.Println("[JOB] Job processor started successfully")
	return nil
}

func (p *JobProcessor) handleGenerateReport(data []byte) error {
	var job types.GenerateReportJob
	if err := json.Unmarshal(data, &job); err != nil {
		return fmt.Errorf("failed to unmarshal generate report job: %w", err)
	}

	log.Printf("[JOB] Processing generate report job: tenant=%s, type=%s, date=%s",
		job.TenantID, job.JobType, job.Date.Format("2006-01-02"))

	switch job.JobType {
	case "daily":
		return p.exportService.ExportDaily(job.TenantID, job.Date)
	case "30min":
		return p.exportService.Export30Min(job.TenantID, job.Date)
	default:
		return fmt.Errorf("unknown job type: %s", job.JobType)
	}
}

func (p *JobProcessor) handleUploadSFTP(data []byte) error {
	var job types.UploadSFTPJob
	if err := json.Unmarshal(data, &job); err != nil {
		return fmt.Errorf("failed to unmarshal SFTP upload job: %w", err)
	}

	log.Printf("[JOB] Processing SFTP upload job: tenant=%s, file=%s",
		job.TenantID, job.FileName)

	return p.sftpService.UploadFile(job)
}
