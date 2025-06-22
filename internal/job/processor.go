package job

import (
	"encoding/json"
	"fmt"
	"jarvist/sftp/internal/queue"
	"jarvist/sftp/internal/service"
	"jarvist/sftp/internal/types"
	"log"
	"time"
)

type JobProcessor struct {
	queue           queue.JobQueue
	exportService   service.ExportService
	sftpService     service.SFTPService
	lateDataService service.LateDataService // NEW: Added late data service
}

func NewJobProcessor(
	queue queue.JobQueue,
	exportService service.ExportService,
	sftpService service.SFTPService,
	lateDataService service.LateDataService, // NEW: Added parameter
) *JobProcessor {
	// Set SFTP service for late data service
	lateDataService.SetSFTPService(sftpService)

	return &JobProcessor{
		queue:           queue,
		exportService:   exportService,
		sftpService:     sftpService,
		lateDataService: lateDataService, // NEW
	}
}

func (p *JobProcessor) Start() error {
	log.Println("[JOB] Starting job processor...")

	// Subscribe to generate report jobs
	log.Println("[JOB] Subscribing to generate report jobs...")
	if err := p.queue.SubscribeJob(types.SubjectGenerateReport, p.handleGenerateReport); err != nil {
		return fmt.Errorf("failed to subscribe to generate report jobs: %w", err)
	}

	// Subscribe to SFTP upload jobs
	log.Println("[JOB] Subscribing to SFTP upload jobs...")
	if err := p.queue.SubscribeJob(types.SubjectUploadSFTP, p.handleUploadSFTP); err != nil {
		return fmt.Errorf("failed to subscribe to SFTP upload jobs: %w", err)
	}

	// NEW: Subscribe to late data check jobs
	log.Println("[JOB] Subscribing to late data check jobs...")
	if err := p.queue.SubscribeJob(types.SubjectLateDataCheck, p.handleLateDataCheck); err != nil {
		return fmt.Errorf("failed to subscribe to late data check jobs: %w", err)
	}

	log.Println("[JOB] Job processor started successfully")
	log.Println("[JOB] Waiting for jobs...")
	return nil
}

func (p *JobProcessor) handleGenerateReport(data []byte) error {
	log.Printf("[JOB] Received generate report job (size: %d bytes)", len(data))

	var job types.GenerateReportJob
	if err := json.Unmarshal(data, &job); err != nil {
		log.Printf("[JOB] Failed to unmarshal generate report job: %v", err)
		log.Printf("[JOB] Raw data: %s", string(data))
		return fmt.Errorf("failed to unmarshal generate report job: %w", err)
	}

	log.Printf("[JOB] Processing generate report job: tenant=%s, type=%s, date=%s",
		job.TenantID, job.JobType, job.Date.Format("2006-01-02"))

	startTime := time.Now()

	var err error
	switch job.JobType {
	case "daily":
		err = p.exportService.ExportDaily(job.TenantID, job.Date)
	case "30min":
		err = p.exportService.Export30Min(job.TenantID, job.Date)
	default:
		err = fmt.Errorf("unknown job type: %s", job.JobType)
	}

	duration := time.Since(startTime)

	if err != nil {
		log.Printf("[JOB] Generate report job failed after %v: %v", duration, err)
		return err
	}

	log.Printf("[JOB] Generate report job completed successfully in %v", duration)
	return nil
}

func (p *JobProcessor) handleUploadSFTP(data []byte) error {
	log.Printf("[JOB] Received SFTP upload job (size: %d bytes)", len(data))

	var job types.UploadSFTPJob
	if err := json.Unmarshal(data, &job); err != nil {
		log.Printf("[JOB] Failed to unmarshal SFTP upload job: %v", err)
		log.Printf("[JOB] Raw data: %s", string(data))
		return fmt.Errorf("failed to unmarshal SFTP upload job: %w", err)
	}

	log.Printf("[JOB] Processing SFTP upload job: tenant=%s, file=%s, remote_path=%s",
		job.TenantID, job.FileName, job.RemotePath)

	startTime := time.Now()

	err := p.sftpService.UploadFile(job)

	duration := time.Since(startTime)

	if err != nil {
		log.Printf("[JOB] SFTP upload job failed after %v: %v", duration, err)
		return err
	}

	log.Printf("[JOB] SFTP upload job completed successfully in %v", duration)
	return nil
}

// NEW: Handle late data check jobs
func (p *JobProcessor) handleLateDataCheck(data []byte) error {
	log.Printf("[JOB] Received late data check job (size: %d bytes)", len(data))

	var job types.LateDataCheckJob
	if err := json.Unmarshal(data, &job); err != nil {
		log.Printf("[JOB] Failed to unmarshal late data check job: %v", err)
		log.Printf("[JOB] Raw data: %s", string(data))
		return fmt.Errorf("failed to unmarshal late data check job: %w", err)
	}

	log.Printf("[JOB] Processing late data check job: tenant=%s, type=%s, date=%s",
		job.TenantID, job.CheckType, job.Date.Format("2006-01-02"))

	startTime := time.Now()

	err := p.lateDataService.CheckForLateData(job.TenantID, job.Date, job.CheckType)

	duration := time.Since(startTime)

	if err != nil {
		log.Printf("[JOB] Late data check job (%s) failed after %v: %v", job.CheckType, duration, err)
		return err
	}

	log.Printf("[JOB] Late data check job (%s) completed successfully in %v", job.CheckType, duration)
	return nil
}
