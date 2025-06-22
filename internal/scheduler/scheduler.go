package scheduler

import (
	"jarvist/sftp/internal/config"
	"jarvist/sftp/internal/queue"
	"jarvist/sftp/internal/service"
	"jarvist/sftp/internal/types"
	"jarvist/sftp/pkg/utils"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/robfig/cron/v3"
)

type Scheduler struct {
	cron        *cron.Cron
	queue       queue.JobQueue
	sftpService service.SFTPService
	localPath   string
}

func NewScheduler(queue queue.JobQueue, sftpService service.SFTPService, localPath string) *Scheduler {
	return &Scheduler{
		cron:        cron.New(cron.WithSeconds()),
		queue:       queue,
		sftpService: sftpService,
		localPath:   localPath,
	}
}

func (s *Scheduler) Start() error {
	log.Println("[SCHEDULER] Starting scheduler...")

	// Job untuk generate report 30 menit - jalan setiap menit 00 dan 30
	_, err := s.cron.AddFunc("0 0,30 * * * *", s.schedule30MinReportJob)
	if err != nil {
		return err
	}

	// Job untuk generate report harian - jalan setiap jam 1 malam
	_, err = s.cron.AddFunc("0 0 1 * * *", s.scheduleDailyReportJob)
	if err != nil {
		return err
	}

	// Job untuk upload SFTP - jalan setiap 5 menit setelah generate report
	_, err = s.cron.AddFunc("0 5,35 * * * *", s.schedule30MinUploadJob)
	if err != nil {
		return err
	}

	// Job untuk upload SFTP harian - jalan setiap jam 1:10 malam
	_, err = s.cron.AddFunc("0 10 1 * * *", s.scheduleDailyUploadJob)
	if err != nil {
		return err
	}

	// Job untuk cleanup old files - jalan setiap hari jam 2 malam
	_, err = s.cron.AddFunc("0 0 2 * * *", s.scheduleCleanupJob)
	if err != nil {
		return err
	}

	s.cron.Start()
	log.Println("[SCHEDULER] Scheduler started successfully")
	return nil
}

func (s *Scheduler) Stop() {
	log.Println("[SCHEDULER] Stopping scheduler...")
	s.cron.Stop()
	log.Println("[SCHEDULER] Scheduler stopped")
}

func (s *Scheduler) schedule30MinReportJob() {
	log.Println("[SCHEDULER] Scheduling 30-minute report generation jobs")

	tenants := config.GetEnabledTenants()
	now := config.NowJakarta()

	for tenantID := range tenants {
		job := types.GenerateReportJob{
			TenantID:  tenantID,
			Date:      now,
			JobType:   "30min",
			CreatedAt: now,
		}

		if err := s.queue.PublishJob(types.SubjectGenerateReport, job); err != nil {
			log.Printf("[SCHEDULER] Failed to publish 30min report job for tenant %s: %v", tenantID, err)
		}
	}
}

func (s *Scheduler) scheduleDailyReportJob() {
	log.Println("[SCHEDULER] Scheduling daily report generation jobs")

	tenants := config.GetEnabledTenants()
	now := config.NowJakarta()
	yesterday := now.AddDate(0, 0, -1) // Generate report untuk kemarin

	for tenantID := range tenants {
		job := types.GenerateReportJob{
			TenantID:  tenantID,
			Date:      yesterday,
			JobType:   "daily",
			CreatedAt: now,
		}

		if err := s.queue.PublishJob(types.SubjectGenerateReport, job); err != nil {
			log.Printf("[SCHEDULER] Failed to publish daily report job for tenant %s: %v", tenantID, err)
		}
	}
}

func (s *Scheduler) schedule30MinUploadJob() {
	log.Println("[SCHEDULER] Scheduling 30-minute SFTP upload jobs")
	s.scheduleUploadJobsForAllTenants("30min")
}

func (s *Scheduler) scheduleDailyUploadJob() {
	log.Println("[SCHEDULER] Scheduling daily SFTP upload jobs")
	s.scheduleUploadJobsForAllTenants("daily")
}

func (s *Scheduler) scheduleUploadJobsForAllTenants(jobType string) {
	tenants := config.GetEnabledTenants()

	for tenantID := range tenants {
		if err := s.scheduleUploadJobsForTenant(tenantID, jobType); err != nil {
			log.Printf("[SCHEDULER] Failed to schedule upload jobs for tenant %s: %v", tenantID, err)
		}
	}
}

func (s *Scheduler) scheduleUploadJobsForTenant(tenantID, jobType string) error {
	tenantDir := filepath.Join(s.localPath, tenantID)
	if _, err := os.Stat(tenantDir); os.IsNotExist(err) {
		log.Printf("[SCHEDULER] No directory found for tenant %s", tenantID)
		return nil
	}

	files, err := filepath.Glob(filepath.Join(tenantDir, "*.csv"))
	if err != nil {
		return err
	}

	now := time.Now()
	uploadCount := 0

	for _, filePath := range files {
		fileName := filepath.Base(filePath)
		fileType := utils.DetermineFileType(fileName)

		// Filter files based on job type
		shouldUpload := false
		switch jobType {
		case "30min":
			// Upload 30min files created in the last 10 minutes
			if fileType == "30MIN" {
				if fileInfo, err := os.Stat(filePath); err == nil {
					if time.Since(fileInfo.ModTime()) < 10*time.Minute {
						shouldUpload = true
					}
				}
			}
		case "daily":
			// Upload daily files created in the last hour
			if fileType == "DAILY" {
				if fileInfo, err := os.Stat(filePath); err == nil {
					if time.Since(fileInfo.ModTime()) < 1*time.Hour {
						shouldUpload = true
					}
				}
			}
		}

		if !shouldUpload {
			continue
		}

		remotePath := filepath.Join("/upload", fileName)
		uploadJob := types.UploadSFTPJob{
			TenantID:   tenantID,
			FilePath:   filePath,
			FileName:   fileName,
			RemotePath: remotePath,
			FileType:   fileType,
			CreatedAt:  now,
		}

		if err := s.queue.PublishJob(types.SubjectUploadSFTP, uploadJob); err != nil {
			log.Printf("[SCHEDULER] Failed to publish upload job for file %s: %v", fileName, err)
			continue
		}

		uploadCount++
	}

	log.Printf("[SCHEDULER] Scheduled %d upload jobs for tenant %s (%s)", uploadCount, tenantID, jobType)
	return nil
}

func (s *Scheduler) scheduleCleanupJob() {
	log.Println("[SCHEDULER] Starting cleanup of old files")

	tenants := config.GetEnabledTenants()
	maxAge := 7 * 24 * time.Hour // Keep files for 7 days

	for tenantID := range tenants {
		tenantDir := filepath.Join(s.localPath, tenantID)
		if err := s.cleanupOldFiles(tenantDir, maxAge); err != nil {
			log.Printf("[SCHEDULER] Failed to cleanup files for tenant %s: %v", tenantID, err)
		}
	}
}

func (s *Scheduler) cleanupOldFiles(dirPath string, maxAge time.Duration) error {
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		return nil
	}

	files, err := filepath.Glob(filepath.Join(dirPath, "*.csv"))
	if err != nil {
		return err
	}

	cleanedCount := 0
	for _, filePath := range files {
		fileInfo, err := os.Stat(filePath)
		if err != nil {
			continue
		}

		if time.Since(fileInfo.ModTime()) > maxAge {
			if err := os.Remove(filePath); err != nil {
				log.Printf("[SCHEDULER] Failed to remove old file %s: %v", filePath, err)
				continue
			}
			cleanedCount++
		}
	}

	if cleanedCount > 0 {
		log.Printf("[SCHEDULER] Cleaned up %d old files from %s", cleanedCount, dirPath)
	}

	return nil
}
