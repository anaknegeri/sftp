package scheduler

import (
	"fmt"
	"jarvist/sftp/internal/config"
	"jarvist/sftp/internal/queue"
	"jarvist/sftp/internal/service"
	"jarvist/sftp/internal/types"
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

	// NEW: Late data check jobs
	lateDataConfig := config.GetLateDataScheduleConfig()
	if lateDataConfig.EnableLateDataCheck {
		// Realtime check - untuk data yang baru masuk (every X minutes)
		cronExpr := fmt.Sprintf("0 %d * * * *", lateDataConfig.ThirtyMinCheckMinute)
		_, err = s.cron.AddFunc(cronExpr, s.scheduleRealtimeLateDataCheck)
		if err != nil {
			return err
		}
		log.Printf("[SCHEDULER] Realtime late data check scheduled every hour at minute %d", lateDataConfig.ThirtyMinCheckMinute)

		// Historical check - untuk data lama yang mungkin terlewat (daily at specific hours)
		for _, hour := range lateDataConfig.DailyCheckHours {
			cronExpr := fmt.Sprintf("0 %d %d * * *", lateDataConfig.DailyCheckMinute, hour)
			_, err = s.cron.AddFunc(cronExpr, s.scheduleHistoricalLateDataCheck)
			if err != nil {
				return err
			}
		}
		log.Printf("[SCHEDULER] Historical late data check scheduled at hours %v, minute %d",
			lateDataConfig.DailyCheckHours, lateDataConfig.DailyCheckMinute)
	} else {
		log.Println("[SCHEDULER] Late data check is disabled")
	}

	// Job untuk cleanup old files - jalan setiap hari jam 2 malam
	_, err = s.cron.AddFunc("0 0 2 * * *", s.scheduleCleanupJob)
	if err != nil {
		return err
	}

	s.cron.Start()
	log.Println("[SCHEDULER] Scheduler started successfully")
	log.Println("[SCHEDULER] Upload jobs are handled automatically by export service (no separate upload scheduling)")
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

// NEW: Schedule realtime late data check
func (s *Scheduler) scheduleRealtimeLateDataCheck() {
	log.Println("[SCHEDULER] Scheduling realtime late data check jobs")

	tenants := config.GetEnabledTenants()
	now := config.NowJakarta()

	for tenantID := range tenants {
		job := types.LateDataCheckJob{
			TenantID:  tenantID,
			Date:      now,
			CheckType: "realtime",
			CreatedAt: now,
		}

		if err := s.queue.PublishJob(types.SubjectLateDataCheck, job); err != nil {
			log.Printf("[SCHEDULER] Failed to publish realtime late data check job for tenant %s: %v", tenantID, err)
		}
	}
}

// NEW: Schedule historical late data check
func (s *Scheduler) scheduleHistoricalLateDataCheck() {
	log.Println("[SCHEDULER] Scheduling historical late data check jobs")

	tenants := config.GetEnabledTenants()
	now := config.NowJakarta()

	lateDataConfig := config.GetLateDataScheduleConfig()

	// Check multiple days back for historical data
	for i := 1; i <= lateDataConfig.HistoricalLookbackDays; i++ {
		checkDate := now.AddDate(0, 0, -i)

		for tenantID := range tenants {
			job := types.LateDataCheckJob{
				TenantID:  tenantID,
				Date:      checkDate,
				CheckType: "historical",
				CreatedAt: now,
			}

			if err := s.queue.PublishJob(types.SubjectLateDataCheck, job); err != nil {
				log.Printf("[SCHEDULER] Failed to publish historical late data check job for tenant %s: %v", tenantID, err)
			}
		}
	}
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
