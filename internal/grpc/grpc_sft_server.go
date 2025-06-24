// File: internal/grpc/grpc_sft_server.go - Async pattern to avoid timeouts
package grpc

import (
	"context"
	"fmt"
	"log"
	"time"

	"jarvist/sftp-service/internal/queue"
	"jarvist/sftp-service/internal/service"
	"jarvist/sftp-service/internal/types"
	pb "jarvist/sftp-service/proto/sftp"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type SFTPGRPCServer struct {
	pb.UnimplementedExportServiceServer
	exportService service.ExportService
	sftpService   service.SFTPService
	jobQueue      queue.JobQueue // Add job queue for async operations
}

func NewSFTPGRPCServer(exportService service.ExportService, sftpService service.SFTPService, jobQueue queue.JobQueue) *SFTPGRPCServer {
	return &SFTPGRPCServer{
		exportService: exportService,
		sftpService:   sftpService,
		jobQueue:      jobQueue,
	}
}

// ASYNC: ExportDaily - Queue job and return immediately
func (s *SFTPGRPCServer) ExportDaily(ctx context.Context, req *pb.ExportDailyRequest) (*pb.ExportResponse, error) {
	log.Printf("[GRPC] ExportDaily request received for tenant: %s", req.TenantId)

	if err := s.validateTenantID(req.TenantId); err != nil {
		return nil, err
	}

	date, err := time.Parse("2006-01-02", req.Date)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid date format, use YYYY-MM-DD")
	}

	// ASYNC: Queue job instead of direct processing
	job := types.GenerateReportJob{
		TenantID:  req.TenantId,
		Date:      date,
		JobType:   "daily",
		CreatedAt: time.Now(),
	}

	if err := s.jobQueue.PublishJob(types.SubjectGenerateReport, job); err != nil {
		log.Printf("[GRPC] Failed to queue daily export job for tenant %s: %v", req.TenantId, err)
		return &pb.ExportResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to queue export job: %v", err),
		}, nil
	}

	log.Printf("[GRPC] Daily export job queued successfully for tenant: %s", req.TenantId)
	return &pb.ExportResponse{
		Success: true,
		Message: fmt.Sprintf("Daily export job queued successfully for %s. Processing will start shortly.", req.Date),
	}, nil
}

// ASYNC: Export30Min - Queue job and return immediately
func (s *SFTPGRPCServer) Export30Min(ctx context.Context, req *pb.Export30MinRequest) (*pb.ExportResponse, error) {
	log.Printf("[GRPC] Export30Min request received for tenant: %s", req.TenantId)

	if err := s.validateTenantID(req.TenantId); err != nil {
		return nil, err
	}

	triggerTime, err := time.Parse("2006-01-02 15:04", req.DateTime)
	if err != nil {
		// Try with date only format
		triggerTime, err = time.Parse("2006-01-02", req.DateTime)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid datetime format, use YYYY-MM-DD HH:MM or YYYY-MM-DD")
		}
	}

	job := types.GenerateReportJob{
		TenantID:  req.TenantId,
		Date:      triggerTime,
		JobType:   "30min",
		CreatedAt: time.Now(),
	}

	if err := s.jobQueue.PublishJob(types.SubjectGenerateReport, job); err != nil {
		log.Printf("[GRPC] Failed to queue 30min export job for tenant %s: %v", req.TenantId, err)
		return &pb.ExportResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to queue export job: %v", err),
		}, nil
	}

	log.Printf("[GRPC] 30min export job queued successfully for tenant: %s", req.TenantId)
	return &pb.ExportResponse{
		Success: true,
		Message: fmt.Sprintf("30-minute export job queued successfully for %s. Processing will start shortly.", req.DateTime),
	}, nil
}

// ASYNC: ExportAllReport - Queue job and return immediately
func (s *SFTPGRPCServer) ExportAllReport(ctx context.Context, req *pb.ExportAllReportRequest) (*pb.ExportResponse, error) {
	log.Printf("[GRPC] ExportAllReport request received for tenant: %s", req.TenantId)

	if err := s.validateTenantID(req.TenantId); err != nil {
		return nil, err
	}

	date, err := time.Parse("2006-01-02", req.Date)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid date format, use YYYY-MM-DD")
	}

	// Queue both daily and 30min jobs
	dailyJob := types.GenerateReportJob{
		TenantID:  req.TenantId,
		Date:      date,
		JobType:   "daily",
		CreatedAt: time.Now(),
	}

	thirtyMinJob := types.GenerateReportJob{
		TenantID:  req.TenantId,
		Date:      date,
		JobType:   "30min",
		CreatedAt: time.Now(),
	}

	// Queue daily job
	if err := s.jobQueue.PublishJob(types.SubjectGenerateReport, dailyJob); err != nil {
		log.Printf("[GRPC] Failed to queue daily job for tenant %s: %v", req.TenantId, err)
		return &pb.ExportResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to queue daily export job: %v", err),
		}, nil
	}

	// Queue 30min job
	if err := s.jobQueue.PublishJob(types.SubjectGenerateReport, thirtyMinJob); err != nil {
		log.Printf("[GRPC] Failed to queue 30min job for tenant %s: %v", req.TenantId, err)
		return &pb.ExportResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to queue 30min export job: %v", err),
		}, nil
	}

	log.Printf("[GRPC] All export jobs queued successfully for tenant: %s", req.TenantId)
	return &pb.ExportResponse{
		Success: true,
		Message: fmt.Sprintf("Complete export jobs (daily + 30min) queued successfully for %s. Processing will start shortly.", req.Date),
	}, nil
}

// ASYNC: ExportByLocationID - Queue job and return immediately
func (s *SFTPGRPCServer) ExportByLocationID(ctx context.Context, req *pb.ExportByLocationIDRequest) (*pb.ExportResponse, error) {
	log.Printf("[GRPC] ExportByLocationID request received for tenant: %s, location: %s",
		req.TenantId, req.LocationId)

	if err := s.validateTenantID(req.TenantId); err != nil {
		return nil, err
	}

	if err := s.validateLocationID(req.LocationId); err != nil {
		return nil, err
	}

	date, err := time.Parse("2006-01-02", req.Date)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid date format, use YYYY-MM-DD")
	}

	// For location-specific exports, we can use a custom job type or add location info
	job := types.GenerateReportJob{
		TenantID:  req.TenantId,
		Date:      date,
		JobType:   fmt.Sprintf("daily_location_%s", req.LocationId), // Custom job type
		CreatedAt: time.Now(),
	}

	if err := s.jobQueue.PublishJob(types.SubjectGenerateReport, job); err != nil {
		log.Printf("[GRPC] Failed to queue location export job for tenant %s, location %s: %v",
			req.TenantId, req.LocationId, err)
		return &pb.ExportResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to queue export job: %v", err),
		}, nil
	}

	log.Printf("[GRPC] Location export job queued successfully for tenant: %s, location: %s",
		req.TenantId, req.LocationId)
	return &pb.ExportResponse{
		Success: true,
		Message: fmt.Sprintf("Daily export job for location %s queued successfully for %s. Processing will start shortly.", req.LocationId, req.Date),
	}, nil
}

// ASYNC: Export30MinByLocationID
func (s *SFTPGRPCServer) Export30MinByLocationID(ctx context.Context, req *pb.Export30MinByLocationIDRequest) (*pb.ExportResponse, error) {
	log.Printf("[GRPC] Export30MinByLocationID request received for tenant: %s, location: %s",
		req.TenantId, req.LocationId)

	if err := s.validateTenantID(req.TenantId); err != nil {
		return nil, err
	}

	if err := s.validateLocationID(req.LocationId); err != nil {
		return nil, err
	}

	triggerTime, err := time.Parse("2006-01-02 15:04", req.DateTime)
	if err != nil {
		triggerTime, err = time.Parse("2006-01-02", req.DateTime)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid datetime format, use YYYY-MM-DD HH:MM or YYYY-MM-DD")
		}
	}

	job := types.GenerateReportJob{
		TenantID:  req.TenantId,
		Date:      triggerTime,
		JobType:   fmt.Sprintf("30min_location_%s", req.LocationId),
		CreatedAt: time.Now(),
	}

	if err := s.jobQueue.PublishJob(types.SubjectGenerateReport, job); err != nil {
		log.Printf("[GRPC] Failed to queue 30min location export job for tenant %s, location %s: %v",
			req.TenantId, req.LocationId, err)
		return &pb.ExportResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to queue export job: %v", err),
		}, nil
	}

	log.Printf("[GRPC] 30min location export job queued successfully for tenant: %s, location: %s",
		req.TenantId, req.LocationId)
	return &pb.ExportResponse{
		Success: true,
		Message: fmt.Sprintf("30-minute export job for location %s queued successfully for %s. Processing will start shortly.", req.LocationId, req.DateTime),
	}, nil
}

// ASYNC: ExportAllReportByLocationID
func (s *SFTPGRPCServer) ExportAllReportByLocationID(ctx context.Context, req *pb.ExportAllReportByLocationIDRequest) (*pb.ExportResponse, error) {
	log.Printf("[GRPC] ExportAllReportByLocationID request received for tenant: %s, location: %s",
		req.TenantId, req.LocationId)

	if err := s.validateTenantID(req.TenantId); err != nil {
		return nil, err
	}

	if err := s.validateLocationID(req.LocationId); err != nil {
		return nil, err
	}

	date, err := time.Parse("2006-01-02", req.Date)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid date format, use YYYY-MM-DD")
	}

	// Queue both daily and 30min jobs for specific location
	dailyJob := types.GenerateReportJob{
		TenantID:  req.TenantId,
		Date:      date,
		JobType:   fmt.Sprintf("daily_location_%s", req.LocationId),
		CreatedAt: time.Now(),
	}

	thirtyMinJob := types.GenerateReportJob{
		TenantID:  req.TenantId,
		Date:      date,
		JobType:   fmt.Sprintf("30min_location_%s", req.LocationId),
		CreatedAt: time.Now(),
	}

	if err := s.jobQueue.PublishJob(types.SubjectGenerateReport, dailyJob); err != nil {
		log.Printf("[GRPC] Failed to queue daily location job: %v", err)
		return &pb.ExportResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to queue daily export job: %v", err),
		}, nil
	}

	if err := s.jobQueue.PublishJob(types.SubjectGenerateReport, thirtyMinJob); err != nil {
		log.Printf("[GRPC] Failed to queue 30min location job: %v", err)
		return &pb.ExportResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to queue 30min export job: %v", err),
		}, nil
	}

	log.Printf("[GRPC] All location export jobs queued successfully for tenant: %s, location: %s",
		req.TenantId, req.LocationId)
	return &pb.ExportResponse{
		Success: true,
		Message: fmt.Sprintf("Complete export jobs for location %s queued successfully for %s. Processing will start shortly.", req.LocationId, req.Date),
	}, nil
}

func (s *SFTPGRPCServer) UploadAllPendingFiles(ctx context.Context, req *pb.UploadAllPendingFilesRequest) (*pb.ExportResponse, error) {
	log.Printf("[GRPC] UploadAllPendingFiles request received for tenant: %s", req.TenantId)

	if err := s.validateTenantID(req.TenantId); err != nil {
		return nil, err
	}

	// Set shorter timeout for upload operations
	uploadCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- s.sftpService.UploadAllPendingFiles(req.TenantId)
	}()

	select {
	case err := <-done:
		if err != nil {
			log.Printf("[GRPC] UploadAllPendingFiles failed for tenant %s: %v", req.TenantId, err)
			return &pb.ExportResponse{
				Success: false,
				Message: fmt.Sprintf("Upload failed: %v", err),
			}, nil
		}

		log.Printf("[GRPC] UploadAllPendingFiles completed successfully for tenant: %s", req.TenantId)
		return &pb.ExportResponse{
			Success: true,
			Message: "Upload all pending files completed successfully",
		}, nil

	case <-uploadCtx.Done():
		log.Printf("[GRPC] UploadAllPendingFiles timeout for tenant: %s", req.TenantId)
		return &pb.ExportResponse{
			Success: false,
			Message: "Upload operation timed out after 5 minutes. Files may still be uploading in background.",
		}, nil
	}
}

// Validation helpers
func (s *SFTPGRPCServer) validateTenantID(tenantID string) error {
	if tenantID == "" {
		return status.Error(codes.InvalidArgument, "tenant_id is required")
	}
	return nil
}

func (s *SFTPGRPCServer) validateLocationID(locationID string) error {
	if locationID == "" {
		return status.Error(codes.InvalidArgument, "location_id is required")
	}
	return nil
}

func (s *SFTPGRPCServer) HealthCheck(ctx context.Context, req *pb.HealthCheckRequest) (*pb.HealthCheckResponse, error) {
	return &pb.HealthCheckResponse{
		Ok:      true,
		Message: "SFTP server is healthy",
	}, nil
}
