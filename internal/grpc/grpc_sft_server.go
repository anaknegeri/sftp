package grpc

import (
	"context"
	"fmt"
	"log"
	"time"

	"jarvist/sftp-service/internal/service"
	pb "jarvist/sftp-service/proto/sftp"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type SFTPGRPCServer struct {
	pb.UnimplementedExportServiceServer
	exportService service.ExportService
}

func NewSFTPGRPCServer(exportService service.ExportService) *SFTPGRPCServer {
	return &SFTPGRPCServer{
		exportService: exportService,
	}
}

func (s *SFTPGRPCServer) ExportDaily(ctx context.Context, req *pb.ExportDailyRequest) (*pb.ExportResponse, error) {
	log.Printf("[GRPC] ExportDaily request received for tenant: %s", req.TenantId)

	if err := s.validateTenantID(req.TenantId); err != nil {
		return nil, err
	}

	date, err := time.Parse("2006-01-02", req.Date)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid date format, use YYYY-MM-DD")
	}

	if err := s.exportService.ExportDaily(req.TenantId, date); err != nil {
		log.Printf("[GRPC] ExportDaily failed for tenant %s: %v", req.TenantId, err)
		return &pb.ExportResponse{
			Success: false,
			Message: fmt.Sprintf("Export failed: %v", err),
		}, nil
	}

	log.Printf("[GRPC] ExportDaily completed successfully for tenant: %s", req.TenantId)
	return &pb.ExportResponse{
		Success: true,
		Message: "Daily export completed successfully",
	}, nil
}

func (s *SFTPGRPCServer) Export30Min(ctx context.Context, req *pb.Export30MinRequest) (*pb.ExportResponse, error) {
	log.Printf("[GRPC] Export30Min request received for tenant: %s", req.TenantId)

	if err := s.validateTenantID(req.TenantId); err != nil {
		return nil, err
	}

	triggerTime, err := time.Parse("2006-01-02", req.DateTime)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid date format, use YYYY-MM-DD")
	}

	if err := s.exportService.Export30Min(req.TenantId, triggerTime); err != nil {
		log.Printf("[GRPC] Export30Min failed for tenant %s: %v", req.TenantId, err)
		return &pb.ExportResponse{
			Success: false,
			Message: fmt.Sprintf("30-minute export failed: %v", err),
		}, nil
	}

	log.Printf("[GRPC] Export30Min completed successfully for tenant: %s", req.TenantId)
	return &pb.ExportResponse{
		Success: true,
		Message: "30-minute export completed successfully",
	}, nil
}

func (s *SFTPGRPCServer) ExportAllReport(ctx context.Context, req *pb.ExportAllReportRequest) (*pb.ExportResponse, error) {
	log.Printf("[GRPC] ExportAllReport request received for tenant: %s", req.TenantId)

	if err := s.validateTenantID(req.TenantId); err != nil {
		return nil, err
	}

	date, err := time.Parse("2006-01-02", req.Date)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid date format, use YYYY-MM-DD")
	}

	if err := s.exportService.ExportAllReport(req.TenantId, date); err != nil {
		log.Printf("[GRPC] ExportAllReport failed for tenant %s: %v", req.TenantId, err)
		return &pb.ExportResponse{
			Success: false,
			Message: fmt.Sprintf("Complete export failed: %v", err),
		}, nil
	}

	log.Printf("[GRPC] ExportAllReport completed successfully for tenant: %s", req.TenantId)
	return &pb.ExportResponse{
		Success: true,
		Message: "Complete export completed successfully",
	}, nil
}

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

	if err := s.exportService.ExportByLocationID(req.TenantId, req.LocationId, date); err != nil {
		log.Printf("[GRPC] ExportByLocationID failed for tenant %s, location %s: %v",
			req.TenantId, req.LocationId, err)
		return &pb.ExportResponse{
			Success: false,
			Message: fmt.Sprintf("Daily export by location failed: %v", err),
		}, nil
	}

	log.Printf("[GRPC] ExportByLocationID completed successfully for tenant: %s, location: %s",
		req.TenantId, req.LocationId)
	return &pb.ExportResponse{
		Success: true,
		Message: "Daily export by location completed successfully",
	}, nil
}

func (s *SFTPGRPCServer) Export30MinByLocationID(ctx context.Context, req *pb.Export30MinByLocationIDRequest) (*pb.ExportResponse, error) {
	log.Printf("[GRPC] Export30MinByLocationID request received for tenant: %s, location: %s",
		req.TenantId, req.LocationId)

	if err := s.validateTenantID(req.TenantId); err != nil {
		return nil, err
	}

	if err := s.validateLocationID(req.LocationId); err != nil {
		return nil, err
	}

	triggerTime, err := time.Parse("2006-01-02", req.DateTime)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid date format, use YYYY-MM-DD")
	}

	if err := s.exportService.Export30MinByLocationID(req.TenantId, req.LocationId, triggerTime); err != nil {
		log.Printf("[GRPC] Export30MinByLocationID failed for tenant %s, location %s: %v",
			req.TenantId, req.LocationId, err)
		return &pb.ExportResponse{
			Success: false,
			Message: fmt.Sprintf("30-minute export by location failed: %v", err),
		}, nil
	}

	log.Printf("[GRPC] Export30MinByLocationID completed successfully for tenant: %s, location: %s",
		req.TenantId, req.LocationId)
	return &pb.ExportResponse{
		Success: true,
		Message: "30-minute export by location completed successfully",
	}, nil
}

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

	if err := s.exportService.ExportAllReportByLocationID(req.TenantId, req.LocationId, date); err != nil {
		log.Printf("[GRPC] ExportAllReportByLocationID failed for tenant %s, location %s: %v",
			req.TenantId, req.LocationId, err)
		return &pb.ExportResponse{
			Success: false,
			Message: fmt.Sprintf("Complete export by location failed: %v", err),
		}, nil
	}

	log.Printf("[GRPC] ExportAllReportByLocationID completed successfully for tenant: %s, location: %s",
		req.TenantId, req.LocationId)
	return &pb.ExportResponse{
		Success: true,
		Message: "Complete export by location completed successfully",
	}, nil
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

// Helper function to convert time to timestamp
func TimeToTimestamp(t time.Time) *timestamppb.Timestamp {
	return timestamppb.New(t)
}

// Helper function to convert timestamp to time
func TimestampToTime(ts *timestamppb.Timestamp) time.Time {
	if ts == nil {
		return time.Time{}
	}
	return ts.AsTime()
}
