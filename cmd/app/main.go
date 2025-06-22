package main

import (
	"context"
	"fmt"
	"jarvist/sftp/internal/config"
	databse "jarvist/sftp/internal/database"
	"jarvist/sftp/internal/file"
	grpcHandler "jarvist/sftp/internal/grpc"
	"jarvist/sftp/internal/job"
	"jarvist/sftp/internal/queue"
	"jarvist/sftp/internal/repository"
	"jarvist/sftp/internal/scheduler"
	"jarvist/sftp/internal/service"
	pb "jarvist/sftp/pkg/pb"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	// Load configuration
	cfg := config.Load()

	// Initialize database
	db, err := databse.NewDatabase(cfg.Database)
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}

	jobQueue, err := queue.NewJobQueue(cfg.NATSURL)
	if err != nil {
		log.Fatalf("Failed to initialize job queue: %v", err)
	}
	defer jobQueue.Close()

	// Initialize dependencies
	peopleRepo := repository.NewPeopleCountRepository(db)
	sftpLogRepo := repository.NewSFTPLogRepository(db)
	csvWriter := file.NewCSVWriter(cfg.LocalPath)

	// Initialize service
	exportService := service.NewExportService(peopleRepo, sftpLogRepo, csvWriter, cfg.LocalPath, jobQueue)
	sftpService := service.NewSFTPService(sftpLogRepo, cfg.LocalPath)

	// Initialize job processor
	jobProcessor := job.NewJobProcessor(jobQueue, exportService, sftpService)
	if err := jobProcessor.Start(); err != nil {
		log.Fatalf("Failed to start job processor: %v", err)
	}

	// Initialize scheduler
	jobScheduler := scheduler.NewScheduler(jobQueue, sftpService, cfg.LocalPath)
	if err := jobScheduler.Start(); err != nil {
		log.Fatalf("Failed to start scheduler: %v", err)
	}

	// Initialize gRPC server
	exportGRPCServer := grpcHandler.NewSFTPGRPCServer(exportService)

	// Start gRPC server in a goroutine
	go func() {
		if err := startGRPCServer(cfg, exportGRPCServer); err != nil {
			log.Fatalf("Failed to start gRPC server: %v", err)
		}
	}()

	// Wait for interrupt signal to gracefully shutdown
	gracefulShutdown()
}

func startGRPCServer(cfg *config.Config, server *grpcHandler.SFTPGRPCServer) error {
	lis, err := net.Listen("tcp", ":"+cfg.GRPCPort)
	if err != nil {
		return fmt.Errorf("failed to listen on gRPC port %s: %w", cfg.GRPCPort, err)
	}

	s := grpc.NewServer(
		grpc.UnaryInterceptor(loggingInterceptor),
	)

	// Register export service
	pb.RegisterExportServiceServer(s, server)

	// Enable reflection for easier debugging
	reflection.Register(s)

	log.Printf("gRPC server starting on port %s", cfg.GRPCPort)
	log.Printf("Export service ready to handle requests")

	if err := s.Serve(lis); err != nil {
		return fmt.Errorf("gRPC server failed to serve: %w", err)
	}

	return nil
}

func gracefulShutdown() {
	// Create a channel to receive OS signals
	sigChan := make(chan os.Signal, 1)

	// Register the channel to receive specific signals
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Block until a signal is received
	sig := <-sigChan
	log.Printf("Received signal: %v. Shutting down gracefully...", sig)

	// Add any cleanup logic here if needed
	log.Println("Server stopped")
}

func loggingInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	start := time.Now()

	log.Printf("[GRPC] Starting %s", info.FullMethod)

	resp, err := handler(ctx, req)

	duration := time.Since(start)
	status := "SUCCESS"
	if err != nil {
		status = "ERROR"
		log.Printf("[GRPC] Error in %s: %v", info.FullMethod, err)
	}

	log.Printf("[GRPC] Completed %s [%s] in %v", info.FullMethod, status, duration)

	return resp, err
}
