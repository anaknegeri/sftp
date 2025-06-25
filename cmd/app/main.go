// File: cmd/app/main.go
package main

import (
	"fmt"
	"jarvist/sftp-service/internal/config"
	"jarvist/sftp-service/internal/database"
	"jarvist/sftp-service/internal/file"
	grpcHandler "jarvist/sftp-service/internal/grpc"
	"jarvist/sftp-service/internal/queue"
	"jarvist/sftp-service/internal/repository"
	"jarvist/sftp-service/internal/scheduler"
	"jarvist/sftp-service/internal/service"
	"jarvist/sftp-service/internal/worker"
	pb "jarvist/sftp-service/proto/sftp"
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
	log.Println("Starting JARVIST SFTP Service...")

	// Load configuration
	cfg := config.Load()
	log.Printf("Configuration loaded successfully")

	// Initialize database
	log.Println("Connecting to database...")
	db, err := database.NewDatabase(cfg.Database)
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	log.Println("Database connected successfully")

	// Initialize repositories
	peopleRepo := repository.NewPeopleCountRepository(db, cfg)
	sftpLogRepo := repository.NewSFTPLogRepository(db)
	csvWriter := file.NewCSVWriter(cfg.LocalPath)

	// Initialize NATS queue (for publishing jobs)
	log.Println("Initializing NATS queue...")
	var jobQueue queue.JobQueue
	maxRetries := 3

	for attempt := 1; attempt <= maxRetries; attempt++ {
		jobQueue, err = queue.NewJobQueue(cfg.NATS.URL)
		if err == nil {
			log.Println("NATS queue connected successfully")
			break
		}

		log.Printf("Queue connection attempt %d failed: %v", attempt, err)
		if attempt < maxRetries {
			time.Sleep(time.Duration(attempt) * 2 * time.Second)
		}
	}

	if jobQueue == nil {
		log.Fatalf("Failed to initialize job queue after %d attempts: %v", maxRetries, err)
	}
	defer jobQueue.Close()

	// Initialize services
	exportService := service.NewExportService(peopleRepo, sftpLogRepo, csvWriter, cfg.LocalPath, jobQueue)
	sftpService := service.NewSFTPService(sftpLogRepo, cfg.LocalPath)
	lateDataService := service.NewLateDataService(peopleRepo, sftpLogRepo, csvWriter, sftpService, cfg.LocalPath)

	// Initialize NATS worker (for consuming jobs)
	log.Println("Starting NATS worker...")
	natsWorker, err := worker.NewNATSWorker(cfg.NATS.URL, exportService, sftpService, lateDataService)
	if err != nil {
		log.Fatalf("Failed to create NATS worker: %v", err)
	}

	if err := natsWorker.Start(); err != nil {
		log.Fatalf("Failed to start NATS worker: %v", err)
	}
	defer natsWorker.Stop()
	log.Println("NATS worker started successfully")

	// Initialize scheduler
	log.Println("Starting scheduler...")
	jobScheduler := scheduler.NewScheduler(jobQueue, sftpService, cfg.LocalPath)
	if err := jobScheduler.Start(); err != nil {
		log.Fatalf("Failed to start scheduler: %v", err)
	}
	defer jobScheduler.Stop()
	log.Println("Scheduler started successfully")

	// Start gRPC server
	log.Printf("Starting gRPC server on port %s...", cfg.GRPCPort)
	exportGRPCServer := grpcHandler.NewSFTPGRPCServer(exportService, sftpService, jobQueue)
	go func() {
		if err := startGRPCServer(cfg, exportGRPCServer); err != nil {
			log.Fatalf("Failed to start gRPC server: %v", err)
		}
	}()

	log.Printf("JARVIST SFTP Service started successfully on port %s", cfg.GRPCPort)
	log.Println("Service is ready to handle requests")

	// Graceful shutdown
	gracefulShutdown(jobScheduler, jobQueue, natsWorker)
}

func startGRPCServer(cfg *config.Config, server *grpcHandler.SFTPGRPCServer) error {
	lis, err := net.Listen("tcp", ":"+cfg.GRPCPort)
	if err != nil {
		return fmt.Errorf("failed to listen on port %s: %w", cfg.GRPCPort, err)
	}

	s := grpc.NewServer()
	pb.RegisterExportServiceServer(s, server)
	reflection.Register(s)

	log.Printf("gRPC server listening on port %s", cfg.GRPCPort)
	return s.Serve(lis)
}

func gracefulShutdown(jobScheduler *scheduler.Scheduler, jobQueue queue.JobQueue, natsWorker *worker.NATSWorker) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigChan
	log.Printf("Received signal %v, shutting down gracefully...", sig)

	// Stop components in reverse order
	log.Println("Stopping scheduler...")
	if jobScheduler != nil {
		jobScheduler.Stop()
	}

	log.Println("Stopping NATS worker...")
	if natsWorker != nil {
		natsWorker.Stop()
	}

	log.Println("Closing NATS queue...")
	if jobQueue != nil {
		jobQueue.Close()
	}

	log.Println("Server stopped gracefully")
}
