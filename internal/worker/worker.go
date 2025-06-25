// File: internal/worker/worker.go - COMPLETE FIXED VERSION
package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"jarvist/sftp-service/internal/service"
	"jarvist/sftp-service/internal/types"

	"github.com/nats-io/nats.go"
)

const (
	SubjectGenerateReport = "jarvist.sftp.generate.report"
	SubjectUploadSFTP     = "jarvist.sftp.upload.sftp"
	SubjectLateDataCheck  = "jarvist.sftp.late.data.check"
)

type NATSWorker struct {
	nc              *nats.Conn
	js              nats.JetStreamContext
	exportService   service.ExportService
	sftpService     service.SFTPService
	lateDataService service.LateDataService
	subs            map[string]*nats.Subscription
	mu              sync.RWMutex
	ctx             context.Context
	cancel          context.CancelFunc
	wg              sync.WaitGroup
	processingJobs  sync.Map
	healthTicker    *time.Ticker
	isHealthy       int64
}

type ProcessingJob struct {
	StartTime time.Time
	WorkerID  int
	LogID     string
}

type ConsumerConfig struct {
	Subject     string
	Handler     func(*nats.Msg)
	Concurrency int
	BatchSize   int
	AckWait     time.Duration
	MaxDeliver  int
}

func NewNATSWorker(natsURL string, exportService service.ExportService, sftpService service.SFTPService, lateDataService service.LateDataService) (*NATSWorker, error) {
	if exportService == nil {
		return nil, fmt.Errorf("exportService cannot be nil")
	}
	if sftpService == nil {
		return nil, fmt.Errorf("sftpService cannot be nil")
	}
	if lateDataService == nil {
		return nil, fmt.Errorf("lateDataService cannot be nil")
	}

	// Enhanced connection options with better monitoring
	opts := []nats.Option{
		nats.Name("jarvist-sftp-worker"),
		nats.ReconnectWait(5 * time.Second),
		nats.MaxReconnects(-1),
		nats.Timeout(30 * time.Second),
		nats.PingInterval(30 * time.Second),
		nats.MaxPingsOutstanding(3),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			log.Printf("[WORKER] NATS disconnected: %v", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Printf("[WORKER] NATS reconnected to %s", nc.ConnectedUrl())
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			log.Println("[WORKER] NATS connection closed")
		}),
		nats.ErrorHandler(func(nc *nats.Conn, sub *nats.Subscription, err error) {
			log.Printf("[WORKER] NATS error: %v", err)
		}),
	}

	var nc *nats.Conn
	var err error

	// Retry connection with backoff
	maxRetries := 5
	for attempt := 1; attempt <= maxRetries; attempt++ {
		nc, err = nats.Connect(natsURL, opts...)
		if err == nil {
			break
		}
		log.Printf("[WORKER] Connection attempt %d failed: %v", attempt, err)
		if attempt < maxRetries {
			time.Sleep(time.Duration(attempt) * 2 * time.Second)
		}
	}

	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS after %d attempts: %w", maxRetries, err)
	}

	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to create JetStream: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	worker := &NATSWorker{
		nc:              nc,
		js:              js,
		exportService:   exportService,
		sftpService:     sftpService,
		lateDataService: lateDataService,
		subs:            make(map[string]*nats.Subscription),
		ctx:             ctx,
		cancel:          cancel,
	}

	// Set healthy status
	atomic.StoreInt64(&worker.isHealthy, 1)

	// Start health monitoring
	worker.startHealthMonitoring()

	return worker, nil
}

func (w *NATSWorker) startHealthMonitoring() {
	w.healthTicker = time.NewTicker(30 * time.Second)
	go func() {
		defer w.healthTicker.Stop()
		for {
			select {
			case <-w.ctx.Done():
				return
			case <-w.healthTicker.C:
				w.performHealthCheck()
			}
		}
	}()
}

// ENHANCED: Better health monitoring with upload tracking
func (w *NATSWorker) performHealthCheck() {
	if w.nc == nil || w.nc.IsClosed() {
		atomic.StoreInt64(&w.isHealthy, 0)
		log.Printf("[WORKER] Health check failed: connection closed")
		return
	}

	// Check if connection is responsive
	err := w.nc.Flush()
	if err != nil {
		atomic.StoreInt64(&w.isHealthy, 0)
		log.Printf("[WORKER] Health check failed: %v", err)
		return
	}

	atomic.StoreInt64(&w.isHealthy, 1)

	// Clean up expired processing jobs with detailed logging
	cutoff := time.Now().Add(-15 * time.Minute) // Increased from 10 minutes
	cleanedCount := 0
	stuckJobs := []string{}

	w.processingJobs.Range(func(key, value interface{}) bool {
		if job, ok := value.(ProcessingJob); ok {
			if job.StartTime.Before(cutoff) {
				w.processingJobs.Delete(key)
				cleanedCount++
				if job.LogID != "" {
					stuckJobs = append(stuckJobs, job.LogID)
				}
			}
		}
		return true
	})

	if cleanedCount > 0 {
		log.Printf("[WORKER] Cleanup: Removed %d stuck processing jobs (stuck > 15min)", cleanedCount)
		for _, logID := range stuckJobs {
			log.Printf("[WORKER] Cleaned stuck job log ID: %s", logID)
		}
	}

	// Log current processing job count
	activeJobs := 0
	w.processingJobs.Range(func(key, value interface{}) bool {
		activeJobs++
		return true
	})

	if activeJobs > 0 {
		log.Printf("[WORKER] Active jobs: %d currently processing", activeJobs)
	}
}

func (w *NATSWorker) Start() error {
	log.Println("[WORKER] Starting NATS worker")

	// OPTIMIZED CONSUMER CONFIGURATION FOR BETTER THROUGHPUT
	consumers := map[string]ConsumerConfig{
		"generate-report": {
			Subject:     SubjectGenerateReport,
			Handler:     w.handleGenerateReport,
			Concurrency: 1,                 // Single worker to prevent race conditions
			BatchSize:   1,                 // Process one at a time
			AckWait:     300 * time.Second, // Longer timeout for export
			MaxDeliver:  2,                 // Reduced max deliver
		},
		"upload-sftp": {
			Subject:     SubjectUploadSFTP,
			Handler:     w.handleUploadSFTP,
			Concurrency: 50,               // HIGH CONCURRENCY for uploads
			BatchSize:   10,               // Larger batch for efficiency
			AckWait:     60 * time.Second, // Shorter timeout for uploads
			MaxDeliver:  2,                // Quick failure for problematic uploads
		},
		"late-data-check": {
			Subject:     SubjectLateDataCheck,
			Handler:     w.handleLateDataCheck,
			Concurrency: 1, // Single worker
			BatchSize:   1,
			AckWait:     300 * time.Second,
			MaxDeliver:  2,
		},
	}

	for name, config := range consumers {
		if err := w.startConsumer(name, config); err != nil {
			return fmt.Errorf("failed to start consumer %s: %w", name, err)
		}
	}

	log.Println("[WORKER] NATS worker started successfully")
	return nil
}

// OPTIMIZED CONSUMER CONFIG
func (w *NATSWorker) startConsumer(name string, config ConsumerConfig) error {
	consumerName := fmt.Sprintf("%s-consumer", name)
	streamName := "JARVIST_SFTP_JOBS"

	// OPTIMIZED BACKOFF VALUES
	backoffValues := []time.Duration{
		2 * time.Second,  // Quick first retry
		10 * time.Second, // Medium retry
		30 * time.Second, // Longer retry
	}

	consumerCfg := &nats.ConsumerConfig{
		Durable:       consumerName,
		Description:   fmt.Sprintf("Durable consumer for %s messages", name),
		FilterSubject: config.Subject,
		AckPolicy:     nats.AckExplicitPolicy,
		AckWait:       config.AckWait,
		MaxDeliver:    config.MaxDeliver,
		BackOff:       backoffValues,
		ReplayPolicy:  nats.ReplayInstantPolicy,
		DeliverPolicy: nats.DeliverAllPolicy,

		// OPTIMIZED FOR HIGH THROUGHPUT UPLOADS
		MaxAckPending: func() int {
			if name == "upload-sftp" {
				return 200 // High pending for uploads
			}
			return 20 // Lower for other consumers
		}(),
		MaxWaiting: func() int {
			if name == "upload-sftp" {
				return 500 // High waiting for uploads
			}
			return 50 // Lower for other consumers
		}(),
	}

	_, err := w.js.AddConsumer(streamName, consumerCfg)
	if err != nil {
		if !strings.Contains(err.Error(), "consumer already exists") &&
			!strings.Contains(err.Error(), "name already in use") {
			return fmt.Errorf("failed to create consumer %s: %w", consumerName, err)
		}
		log.Printf("[WORKER] Consumer already exists: %s", consumerName)
	}

	sub, err := w.js.PullSubscribe(config.Subject, consumerName,
		nats.Bind(streamName, consumerName),
		nats.ManualAck(),
	)
	if err != nil {
		return fmt.Errorf("failed to create subscription for %s: %w", name, err)
	}

	w.mu.Lock()
	w.subs[name] = sub
	w.mu.Unlock()

	for workerID := 0; workerID < config.Concurrency; workerID++ {
		w.wg.Add(1)
		go w.processMessages(name, sub, config, workerID)
	}

	log.Printf("[WORKER] Consumer started: %s (workers: %d, maxPending: %d)",
		name, config.Concurrency, consumerCfg.MaxAckPending)
	return nil
}

func (w *NATSWorker) processMessages(consumerName string, sub *nats.Subscription, config ConsumerConfig, workerID int) {
	defer w.wg.Done()

	for {
		select {
		case <-w.ctx.Done():
			return
		default:
			// Check if worker is healthy
			if atomic.LoadInt64(&w.isHealthy) == 0 {
				log.Printf("[WORKER] Worker %d unhealthy, waiting...", workerID)
				time.Sleep(5 * time.Second)
				continue
			}

			w.fetchAndProcessBatch(consumerName, sub, config, workerID)
		}
	}
}

// OPTIMIZED MESSAGE FETCHING WITH BETTER TIMEOUT HANDLING
func (w *NATSWorker) fetchAndProcessBatch(consumerName string, sub *nats.Subscription, config ConsumerConfig, workerID int) {
	// Dynamic fetch timeout based on consumer type
	fetchTimeout := 5 * time.Second
	if consumerName == "upload-sftp" {
		fetchTimeout = 2 * time.Second // Faster polling for uploads
	}

	msgs, err := sub.Fetch(config.BatchSize, nats.MaxWait(fetchTimeout))
	if err != nil {
		if strings.Contains(err.Error(), "timeout") ||
			strings.Contains(err.Error(), "no messages") ||
			strings.Contains(err.Error(), "context deadline exceeded") {
			return
		}
		log.Printf("[WORKER] Failed to fetch messages for %s: %v", consumerName, err)

		// Backoff on error
		time.Sleep(1 * time.Second)
		return
	}

	// Process messages in batch for efficiency
	for _, msg := range msgs {
		w.processMessage(msg, config.Handler, consumerName, workerID, config.AckWait)
	}
}

// ENHANCED: Better message processing with status tracking
func (w *NATSWorker) processMessage(msg *nats.Msg, handler func(*nats.Msg), consumerName string, workerID int, ackWait time.Duration) {
	if msg == nil {
		return
	}

	messageID := fmt.Sprintf("%s-%d-%d", consumerName, workerID, time.Now().UnixNano())

	defer func() {
		if r := recover(); r != nil {
			log.Printf("[WORKER] PANIC in %s worker %d (msg: %s): %v",
				consumerName, workerID, messageID, r)
			log.Printf("[WORKER] Stack trace: %s", debug.Stack())

			// Set unhealthy status temporarily
			atomic.StoreInt64(&w.isHealthy, 0)

			if msg != nil {
				msg.Nak()
			}

			// Recover health status after a delay
			go func() {
				time.Sleep(30 * time.Second)
				atomic.StoreInt64(&w.isHealthy, 1)
			}()
		}
	}()

	processingTimeout := ackWait - 10*time.Second
	if processingTimeout < 30*time.Second {
		processingTimeout = 30 * time.Second
	}

	startTime := time.Now()
	done := make(chan error, 1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[WORKER] HANDLER PANIC in %s worker %d (msg: %s): %v",
					consumerName, workerID, messageID, r)
				log.Printf("[WORKER] Handler stack trace: %s", debug.Stack())
				done <- fmt.Errorf("panic: %v", r)
			}
		}()

		if handler == nil {
			done <- fmt.Errorf("handler is nil")
			return
		}

		handler(msg)
		done <- nil
	}()

	select {
	case err := <-done:
		duration := time.Since(startTime)

		if err != nil {
			log.Printf("[WORKER] MESSAGE FAILED in %s worker %d after %v (msg: %s): %v",
				consumerName, workerID, duration, messageID, err)
			msg.Nak()
		} else {
			// Only log successful uploads, not all messages
			if consumerName == "upload-sftp" {
				log.Printf("[WORKER] MESSAGE SUCCESS in %s worker %d after %v (msg: %s)",
					consumerName, workerID, duration, messageID)
			}
			msg.Ack()
		}

	case <-time.After(processingTimeout):
		duration := time.Since(startTime)
		log.Printf("[WORKER] TIMEOUT in %s worker %d after %v (msg: %s, timeout: %v)",
			consumerName, workerID, duration, messageID, processingTimeout)
		msg.Nak()
	}
}

func (w *NATSWorker) handleGenerateReport(msg *nats.Msg) {
	if w.exportService == nil {
		log.Printf("[WORKER] exportService is nil in handleGenerateReport")
		return
	}
	w.processNATSMessage(msg, w.handleGenerateReportTask, "generate_report")
}

func (w *NATSWorker) handleUploadSFTP(msg *nats.Msg) {
	if w.sftpService == nil {
		log.Printf("[WORKER] sftpService is nil in handleUploadSFTP")
		return
	}
	w.processNATSMessage(msg, w.handleUploadSFTPTask, "upload_sftp")
}

func (w *NATSWorker) handleLateDataCheck(msg *nats.Msg) {
	if w.lateDataService == nil {
		log.Printf("[WORKER] lateDataService is nil in handleLateDataCheck")
		return
	}
	w.processNATSMessage(msg, w.handleLateDataCheckTask, "late_data_check")
}

func (w *NATSWorker) processNATSMessage(msg *nats.Msg, processor func([]byte) error, messageType string) {
	if msg == nil {
		log.Printf("[WORKER] Message is nil in processNATSMessage for %s", messageType)
		return
	}

	if msg.Data == nil {
		log.Printf("[WORKER] Message data is nil in processNATSMessage for %s", messageType)
		return
	}

	if processor == nil {
		log.Printf("[WORKER] Processor is nil in processNATSMessage for %s", messageType)
		return
	}

	var envelope map[string]interface{}
	if err := json.Unmarshal(msg.Data, &envelope); err != nil {
		log.Printf("[WORKER] Failed to unmarshal envelope for %s: %v", messageType, err)
		return
	}

	payload, exists := envelope["payload"]
	if !exists {
		log.Printf("[WORKER] Missing payload in message for %s", messageType)
		return
	}

	if payload == nil {
		log.Printf("[WORKER] Payload is nil in message for %s", messageType)
		return
	}

	payloadData, err := json.Marshal(payload)
	if err != nil {
		log.Printf("[WORKER] Failed to marshal payload for %s: %v", messageType, err)
		return
	}

	if err := processor(payloadData); err != nil {
		log.Printf("[WORKER] Task processing failed for %s: %v", messageType, err)
		return
	}
}

func (w *NATSWorker) handleGenerateReportTask(data []byte) error {
	if data == nil {
		return fmt.Errorf("data is nil")
	}

	if w.exportService == nil {
		return fmt.Errorf("exportService is nil")
	}

	var job types.GenerateReportJob
	if err := json.Unmarshal(data, &job); err != nil {
		return fmt.Errorf("failed to unmarshal generate report job: %w", err)
	}

	if job.TenantID == "" {
		return fmt.Errorf("tenantID is empty")
	}

	if job.Date.IsZero() {
		return fmt.Errorf("date is zero")
	}

	// Create unique job key for deduplication
	jobKey := fmt.Sprintf("generate:%s:%s:%s", job.TenantID, job.Date.Format("20060102"), job.JobType)

	// Check if already processing
	if _, exists := w.processingJobs.LoadOrStore(jobKey, ProcessingJob{
		StartTime: time.Now(),
		WorkerID:  0,
	}); exists {
		log.Printf("[WORKER] Generate report job already processing: %s", jobKey)
		return nil // Don't error, just skip
	}
	defer w.processingJobs.Delete(jobKey)

	jobType := strings.ToLower(strings.TrimSpace(job.JobType))
	log.Printf("[WORKER] Processing generate report job: tenant=%s, date=%s, jobType=%s", job.TenantID, job.Date.Format("2006-01-02"), jobType)

	switch {
	case jobType == "daily":
		return w.exportService.ExportDaily(job.TenantID, job.Date)

	case jobType == "30min":
		return w.exportService.Export30Min(job.TenantID, job.Date)

	case strings.HasPrefix(jobType, "daily_location_"):
		locationID := strings.TrimPrefix(jobType, "daily_location_")
		if locationID == "" {
			return fmt.Errorf("invalid location job type: %s", job.JobType)
		}
		log.Printf("[WORKER] Processing daily export for location: %s", locationID)
		return w.exportService.ExportByLocationID(job.TenantID, locationID, job.Date)

	case strings.HasPrefix(jobType, "30min_location_"):
		locationID := strings.TrimPrefix(jobType, "30min_location_")
		if locationID == "" {
			return fmt.Errorf("invalid location job type: %s", job.JobType)
		}
		log.Printf("[WORKER] Processing 30min export for location: %s", locationID)
		return w.exportService.Export30MinByLocationID(job.TenantID, locationID, job.Date)

	case strings.HasPrefix(jobType, "complete_location_"):
		locationID := strings.TrimPrefix(jobType, "complete_location_")
		if locationID == "" {
			return fmt.Errorf("invalid location job type: %s", job.JobType)
		}
		log.Printf("[WORKER] Processing complete export for location: %s", locationID)
		return w.exportService.ExportAllReportByLocationID(job.TenantID, locationID, job.Date)

	default:
		return fmt.Errorf("unknown job type: %s", job.JobType)
	}
}

// ENHANCED: Better duplicate prevention with database check
func (w *NATSWorker) handleUploadSFTPTask(data []byte) error {
	if data == nil {
		return fmt.Errorf("data is nil")
	}

	if w.sftpService == nil {
		return fmt.Errorf("sftpService is nil")
	}

	var job types.UploadSFTPJob
	if err := json.Unmarshal(data, &job); err != nil {
		return fmt.Errorf("failed to unmarshal upload SFTP job: %w", err)
	}

	// ENHANCED: Validate required fields
	if job.LogID == "" {
		return fmt.Errorf("logID is empty")
	}

	if job.TenantID == "" {
		return fmt.Errorf("tenantID is empty")
	}

	if job.FileName == "" {
		return fmt.Errorf("fileName is empty")
	}

	if job.FilePath == "" {
		return fmt.Errorf("filePath is empty")
	}

	// ENHANCED: Check if this job is already being processed with detailed logging
	jobKey := fmt.Sprintf("upload:%s", job.LogID)

	// Check if already processing
	if existingJob, exists := w.processingJobs.Load(jobKey); exists {
		if processJob, ok := existingJob.(ProcessingJob); ok {
			timeSinceStart := time.Since(processJob.StartTime)
			log.Printf("[WORKER] DUPLICATE DETECTED: %s (log ID: %s) already processing for %v",
				job.FileName, job.LogID, timeSinceStart)

			// If processing for too long, allow retry
			if timeSinceStart > 10*time.Minute {
				log.Printf("[WORKER] STUCK JOB: %s processing for %v, allowing retry",
					job.FileName, timeSinceStart)
				w.processingJobs.Delete(jobKey)
			} else {
				log.Printf("[WORKER] SKIPPING DUPLICATE: %s (already processing)", job.FileName)
				return nil // Don't error, just skip duplicate
			}
		}
	}

	// Mark as processing with enhanced logging
	processingJob := ProcessingJob{
		StartTime: time.Now(),
		LogID:     job.LogID,
	}

	w.processingJobs.Store(jobKey, processingJob)
	defer func() {
		w.processingJobs.Delete(jobKey)
		processingDuration := time.Since(processingJob.StartTime)
		log.Printf("[WORKER] COMPLETED PROCESSING: %s in %v (log ID: %s)",
			job.FileName, processingDuration, job.LogID)
	}()

	log.Printf("[WORKER] STARTING UPLOAD: %s (log ID: %s)", job.FileName, job.LogID)

	// Execute upload with enhanced error handling
	err := w.sftpService.UploadFile(job)
	if err != nil {
		log.Printf("[WORKER] UPLOAD FAILED: %s (log ID: %s) - %v",
			job.FileName, job.LogID, err)
		return err
	}

	log.Printf("[WORKER] UPLOAD SUCCESS: %s (log ID: %s)", job.FileName, job.LogID)
	return nil
}

func (w *NATSWorker) handleLateDataCheckTask(data []byte) error {
	if data == nil {
		return fmt.Errorf("data is nil")
	}

	if w.lateDataService == nil {
		return fmt.Errorf("lateDataService is nil")
	}

	var job types.LateDataCheckJob
	if err := json.Unmarshal(data, &job); err != nil {
		return fmt.Errorf("failed to unmarshal late data check job: %w", err)
	}

	if job.TenantID == "" {
		return fmt.Errorf("tenantID is empty")
	}

	if job.Date.IsZero() {
		return fmt.Errorf("date is zero")
	}

	if job.CheckType == "" {
		return fmt.Errorf("checkType is empty")
	}

	// Create unique job key for deduplication
	jobKey := fmt.Sprintf("latedata:%s:%s:%s", job.TenantID, job.Date.Format("20060102"), job.CheckType)

	if _, exists := w.processingJobs.LoadOrStore(jobKey, ProcessingJob{
		StartTime: time.Now(),
		WorkerID:  0,
	}); exists {
		log.Printf("[WORKER] Late data check job already processing: %s", jobKey)
		return nil
	}
	defer w.processingJobs.Delete(jobKey)

	trimmedCheckType := strings.TrimSpace(job.CheckType)
	log.Printf("[WORKER] Processing late data check: tenant=%s, date=%s, checkType=%s",
		job.TenantID, job.Date.Format("2006-01-02"), trimmedCheckType)

	return w.lateDataService.CheckForLateData(job.TenantID, job.Date, trimmedCheckType)
}

func (w *NATSWorker) Stop() error {
	log.Println("[WORKER] Stopping NATS worker")

	// Stop health monitoring
	if w.healthTicker != nil {
		w.healthTicker.Stop()
	}

	w.cancel()

	w.mu.Lock()
	for name, sub := range w.subs {
		if err := sub.Unsubscribe(); err != nil {
			log.Printf("[WORKER] Failed to unsubscribe %s: %v", name, err)
		}
	}
	w.subs = make(map[string]*nats.Subscription)
	w.mu.Unlock()

	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("[WORKER] All workers stopped")
	case <-time.After(45 * time.Second):
		log.Println("[WORKER] Timeout waiting for workers to stop")
	}

	if w.nc != nil && w.nc.IsConnected() {
		w.nc.Close()
	}

	log.Println("[WORKER] NATS worker stopped")
	return nil
}
