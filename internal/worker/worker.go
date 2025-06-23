package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"runtime/debug"
	"strings"
	"sync"
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
	// Validate services are not nil
	if exportService == nil {
		return nil, fmt.Errorf("exportService cannot be nil")
	}
	if sftpService == nil {
		return nil, fmt.Errorf("sftpService cannot be nil")
	}
	if lateDataService == nil {
		return nil, fmt.Errorf("lateDataService cannot be nil")
	}

	// Connection options
	opts := []nats.Option{
		nats.Name("jarvist-sftp-worker"),
		nats.ReconnectWait(2 * time.Second),
		nats.MaxReconnects(-1),
		nats.Timeout(30 * time.Second),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			log.Printf("[WORKER] NATS disconnected: %v", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Printf("[WORKER] NATS reconnected to %s", nc.ConnectedUrl())
		}),
	}

	// Connect
	nc, err := nats.Connect(natsURL, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	// Create JetStream context
	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to create JetStream: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &NATSWorker{
		nc:              nc,
		js:              js,
		exportService:   exportService,
		sftpService:     sftpService,
		lateDataService: lateDataService,
		subs:            make(map[string]*nats.Subscription),
		ctx:             ctx,
		cancel:          cancel,
	}, nil
}

func (w *NATSWorker) Start() error {
	log.Println("[WORKER] Starting NATS worker")

	// Define consumers
	consumers := map[string]ConsumerConfig{
		"generate-report": {
			Subject:     SubjectGenerateReport,
			Handler:     w.handleGenerateReport,
			Concurrency: 3,
			BatchSize:   2,
			AckWait:     180 * time.Second,
			MaxDeliver:  5,
		},
		"upload-sftp": {
			Subject:     SubjectUploadSFTP,
			Handler:     w.handleUploadSFTP,
			Concurrency: 25,
			BatchSize:   10,
			AckWait:     300 * time.Second,
			MaxDeliver:  3,
		},
		"late-data-check": {
			Subject:     SubjectLateDataCheck,
			Handler:     w.handleLateDataCheck,
			Concurrency: 2,
			BatchSize:   1,
			AckWait:     180 * time.Second,
			MaxDeliver:  4,
		},
	}

	// Start all consumers
	for name, config := range consumers {
		if err := w.startConsumer(name, config); err != nil {
			return fmt.Errorf("failed to start consumer %s: %w", name, err)
		}
	}

	log.Println("[WORKER] NATS worker started successfully")
	return nil
}

func (w *NATSWorker) startConsumer(name string, config ConsumerConfig) error {
	consumerName := fmt.Sprintf("%s-consumer", name)
	streamName := "JARVIST_SFTP_JOBS"

	backoffValues := []time.Duration{
		2 * time.Second,
		10 * time.Second,
		30 * time.Second,
	}

	if config.MaxDeliver <= len(backoffValues) {
		config.MaxDeliver = len(backoffValues) + 1
	}

	// Create consumer config
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
		MaxAckPending: 200,
		MaxWaiting:    1000,
	}

	// Add consumer
	_, err := w.js.AddConsumer(streamName, consumerCfg)
	if err != nil {
		if !strings.Contains(err.Error(), "consumer already exists") &&
			!strings.Contains(err.Error(), "name already in use") {
			return fmt.Errorf("failed to create consumer %s: %w", consumerName, err)
		}
		log.Printf("[WORKER] Consumer already exists: %s", consumerName)
	}

	// Create pull subscription
	sub, err := w.js.PullSubscribe(config.Subject, consumerName,
		nats.Bind(streamName, consumerName),
		nats.ManualAck(),
	)
	if err != nil {
		return fmt.Errorf("failed to create subscription for %s: %w", name, err)
	}

	// Store subscription
	w.mu.Lock()
	w.subs[name] = sub
	w.mu.Unlock()

	// Start workers
	for workerID := 0; workerID < config.Concurrency; workerID++ {
		w.wg.Add(1)
		go w.processMessages(name, sub, config, workerID)
	}

	log.Printf("[WORKER] Consumer started: %s (workers: %d)", name, config.Concurrency)
	return nil
}

func (w *NATSWorker) processMessages(consumerName string, sub *nats.Subscription, config ConsumerConfig, workerID int) {
	defer w.wg.Done()

	for {
		select {
		case <-w.ctx.Done():
			return
		default:
			w.fetchAndProcessBatch(consumerName, sub, config, workerID)
		}
	}
}

func (w *NATSWorker) fetchAndProcessBatch(consumerName string, sub *nats.Subscription, config ConsumerConfig, workerID int) {
	msgs, err := sub.Fetch(config.BatchSize, nats.MaxWait(5*time.Second))
	if err != nil {
		if strings.Contains(err.Error(), "timeout") ||
			strings.Contains(err.Error(), "no messages") ||
			strings.Contains(err.Error(), "context deadline exceeded") {
			return
		}
		log.Printf("[WORKER] Failed to fetch messages: %v", err)
		time.Sleep(time.Second)
		return
	}

	for _, msg := range msgs {
		w.processMessage(msg, config.Handler, consumerName, workerID, config.AckWait)
	}
}

func (w *NATSWorker) processMessage(msg *nats.Msg, handler func(*nats.Msg), consumerName string, workerID int, ackWait time.Duration) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[WORKER] Message processing panicked in %s worker %d: %v", consumerName, workerID, r)
			log.Printf("[WORKER] Stack trace: %s", debug.Stack())

			// Log message details for debugging
			if msg != nil {
				log.Printf("[WORKER] Failed message subject: %s", msg.Subject)
				log.Printf("[WORKER] Failed message data: %s", string(msg.Data))
			}

			if msg != nil {
				msg.Nak()
			}
		}
	}()

	done := make(chan error, 1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[WORKER] Handler panicked in %s worker %d: %v", consumerName, workerID, r)
				log.Printf("[WORKER] Handler stack trace: %s", debug.Stack())
				done <- fmt.Errorf("panic: %v", r)
			}
		}()

		// Additional nil check
		if handler == nil {
			done <- fmt.Errorf("handler is nil")
			return
		}

		if msg == nil {
			done <- fmt.Errorf("message is nil")
			return
		}

		handler(msg)
		done <- nil
	}()

	select {
	case err := <-done:
		if err != nil {
			log.Printf("[WORKER] Message processing failed in %s worker %d: %v", consumerName, workerID, err)
			if msg != nil {
				msg.Nak()
			}
		} else {
			if msg != nil {
				msg.Ack()
			}
		}

	case <-time.After(ackWait):
		log.Printf("[WORKER] Message processing timeout in %s worker %d", consumerName, workerID)
		if msg != nil {
			msg.Nak()
		}
	}
}

// Message handlers with improved error handling
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
	// Add nil checks
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
		log.Printf("[WORKER] Raw message data: %s", string(msg.Data))
		return
	}

	payload, exists := envelope["payload"]
	if !exists {
		log.Printf("[WORKER] Missing payload in message for %s", messageType)
		log.Printf("[WORKER] Envelope content: %+v", envelope)
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

// Task processors with improved error handling
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

	// Validate job data
	if job.TenantID == "" {
		return fmt.Errorf("tenantID is empty")
	}

	if job.Date.IsZero() {
		return fmt.Errorf("date is zero")
	}

	switch strings.ToLower(strings.TrimSpace(job.JobType)) {
	case "daily":
		return w.exportService.ExportDaily(job.TenantID, job.Date)
	case "30min":
		return w.exportService.Export30Min(job.TenantID, job.Date)
	default:
		return fmt.Errorf("unknown job type: %s", job.JobType)
	}
}

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

	// Validate job data
	if job.TenantID == "" {
		return fmt.Errorf("tenantID is empty")
	}

	if job.FileName == "" {
		return fmt.Errorf("fileName is empty")
	}

	if job.FilePath == "" {
		return fmt.Errorf("filePath is empty")
	}

	return w.sftpService.UploadFile(job)
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

	// Validate job data
	if job.TenantID == "" {
		return fmt.Errorf("tenantID is empty")
	}

	if job.Date.IsZero() {
		return fmt.Errorf("date is zero")
	}

	if job.CheckType == "" {
		return fmt.Errorf("checkType is empty")
	}

	trimmedCheckType := strings.TrimSpace(job.CheckType)
	log.Printf("[WORKER] Processing late data check: tenant=%s, date=%s, checkType=%s",
		job.TenantID, job.Date.Format("2006-01-02"), trimmedCheckType)

	return w.lateDataService.CheckForLateData(job.TenantID, job.Date, trimmedCheckType)
}

func (w *NATSWorker) Stop() error {
	log.Println("[WORKER] Stopping NATS worker")

	w.cancel()

	// Unsubscribe
	w.mu.Lock()
	for name, sub := range w.subs {
		if err := sub.Unsubscribe(); err != nil {
			log.Printf("[WORKER] Failed to unsubscribe %s: %v", name, err)
		}
	}
	w.subs = make(map[string]*nats.Subscription)
	w.mu.Unlock()

	// Wait for workers
	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("[WORKER] All workers stopped")
	case <-time.After(30 * time.Second):
		log.Println("[WORKER] Timeout waiting for workers")
	}

	// Close connection
	if w.nc != nil && w.nc.IsConnected() {
		w.nc.Close()
	}

	log.Println("[WORKER] NATS worker stopped")
	return nil
}
