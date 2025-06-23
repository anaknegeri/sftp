package queue

import (
	"encoding/json"
	"fmt"
	"jarvist/sftp-service/internal/config"
	"log"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	SubjectGenerateReport = "jarvist.sftp.generate.report"
	SubjectUploadSFTP     = "jarvist.sftp.upload.sftp"
	SubjectLateDataCheck  = "jarvist.sftp.late.data.check"
)

type NATSQueue struct {
	nc     *nats.Conn
	js     nats.JetStreamContext
	config config.NATSConfig
}

func NewJobQueue(natsURL string) (JobQueue, error) {
	cfg := config.NATSConfig{
		URL:            natsURL,
		StreamName:     "JARVIST_SFTP_JOBS",
		ConnectTimeout: 30,
		ReconnectWait:  2,
		MaxReconnects:  -1,
		MaxAge:         72, // hours
		MaxMessages:    100000,
		MaxBytes:       1024 * 1024 * 1024, // 1GB
	}

	// Connection options
	opts := []nats.Option{
		nats.Name("jarvist-sftp-service"),
		nats.ReconnectWait(time.Duration(cfg.ReconnectWait) * time.Second),
		nats.MaxReconnects(cfg.MaxReconnects),
		nats.Timeout(time.Duration(cfg.ConnectTimeout) * time.Second),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			log.Printf("[QUEUE] NATS disconnected: %v", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Printf("[QUEUE] NATS reconnected to %s", nc.ConnectedUrl())
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			log.Println("[QUEUE] NATS connection closed")
		}),
	}

	// Connect dengan retry
	var nc *nats.Conn
	var err error
	for attempt := 1; attempt <= 5; attempt++ {
		nc, err = nats.Connect(cfg.URL, opts...)
		if err == nil {
			break
		}
		log.Printf("[QUEUE] Connection attempt %d failed: %v", attempt, err)
		time.Sleep(time.Duration(attempt) * time.Second)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	// Create JetStream context
	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to create JetStream: %w", err)
	}

	queue := &NATSQueue{
		nc:     nc,
		js:     js,
		config: cfg,
	}

	// Setup stream
	if err := queue.setupStream(); err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to setup stream: %w", err)
	}

	log.Printf("[QUEUE] NATS queue initialized, stream: %s", cfg.StreamName)
	return queue, nil
}

func (q *NATSQueue) setupStream() error {
	// First, check if there are existing streams that might conflict
	streams := q.js.StreamNames()
	conflictingStreams := []string{}

	for streamName := range streams {
		if streamName != q.config.StreamName {
			streamInfo, err := q.js.StreamInfo(streamName)
			if err == nil {
				for _, subject := range streamInfo.Config.Subjects {
					// Check for overlapping subjects
					if strings.HasPrefix(subject, "jarvist.sftp.") ||
						strings.Contains(subject, "jarvist.sftp.>") ||
						subject == "jarvist.sftp.>" {
						conflictingStreams = append(conflictingStreams, streamName)
						break
					}
				}
			}
		}
	}

	if len(conflictingStreams) > 0 {
		log.Printf("[QUEUE] Found conflicting streams: %v", conflictingStreams)
		log.Printf("[QUEUE] You may need to delete these streams or use different subjects")

		for _, streamName := range conflictingStreams {
			err := q.js.DeleteStream(streamName)
			if err != nil {
				log.Printf("[QUEUE] Failed to delete conflicting stream %s: %v", streamName, err)
			} else {
				log.Printf("[QUEUE] Deleted conflicting stream: %s", streamName)
			}
		}
	}

	// Try to get existing stream first
	existingStream, err := q.js.StreamInfo(q.config.StreamName)
	if err == nil {
		log.Printf("[QUEUE] Stream already exists: %s", q.config.StreamName)

		// Check if subjects match what we expect
		expectedSubjects := []string{
			SubjectGenerateReport,
			SubjectUploadSFTP,
			SubjectLateDataCheck,
		}

		currentSubjects := existingStream.Config.Subjects
		log.Printf("[QUEUE] Current subjects: %v", currentSubjects)
		log.Printf("[QUEUE] Expected subjects: %v", expectedSubjects)

		// If subjects don't match, try to update
		subjectsMatch := len(currentSubjects) == len(expectedSubjects)
		if subjectsMatch {
			for i, subject := range expectedSubjects {
				if i >= len(currentSubjects) || currentSubjects[i] != subject {
					subjectsMatch = false
					break
				}
			}
		}

		if !subjectsMatch {
			log.Printf("[QUEUE] Subjects don't match, attempting to update stream")
			return q.updateStreamSubjects(expectedSubjects)
		}

		return nil
	}

	// Create new stream with specific subjects instead of wildcard
	streamConfig := &nats.StreamConfig{
		Name:        q.config.StreamName,
		Description: "JARVIST SFTP Service Message Stream",
		Subjects: []string{
			SubjectGenerateReport,
			SubjectUploadSFTP,
			SubjectLateDataCheck,
		},
		Retention:  nats.WorkQueuePolicy,
		MaxAge:     time.Duration(q.config.MaxAge) * time.Hour,
		MaxMsgs:    q.config.MaxMessages,
		MaxBytes:   q.config.MaxBytes,
		Storage:    nats.FileStorage,
		Duplicates: 2 * time.Minute,
		Discard:    nats.DiscardOld,
	}

	_, err = q.js.AddStream(streamConfig)
	if err != nil {
		// If still failing due to overlap, provide more specific error information
		if strings.Contains(err.Error(), "subjects overlap") {
			return fmt.Errorf("subjects overlap with existing stream. Current subjects: %v. Error: %w",
				streamConfig.Subjects, err)
		}
		return fmt.Errorf("failed to create stream: %w", err)
	}

	log.Printf("[QUEUE] Created new stream: %s with subjects: %v", q.config.StreamName, streamConfig.Subjects)
	return nil
}

func (q *NATSQueue) updateStreamSubjects(subjects []string) error {
	streamConfig := &nats.StreamConfig{
		Name:        q.config.StreamName,
		Description: "JARVIST SFTP Service Message Stream",
		Subjects:    subjects,
		Retention:   nats.WorkQueuePolicy,
		MaxAge:      time.Duration(q.config.MaxAge) * time.Hour,
		MaxMsgs:     q.config.MaxMessages,
		MaxBytes:    q.config.MaxBytes,
		Storage:     nats.FileStorage,
		Duplicates:  2 * time.Minute,
		Discard:     nats.DiscardOld,
	}

	_, err := q.js.UpdateStream(streamConfig)
	if err != nil {
		return fmt.Errorf("failed to update stream subjects: %w", err)
	}

	log.Printf("[QUEUE] Updated stream: %s with subjects: %v", q.config.StreamName, subjects)
	return nil
}

func (q *NATSQueue) PublishJob(subject string, job interface{}) error {
	// Create message envelope
	message := map[string]interface{}{
		"type":        subject,
		"payload":     job,
		"enqueued_at": time.Now(),
		"id":          fmt.Sprintf("%s_%d", subject, time.Now().UnixNano()),
	}

	data, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	// Convert subject
	natsSubject := q.getSubjectForTaskType(subject)

	// Generate unique message ID
	msgID := fmt.Sprintf("%s_%d_%d",
		subject,
		time.Now().Unix(),
		time.Now().UnixNano())

	// Publish
	_, err = q.js.Publish(natsSubject, data, nats.MsgId(msgID))
	if err != nil {
		return fmt.Errorf("failed to publish to %s: %w", natsSubject, err)
	}

	log.Printf("[QUEUE] Published job: %s", subject)
	return nil
}

func (q *NATSQueue) getSubjectForTaskType(taskType string) string {
	switch taskType {
	case "job.sftp.generate.report":
		return SubjectGenerateReport
	case "job.sftp.upload.sftp":
		return SubjectUploadSFTP
	case "job.sftp.late.data.check":
		return SubjectLateDataCheck
	default:
		log.Printf("[QUEUE] Unknown task type: %s", taskType)
		return "jarvist.sftp.unknown"
	}
}

// Helper method to list and inspect existing streams for debugging
func (q *NATSQueue) ListStreams() error {
	streams := q.js.StreamNames()
	log.Printf("[QUEUE] Listing all streams:")

	for streamName := range streams {
		streamInfo, err := q.js.StreamInfo(streamName)
		if err != nil {
			log.Printf("[QUEUE] Failed to get info for stream %s: %v", streamName, err)
			continue
		}

		log.Printf("[QUEUE] Stream: %s, Subjects: %v", streamName, streamInfo.Config.Subjects)
	}

	return nil
}

func (q *NATSQueue) Close() error {
	log.Println("[QUEUE] Closing NATS queue")

	if q.nc != nil && q.nc.IsConnected() {
		q.nc.Close()
	}

	log.Println("[QUEUE] NATS queue closed")
	return nil
}
