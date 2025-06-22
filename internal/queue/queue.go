package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type JobQueue interface {
	PublishJob(subject string, job interface{}) error
	SubscribeJob(subject string, handler JobHandler) error
	Close() error
}

type JobHandler func(data []byte) error

type natsJobQueue struct {
	conn *nats.Conn
	js   jetstream.JetStream
}

// Constants yang tepat sesuai referensi
const (
	StreamName   = "SFTP_JOBS"
	SubjectJobs  = "sftp.jobs.*"
	ConsumerName = "job-worker"
)

func NewJobQueue(natsURL string) (JobQueue, error) {
	log.Printf("[QUEUE] Connecting to NATS at %s", natsURL)

	// Simple connection seperti referensi
	conn, err := nats.Connect(natsURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	log.Printf("[QUEUE] Connected to NATS: %s", conn.ConnectedUrl())

	// Create JetStream context
	js, err := jetstream.New(conn)
	if err != nil {
		return nil, fmt.Errorf("failed to create JetStream: %w", err)
	}

	queue := &natsJobQueue{
		conn: conn,
		js:   js,
	}

	// Setup stream dengan handling conflict
	if err := queue.setupStream(); err != nil {
		return nil, fmt.Errorf("failed to setup stream: %w", err)
	}

	log.Printf("[QUEUE] NATS queue initialized successfully")
	return queue, nil
}

func (q *natsJobQueue) setupStream() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	log.Printf("[QUEUE] Setting up stream: %s", StreamName)

	// Check if our stream already exists with correct config
	if stream, err := q.js.Stream(ctx, StreamName); err == nil {
		if info, err := stream.Info(ctx); err == nil {
			log.Printf("[QUEUE] Stream %s already exists with subjects: %v",
				StreamName, info.Config.Subjects)
			return nil
		}
	}

	// Create stream dengan config minimal seperti referensi
	streamConfig := jetstream.StreamConfig{
		Name:     StreamName,
		Subjects: []string{SubjectJobs},
		Storage:  jetstream.FileStorage,
		MaxAge:   24 * time.Hour,
		MaxMsgs:  1000,
	}

	_, err := q.js.CreateOrUpdateStream(ctx, streamConfig)
	if err != nil {
		return fmt.Errorf("failed to create stream: %w", err)
	}

	log.Printf("[QUEUE] Stream %s created successfully", StreamName)
	return nil
}

func (q *natsJobQueue) PublishJob(subject string, job interface{}) error {
	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("failed to marshal job: %w", err)
	}

	// Convert subject format
	natsSubject := q.convertToNATSSubject(subject)

	log.Printf("[QUEUE] Publishing job to subject: %s", natsSubject)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ack, err := q.js.Publish(ctx, natsSubject, data)
	if err != nil {
		return fmt.Errorf("failed to publish job: %w", err)
	}

	log.Printf("[QUEUE] Job published successfully, sequence: %d", ack.Sequence)
	return nil
}

func (q *natsJobQueue) SubscribeJob(subject string, handler JobHandler) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create consumer dengan pattern seperti referensi
	consumerConfig := jetstream.ConsumerConfig{
		Name:          ConsumerName,
		FilterSubject: SubjectJobs, // Listen to all jobs
		AckPolicy:     jetstream.AckExplicitPolicy,
		MaxDeliver:    3,
		AckWait:       10 * time.Minute,
	}

	consumer, err := q.js.CreateOrUpdateConsumer(ctx, StreamName, consumerConfig)
	if err != nil {
		return fmt.Errorf("failed to create consumer: %w", err)
	}

	// Convert expected subject untuk filtering
	expectedSubject := q.convertToNATSSubject(subject)

	// Start consuming
	_, err = consumer.Consume(func(msg jetstream.Msg) {
		msgSubject := msg.Subject()

		// Filter messages berdasarkan subject yang diinginkan
		if expectedSubject == "sftp.jobs.*" || msgSubject == expectedSubject {
			log.Printf("[QUEUE] Processing job from subject: %s", msgSubject)

			if err := handler(msg.Data()); err != nil {
				log.Printf("[QUEUE] Job processing failed: %v", err)
				msg.Nak()
				return
			}

			msg.Ack()
			log.Printf("[QUEUE] Job processed successfully")
		} else {
			// Skip dan ack message yang tidak sesuai filter
			msg.Ack()
		}
	})

	if err != nil {
		return fmt.Errorf("failed to start consuming: %w", err)
	}

	log.Printf("[QUEUE] Subscribed to jobs for subject pattern: %s", expectedSubject)
	return nil
}

func (q *natsJobQueue) convertToNATSSubject(oldSubject string) string {
	switch oldSubject {
	case "job.sftp.generate.report":
		return "sftp.jobs.generate_report"
	case "job.sftp.upload.sftp":
		return "sftp.jobs.upload_sftp"
	default:
		// Generic conversion
		parts := strings.Split(oldSubject, ".")
		if len(parts) >= 3 {
			jobType := strings.Join(parts[2:], "_")
			return fmt.Sprintf("sftp.jobs.%s", jobType)
		}
		return fmt.Sprintf("sftp.jobs.%s", strings.ReplaceAll(oldSubject, ".", "_"))
	}
}

func (q *natsJobQueue) Close() error {
	if q.conn != nil {
		q.conn.Close()
		log.Println("[QUEUE] NATS connection closed")
	}
	return nil
}
