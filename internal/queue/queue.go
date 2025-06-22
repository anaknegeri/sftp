package queue

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

type JobQueue interface {
	PublishJob(subject string, job interface{}) error
	SubscribeJob(subject string, handler JobHandler) error
	Close() error
}

type JobHandler func(data []byte) error

type natsJobQueue struct {
	conn *nats.Conn
	js   nats.JetStreamContext
}

func NewJobQueue(natsURL string) (JobQueue, error) {
	conn, err := nats.Connect(natsURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	js, err := conn.JetStream()
	if err != nil {
		return nil, fmt.Errorf("failed to get JetStream context: %w", err)
	}

	// Create stream for job queue
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "JARVIST_SFTP_JOB",
		Subjects: []string{"job.sftp.*"},
		Storage:  nats.FileStorage,
		MaxAge:   24 * time.Hour,
	})
	if err != nil && err != nats.ErrStreamNameAlreadyInUse {
		return nil, fmt.Errorf("failed to create stream: %w", err)
	}

	return &natsJobQueue{
		conn: conn,
		js:   js,
	}, nil
}

func (q *natsJobQueue) PublishJob(subject string, job interface{}) error {
	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("failed to marshal job: %w", err)
	}

	_, err = q.js.Publish(subject, data)
	if err != nil {
		return fmt.Errorf("failed to publish job: %w", err)
	}

	log.Printf("[QUEUE] Published job to subject: %s", subject)
	return nil
}

func (q *natsJobQueue) SubscribeJob(subject string, handler JobHandler) error {
	_, err := q.js.Subscribe(subject, func(msg *nats.Msg) {
		log.Printf("[QUEUE] Processing job from subject: %s", subject)

		if err := handler(msg.Data); err != nil {
			log.Printf("[QUEUE] Job processing failed: %v", err)
			msg.Nak()
			return
		}

		msg.Ack()
		log.Printf("[QUEUE] Job processed successfully from subject: %s", subject)
	}, nats.Durable("job-processor"))

	if err != nil {
		return fmt.Errorf("failed to subscribe to subject %s: %w", subject, err)
	}

	log.Printf("[QUEUE] Subscribed to subject: %s", subject)
	return nil
}

func (q *natsJobQueue) Close() error {
	q.conn.Close()
	return nil
}
