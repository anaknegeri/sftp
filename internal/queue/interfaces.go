package queue

// JobQueue interface - tidak ada dependency ke service
type JobQueue interface {
	PublishJob(subject string, job interface{}) error
	Close() error
}

type JobHandler func(data []byte) error
