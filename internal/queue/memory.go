package queue

import (
	"context"
	"log"
)

type MemoryQueue struct {
	jobs chan string
}

func NewMemoryQueue(size int) *MemoryQueue {
	if size <= 0 {
		size = 128
	}
	return &MemoryQueue{jobs: make(chan string, size)}
}

func (q *MemoryQueue) Enqueue(ctx context.Context, recordID string) error {
	select {
	case q.jobs <- recordID:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (q *MemoryQueue) Start(ctx context.Context, handler func(context.Context, string) error) {
	for {
		select {
		case <-ctx.Done():
			return
		case recordID := <-q.jobs:
			if err := handler(ctx, recordID); err != nil {
				log.Printf("handle memory delivery message failed id=%s error=%v", recordID, err)
			}
		}
	}
}
