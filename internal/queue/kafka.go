package queue

import (
	"context"
	"log"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

type KafkaQueue struct {
	brokers []string
	topic   string
	groupID string
	writer  *kafka.Writer
}

func NewKafkaQueue(brokers []string, topic string, groupID string) *KafkaQueue {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
		BatchTimeout: 10 * time.Millisecond,
	}
	return &KafkaQueue{
		brokers: brokers,
		topic:   topic,
		groupID: groupID,
		writer:  writer,
	}
}

func ParseBrokers(raw string) []string {
	parts := strings.Split(raw, ",")
	brokers := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			brokers = append(brokers, part)
		}
	}
	if len(brokers) == 0 {
		return []string{"localhost:9092"}
	}
	return brokers
}

func (q *KafkaQueue) Enqueue(ctx context.Context, recordID string) error {
	return q.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(recordID),
		Value: []byte(recordID),
		Time:  time.Now(),
	})
}

func (q *KafkaQueue) Start(ctx context.Context, handler func(context.Context, string) error) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        q.brokers,
		Topic:          q.topic,
		GroupID:        q.groupID,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: 0,
	})
	defer reader.Close()

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("kafka fetch message failed: %v", err)
			time.Sleep(time.Second)
			continue
		}

		recordID := string(msg.Value)
		if recordID == "" {
			recordID = string(msg.Key)
		}
		if err := handler(ctx, recordID); err != nil {
			log.Printf("handle kafka delivery message failed id=%s error=%v", recordID, err)
			time.Sleep(time.Second)
			continue
		}
		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("commit kafka message failed id=%s error=%v", recordID, err)
		}
	}
}

func (q *KafkaQueue) Close() error {
	return q.writer.Close()
}
