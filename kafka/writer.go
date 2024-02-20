package kafka

import (
	"context"
	"encoding/json"
	"github.com/segmentio/kafka-go"
)

type Writer interface {
	WriteBatch(ctx context.Context, messages ...any) error
}

type writer[T any] struct {
	writer *kafka.Writer
}

func NewWriter[T any]() *writer[T] {
	w := &kafka.Writer{
		Addr:                   kafka.TCP("localhost:9092"),
		Topic:                  "drs_responses",
		AllowAutoTopicCreation: true,
		Balancer:               &kafka.LeastBytes{},
	}
	return &writer[T]{writer: w}
}

func (w *writer[T]) WriteBatch(ctx context.Context, messages ...T) error {
	kafkaMessages := make([]kafka.Message, len(messages))
	for i, item := range messages {
		b, _ := json.Marshal(item)
		kafkaMessages[i] = kafka.Message{
			Value: b,
		}
	}
	return w.writer.WriteMessages(ctx, kafkaMessages...)
}
