package easykafka

import (
	"context"
	"encoding/json"
	"github.com/segmentio/kafka-go"
)

// Producer is a wrapper around BaseProducer
type Producer[T any] struct {
	*BaseProducer
}

// InitProducer initializes a new Producer instance
func InitProducer[T any](
	brokers []string,
	opts ...BaseProducerOption,
) (producer *Producer[T], close func() error) {
	baseProducer, closeBaseProducer := InitBaseProducer(brokers, opts...)

	producer = &Producer[T]{
		BaseProducer: baseProducer,
	}

	return producer, closeBaseProducer
}

// Produce sends messages to kafka
func (p *Producer[T]) Produce(ctx context.Context, topics []string, messages ...*T) error {
	var kms []*kafka.Message
	for _, m := range messages {

		b, err := json.Marshal(m)
		if err != nil {
			return err
		}

		for _, topic := range topics {
			kms = append(
				kms,
				&kafka.Message{
					Value: b,
					Topic: topic,
				})
		}
	}

	return p.BaseProducer.Produce(ctx, kms...)
}
