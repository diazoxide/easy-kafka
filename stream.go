package easykafka

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
)

// StreamRoutingRule is a function that modifies a message
type StreamRoutingRule[T any] func(message *T, kafkaMessage *kafka.Message) (newMessage *kafka.Message, err error)

// Stream is a wrapper around Consumer and Producer
type Stream[T any] struct {
	brokers         []string
	groupId         string
	consumer        *Consumer[T]
	producer        *BaseProducer
	topics          []string
	consumerOptions []ConsumerOption[T]
	producerOptions []BaseProducerOption
}

// StreamOption is a function that modifies a Stream instance
type StreamOption[T any] func(stream *Stream[T]) error

// InitStream initializes a new Stream instance
func InitStream[T any](
	brokers []string,
	topicsList []string,
	groupId string,
	opts ...StreamOption[T],
) (*Stream[T], func() error) {
	s := &Stream[T]{
		brokers: brokers,
		groupId: groupId,
	}

	for _, opt := range opts {
		if err := opt(s); err != nil {
			panic(err)
		}
	}

	consumer, closeConsumer := InitConsumer[T](brokers, topicsList, groupId, s.consumerOptions...)
	s.consumer = consumer

	producer, closeProducer := InitBaseProducer(brokers, s.producerOptions...)
	s.producer = producer

	return s, func() error {
		err := closeProducer()
		if err != nil {
			return err
		}
		err = closeConsumer()
		if err != nil {
			return err
		}
		return nil
	}
}

// Run starts the stream
func (s *Stream[T]) Run(ctx context.Context, routingRules ...StreamRoutingRule[T]) {
	s.consumer.Consume(ctx, func(message *T, kafkaMessage *kafka.Message) error {
		for _, routingRule := range routingRules {
			newMessage, err := routingRule(message, kafkaMessage)
			if err != nil {
				return err
			}
			if newMessage != nil {
				err := s.producer.Produce(ctx, newMessage)
				if err != nil {
					fmt.Println("error producing message", err.Error())
					return err
				}
			}
		}
		return nil
	})
}
