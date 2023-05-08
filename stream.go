package easykafka

import (
	"context"
	"github.com/segmentio/kafka-go"
	"time"
)

// StreamRoutingRule is a function that modifies a message
type StreamRoutingRule[T any] func(message *T, kafkaMessage *kafka.Message) (newMessage *kafka.Message, err error)

// Stream is a wrapper around Consumer and Producer
type Stream[T any] struct {
	LoggerContainer
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

	consumer, closeConsumer := InitConsumer[T](
		brokers,
		topicsList,
		groupId,
		append(
			s.consumerOptions,
			ConsumerWithLogger[T](s.logger),
			ConsumerWithErrorLogger[T](s.logger),
		)...,
	)
	s.consumer = consumer

	producer, closeProducer := InitBaseProducer(
		brokers,
		append(
			s.producerOptions,
			BaseProducerWithLogger(s.logger),
			BaseProducerWithErrorLogger(s.logger),
			BaseProducerWithWriterConfig(&kafka.Writer{
				Balancer:     &kafka.LeastBytes{},
				BatchTimeout: time.Nanosecond,
			}),
		)...,
	)
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
	s.log("starting stream with %d routing rules", len(routingRules))
	s.consumer.Consume(ctx, func(message *T, kafkaMessage *kafka.Message) error {
		start := time.Now()
		// Log message topic and partition
		s.log("received message from topic %s partition %d", kafkaMessage.Topic, kafkaMessage.Partition)

		for _, routingRule := range routingRules {
			newMessage, err := routingRule(message, kafkaMessage)
			if err != nil {
				return err
			}
			if newMessage != nil {
				err := s.producer.Produce(ctx, newMessage)
				if err != nil {
					s.error("error producing message: %s", err.Error())
					return err
				}
			}
		}
		s.log("message processed in %s", time.Since(start))
		return nil
	})
}
