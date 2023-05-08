package easykafka

import "github.com/segmentio/kafka-go"

func StreamWithConsumerOptions[T any](opts ...ConsumerOption[T]) StreamOption[T] {
	return func(s *Stream[T]) (err error) {
		s.consumerOptions = opts
		return nil
	}
}

func StreamWithProducerOptions[T any](opts ...BaseProducerOption) StreamOption[T] {
	return func(s *Stream[T]) (err error) {
		s.producerOptions = opts
		return nil
	}
}

func StreamWithLogger[T any](logger kafka.Logger) StreamOption[T] {
	return func(c *Stream[T]) (err error) {
		c.logger = logger
		return nil
	}
}

func StreamWithErrorLogger[T any](logger kafka.Logger) StreamOption[T] {
	return func(c *Stream[T]) (err error) {
		c.errorLogger = logger
		return nil
	}
}
