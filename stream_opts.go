package easykafka

import "github.com/segmentio/kafka-go"

// StreamWithConsumerOptions sets consumer options
func StreamWithConsumerOptions[T any](opts ...ConsumerOption[T]) StreamOption[T] {
	return func(s *Stream[T]) (err error) {
		s.consumerOptions = opts
		return nil
	}
}

// StreamWithProducerOptions sets producer options
func StreamWithProducerOptions[T any](opts ...BaseProducerOption) StreamOption[T] {
	return func(s *Stream[T]) (err error) {
		s.producerOptions = opts
		return nil
	}
}

// StreamWithLogger sets logger
func StreamWithLogger[T any](logger kafka.Logger) StreamOption[T] {
	return func(c *Stream[T]) (err error) {
		c.logger = logger
		return nil
	}
}

// StreamWithErrorLogger sets error logger
func StreamWithErrorLogger[T any](logger kafka.Logger) StreamOption[T] {
	return func(c *Stream[T]) (err error) {
		c.errorLogger = logger
		return nil
	}
}

// StreamWithParallelJobs sets parallel jobs count for stream consumer
func StreamWithParallelJobs[T any](parallelJobs uint) StreamOption[T] {
	return func(c *Stream[T]) (err error) {
		c.parallelJobs = parallelJobs
		return nil
	}
}
