package easykafka

import "github.com/segmentio/kafka-go"

func WithReaderConfig[T any](config kafka.ReaderConfig) ConsumerOption[T] {
	return func(k *Consumer[T]) (err error) {
		k.readerConfig = config
		return nil
	}
}

func ConsumerInitialPartitionsCount[T any](count uint) ConsumerOption[T] {
	return func(c *Consumer[T]) (err error) {
		c.partitions = count
		return nil
	}
}

func ProducerInitialPartitionsCount[T any](count uint) ProducerOption[T] {
	return func(c *Producer[T]) (err error) {
		c.partitions = count
		return nil
	}
}

func ProducerMaxAttempts[T any](attempts uint) ProducerOption[T] {
	return func(c *Producer[T]) (err error) {
		c.maxAttempts = attempts
		return nil
	}
}

func ProducerOnRetry[T any](fn ProducerRetryHandler[T]) ProducerOption[T] {
	return func(c *Producer[T]) (err error) {
		c.OnRetry = &fn
		return nil
	}
}
