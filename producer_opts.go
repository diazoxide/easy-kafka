package easykafka

import "github.com/segmentio/kafka-go"

func WithWriterConfig[T any](writer *kafka.Writer) ProducerOption[T] {
	return func(k *Producer[T]) (err error) {
		k.writer = writer
		return nil
	}
}

func ProducerInitialPartitionsCount[T any](count uint) ProducerOption[T] {
	return func(c *Producer[T]) (err error) {
		c.partitions = count
		return nil
	}
}
