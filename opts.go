package easykafka

import "github.com/segmentio/kafka-go"

func WithReaderConfig[T any](config kafka.ReaderConfig) ConsumerOption[T] {
	return func(k *Consumer[T]) (err error) {
		k.readerConfig = config
		return nil
	}
}

func WithMaxBlockingTasks[T any](count uint) ConsumerOption[T] {
	return func(k *Consumer[T]) (err error) {
		k.maxBlockingTasks = count
		return nil
	}
}

func ConsumerInitialPartitionsCount[T any](count uint) ConsumerOption[T] {
	return func(c *Consumer[T]) (err error) {
		c.partitions = count
		return nil
	}
}
func ConsumerConcurrency[T any](concurrency uint) ConsumerOption[T] {
	return func(c *Consumer[T]) (err error) {
		c.concurrency = concurrency
		return nil
	}
}

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
