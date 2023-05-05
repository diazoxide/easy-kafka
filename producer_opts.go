package easykafka

import "github.com/segmentio/kafka-go"

func WithWriterConfig[T any](writer *kafka.Writer) BaseProducerOption {
	return func(k *BaseProducer) (err error) {
		k.writer = writer
		return nil
	}
}

func ProducerInitialPartitionsCount[T any](count uint) BaseProducerOption {
	return func(c *BaseProducer) (err error) {
		c.partitions = count
		return nil
	}
}
