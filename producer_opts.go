package easykafka

import "github.com/segmentio/kafka-go"

func BaseProducerWithWriterConfig(writer *kafka.Writer) BaseProducerOption {
	return func(k *BaseProducer) (err error) {
		k.writer = writer
		return nil
	}
}

func BaseProducerInitialPartitionsCount(count uint) BaseProducerOption {
	return func(c *BaseProducer) (err error) {
		c.partitions = count
		return nil
	}
}
