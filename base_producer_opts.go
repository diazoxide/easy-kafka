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

func BaseProducerWithLogger(logger kafka.Logger) BaseProducerOption {
	return func(c *BaseProducer) (err error) {
		c.LoggerContainer.logger = logger
		return nil
	}
}

func BaseProducerWithErrorLogger(logger kafka.Logger) BaseProducerOption {
	return func(c *BaseProducer) (err error) {
		c.LoggerContainer.errorLogger = logger
		return nil
	}
}
