package easykafka

import "github.com/segmentio/kafka-go"

// BaseProducerWithWriterConfig sets writer config
func BaseProducerWithWriterConfig(writer *kafka.Writer) BaseProducerOption {
	return func(k *BaseProducer) (err error) {
		k.writer = writer
		return nil
	}
}

// BaseProducerInitialPartitionsCount sets initial partitions count
func BaseProducerInitialPartitionsCount(count uint) BaseProducerOption {
	return func(c *BaseProducer) (err error) {
		c.partitions = count
		return nil
	}
}

// BaseProducerWithLogger sets logger
func BaseProducerWithLogger(logger kafka.Logger) BaseProducerOption {
	return func(c *BaseProducer) (err error) {
		c.LoggerContainer.logger = logger
		return nil
	}
}

// BaseProducerWithErrorLogger sets error logger
func BaseProducerWithErrorLogger(logger kafka.Logger) BaseProducerOption {
	return func(c *BaseProducer) (err error) {
		c.LoggerContainer.errorLogger = logger
		return nil
	}
}
