package easykafka

import (
	"github.com/segmentio/kafka-go"
	"time"
)

// ConsumerWithLogger sets logger
func ConsumerWithLogger[T any](logger kafka.Logger) ConsumerOption[T] {
	return func(c *Consumer[T]) (err error) {
		c.LoggerContainer.logger = logger
		return nil
	}
}

// ConsumerWithErrorLogger sets error logger
func ConsumerWithErrorLogger[T any](logger kafka.Logger) ConsumerOption[T] {
	return func(c *Consumer[T]) (err error) {
		c.LoggerContainer.errorLogger = logger
		return nil
	}
}

// ConsumerWithWrongMessageHandler sets handler for wrong message
func ConsumerWithWrongMessageHandler[T any](handler ConsumerErrorHandler[T]) ConsumerOption[T] {
	return func(k *Consumer[T]) (err error) {
		k.onWrongMessage = &handler
		return nil
	}
}

// ConsumerWithReadMessageHandler sets handler for read message
func ConsumerWithReadMessageHandler[T any](handler ConsumerErrorHandler[T]) ConsumerOption[T] {
	return func(k *Consumer[T]) (err error) {
		k.onReadError = &handler
		return nil
	}
}

// ConsumerWithTopicsListUpdatedHandler sets handler for topics list update
func ConsumerWithTopicsListUpdatedHandler[T any](handler ConsumerTopicsListUpdatedHandler[T]) ConsumerOption[T] {
	return func(k *Consumer[T]) (err error) {
		k.onTopicsListUpdated = &handler
		return nil
	}
}

// ConsumerWithOnFailCommitHandler sets handler for commit error
func ConsumerWithOnFailCommitHandler[T any](handler ConsumerErrorHandler[T]) ConsumerOption[T] {
	return func(k *Consumer[T]) (err error) {
		k.onFailCommit = &handler
		return nil
	}
}

// ConsumerWithReaderConfig sets reader config
func ConsumerWithReaderConfig[T any](config kafka.ReaderConfig) ConsumerOption[T] {
	return func(k *Consumer[T]) (err error) {
		k.readerConfig = config
		return nil
	}
}

// ConsumerWithMaxBlockingTasks sets max blocking tasks
func ConsumerWithMaxBlockingTasks[T any](count uint) ConsumerOption[T] {
	return func(k *Consumer[T]) (err error) {
		k.maxBlockingTasks = count
		return nil
	}
}

// ConsumerInitialPartitionsCount sets initial partitions count
func ConsumerInitialPartitionsCount[T any](count uint) ConsumerOption[T] {
	return func(c *Consumer[T]) (err error) {
		c.partitions = count
		return nil
	}
}

// ConsumerConcurrency sets parallel tasks count
func ConsumerConcurrency[T any](concurrency uint) ConsumerOption[T] {
	return func(c *Consumer[T]) (err error) {
		c.concurrency = concurrency
		return nil
	}
}

// ConsumerDynamicTopicsDiscovery enable dynamic topics discovery
func ConsumerDynamicTopicsDiscovery[T any]() ConsumerOption[T] {
	return func(c *Consumer[T]) (err error) {
		c.dynamicTopicsDiscovery = true
		return nil
	}
}

// ConsumerDynamicTopicsDiscoveryInterval sets dynamic topics discovery interval
func ConsumerDynamicTopicsDiscoveryInterval[T any](interval time.Duration) ConsumerOption[T] {
	return func(c *Consumer[T]) (err error) {
		c.dynamicTopicsDiscoveryInterval = interval
		return nil
	}
}

// ConsumerTopicNamesRegexMatch enable regex match for topic names
func ConsumerTopicNamesRegexMatch[T any]() ConsumerOption[T] {
	return func(c *Consumer[T]) (err error) {
		c.topicNamesRegexMatch = true
		return nil
	}
}

// ConsumerTopicNamesExactMatch enable exact match for topic names
func ConsumerTopicNamesExactMatch[T any]() ConsumerOption[T] {
	return func(c *Consumer[T]) (err error) {
		c.topicNamesRegexMatch = false
		return nil
	}
}
