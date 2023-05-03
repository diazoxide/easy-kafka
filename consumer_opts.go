package easykafka

import "github.com/segmentio/kafka-go"

func ConsumerWithWrongMessageHandler[T any](handler ConsumerErrorHandler[T]) ConsumerOption[T] {
	return func(k *Consumer[T]) (err error) {
		k.onWrongMessage = &handler
		return nil
	}
}

func ConsumerWithReadMessageHandler[T any](handler ConsumerErrorHandler[T]) ConsumerOption[T] {
	return func(k *Consumer[T]) (err error) {
		k.onReadError = &handler
		return nil
	}
}

func ConsumerWithTopicsListUpdatedHandler[T any](handler ConsumerTopicsListUpdatedHandler[T]) ConsumerOption[T] {
	return func(k *Consumer[T]) (err error) {
		k.onTopicsListUpdated = &handler
		return nil
	}
}

func ConsumerWithOnFailCommitHandler[T any](handler ConsumerErrorHandler[T]) ConsumerOption[T] {
	return func(k *Consumer[T]) (err error) {
		k.onFailCommit = &handler
		return nil
	}
}

func ConsumerWithReaderConfig[T any](config kafka.ReaderConfig) ConsumerOption[T] {
	return func(k *Consumer[T]) (err error) {
		k.readerConfig = config
		return nil
	}
}

func ConsumerWithMaxBlockingTasks[T any](count uint) ConsumerOption[T] {
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

func ConsumerDynamicTopicsDiscovery[T any]() ConsumerOption[T] {
	return func(c *Consumer[T]) (err error) {
		c.dynamicTopicsDiscovery = true
		return nil
	}
}
