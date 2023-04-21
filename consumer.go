package easykafka

import (
	"encoding/json"
	"github.com/panjf2000/ants/v2"
	"github.com/segmentio/kafka-go"
	"golang.org/x/net/context"
)

type ConsumerErrorHandler[T any] func(k *Consumer[T], err error)

type Consumer[T interface{}] struct {
	addresses   []string
	topics      []string
	concurrency uint

	groupId      string
	partitions   uint
	readerConfig kafka.ReaderConfig

	onWrongMessage *ConsumerErrorHandler[T]
	onReadError    *ConsumerErrorHandler[T]
	threads        []*Consumer[T]
	reader         *kafka.Reader
}

type ErrorHandler[T any] func(k *Consumer[T], err error)
type ConsumerOption[T any] func(kafka *Consumer[T]) error
type ConsumerHandler[T any] func(message *T, kafkaMessage *kafka.Message)

func NewConsumer[T any](
	addresses []string,
	topics []string,
	groupId string,
	opts ...ConsumerOption[T],
) *Consumer[T] {

	k := &Consumer[T]{
		addresses:    addresses,
		topics:       topics,
		groupId:      groupId,
		partitions:   3,
		concurrency:  3,
		readerConfig: kafka.ReaderConfig{},
	}

	for _, opt := range opts {
		if err := opt(k); err != nil {
			panic(err)
		}
	}

	k.readerConfig.Brokers = k.addresses
	k.readerConfig.GroupID = k.groupId
	k.readerConfig.GroupTopics = topics
	err := prepareTopics(k.addresses[0], k.partitions, topics...)
	if err != nil {
		panic(err)
	}
	k.reader = kafka.NewReader(k.readerConfig)

	return k

}

func (k *Consumer[T]) readMessages() (kafka.Message, error) {

	if k.reader == nil {
		panic("reader not initialized")
	}
	return k.reader.ReadMessage(context.Background())
}

func (k *Consumer[T]) Consume(handler ConsumerHandler[T]) {
	pool, _ := ants.NewPool(int(k.concurrency))
	defer pool.Release()

	for {
		m, err := k.readMessages()
		if err != nil {
			if k.onReadError != nil {
				(*k.onReadError)(k, err)
			}
			continue
		}

		var message T

		err = json.Unmarshal(m.Value, &message)
		if err != nil {
			if k.onWrongMessage != nil {
				(*k.onWrongMessage)(k, err)
			}
			continue
		}

		_ = pool.Submit(func() {
			handler(&message, &m)
		})

	}
}

func (k *Consumer[T]) Close() error {
	return k.reader.Close()
}
