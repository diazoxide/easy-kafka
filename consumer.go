package easykafka

import (
	"encoding/json"
	"github.com/panjf2000/ants/v2"
	"github.com/segmentio/kafka-go"
	"golang.org/x/net/context"
)

type ConsumerErrorHandler[T any] func(k *Consumer[T], err error)

type Consumer[T interface{}] struct {
	brokers          []string
	topics           []string
	concurrency      uint
	maxBlockingTasks uint
	groupId          string
	partitions       uint
	readerConfig     kafka.ReaderConfig
	onWrongMessage   *ConsumerErrorHandler[T]
	onReadError      *ConsumerErrorHandler[T]
	threads          []*Consumer[T]
	reader           *kafka.Reader
}

type ErrorHandler[T any] func(k *Consumer[T], err error)
type ConsumerOption[T any] func(kafka *Consumer[T]) error
type ConsumerHandler[T any] func(message *T, kafkaMessage *kafka.Message)

func InitConsumer[T any](
	addresses []string,
	topics []string,
	groupId string,
	opts ...ConsumerOption[T],
) (consumer *Consumer[T], close func() error) {
	consumer = &Consumer[T]{
		brokers:          addresses,
		topics:           topics,
		groupId:          groupId,
		partitions:       3,
		concurrency:      3,
		maxBlockingTasks: 0,
		readerConfig:     kafka.ReaderConfig{},
	}

	for _, opt := range opts {
		if err := opt(consumer); err != nil {
			panic(err)
		}
	}

	consumer.readerConfig.Brokers = consumer.brokers
	consumer.readerConfig.GroupID = consumer.groupId
	consumer.readerConfig.GroupTopics = topics
	err := prepareTopics(consumer.brokers[0], consumer.partitions, topics...)
	if err != nil {
		panic(err)
	}
	consumer.reader = kafka.NewReader(consumer.readerConfig)

	return consumer, consumer.reader.Close
}

func (k *Consumer[T]) readMessages() (kafka.Message, error) {
	if k.reader == nil {
		panic("reader not initialized")
	}
	return k.reader.ReadMessage(context.Background())
}

func (k *Consumer[T]) Consume(handler ConsumerHandler[T]) {
	pool, _ := ants.NewPool(int(k.concurrency),
		ants.WithMaxBlockingTasks(int(k.maxBlockingTasks)),
	)
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

		err = pool.Submit(func() {
			handler(&message, &m)
		})

		if err != nil {
			panic(err)
		}
	}
}
