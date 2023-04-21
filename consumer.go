package easykafka

import (
	"encoding/json"
	"github.com/segmentio/kafka-go"
	"golang.org/x/net/context"
)

type Consumer[T interface{}] struct {
	addresses []string
	topics    []string

	groupId      string
	partitions   uint
	readerConfig kafka.ReaderConfig

	threads []*Consumer[T]
	writer  *kafka.Writer
	reader  *kafka.Reader
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

func (k *Consumer[T]) Consume(handler ConsumerHandler[T], async bool) (err error) {

	if err != nil {
		return err
	}

	for {
		m, err := k.readMessages()
		if err != nil {
			return err
		}

		var message T

		err = json.Unmarshal(m.Value, &message)
		if err != nil {
			return err
		}

		if async {
			go func() {
				handler(&message, &m)
			}()
		} else {
			handler(&message, &m)
		}
	}
}

func (k *Consumer[T]) Close() {
	if k.reader != nil {
		err := k.reader.Close()
		if err != nil {
			panic(err)
		}
	}

	if k.writer != nil {
		err := k.writer.Close()
		if err != nil {
			panic(err)
		}
	}
}
