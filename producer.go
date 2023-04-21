package easykafka

import (
	"context"
	"encoding/json"
	"github.com/segmentio/kafka-go"
	"time"
)

type ProducerOption[T any] func(kafka *Producer[T]) error

type Producer[T interface{}] struct {
	addresses []string

	groupId        string
	threads        []*Consumer[T]
	preparedTopics []string

	partitions uint
	writer     *kafka.Writer
}

var DefaultWriter = kafka.Writer{
	Balancer:        kafka.CRC32Balancer{},
	WriteBackoffMin: 3 * time.Second,
	WriteBackoffMax: 3 * time.Second,
	BatchTimeout:    10 * time.Second,
}

func NewProducer[T any](
	addresses []string,
	groupId string,
	opts ...ProducerOption[T],
) *Producer[T] {

	k := &Producer[T]{
		addresses:  addresses,
		groupId:    groupId,
		partitions: 3,
		writer:     &DefaultWriter,
	}

	for _, opt := range opts {
		if err := opt(k); err != nil {
			panic(err)
		}
	}

	k.writer.Addr = kafka.TCP(addresses...)
	k.writer.AllowAutoTopicCreation = false

	return k

}

func (p *Producer[T]) Close() error {
	return p.writer.Close()
}

func (p *Producer[T]) Produce(topics []string, messages ...*T) error {

	notPreparedTopics := findMissing(topics, p.preparedTopics)
	err := prepareTopics(p.addresses[0], p.partitions, notPreparedTopics...)
	if err != nil {
		return err
	}
	p.preparedTopics = append(p.preparedTopics, notPreparedTopics...)

	var kms []kafka.Message
	for _, m := range messages {

		b, err := json.Marshal(m)
		if err != nil {
			return err
		}

		for _, topic := range topics {
			kms = append(kms, kafka.Message{Value: b, Topic: topic})
		}
	}

	return p.writer.WriteMessages(context.Background(), kms...)
}
