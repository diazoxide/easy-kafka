package easykafka

import (
	"context"
	"encoding/json"
	"github.com/avast/retry-go"
	"github.com/segmentio/kafka-go"
)

type ProducerOption[T any] func(kafka *Producer[T]) error
type ProducerRetryHandler[T any] func(k *Producer[T], n uint, err error)

type Producer[T interface{}] struct {
	addresses []string

	topicPrefix    string
	groupId        string
	threads        []*Consumer[T]
	maxAttempts    uint
	preparedTopics []string

	partitions uint
	writer     *kafka.Writer

	OnRetry *ProducerRetryHandler[T]
}

func NewProducer[T any](
	addresses []string,
	groupId string,
	opts ...ProducerOption[T],
) *Producer[T] {

	k := &Producer[T]{
		addresses:   addresses,
		groupId:     groupId,
		partitions:  3,
		maxAttempts: 10,
		writer: &kafka.Writer{
			Balancer: kafka.CRC32Balancer{},
		},
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
			topic = prepareTopicName(p.topicPrefix, topic)
			kms = append(kms, kafka.Message{Value: b, Topic: topic})
		}
	}

	return retry.Do(
		func() error {
			return p.writer.WriteMessages(context.Background(), kms...)
		},
		retry.OnRetry(func(n uint, err error) {
			if p.OnRetry != nil {
				(*p.OnRetry)(p, n, err)
			}
		}),
		retry.Attempts(p.maxAttempts),
	)
}
