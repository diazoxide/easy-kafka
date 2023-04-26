package easykafka

import (
	"context"
	"encoding/json"
	"github.com/segmentio/kafka-go"
)

type ProducerOption[T any] func(kafka *Producer[T]) error

type Producer[T interface{}] struct {
	brokers        []string
	groupId        string
	threads        []*Consumer[T]
	preparedTopics []string
	partitions     uint
	writer         *kafka.Writer
}

var DefaultWriter = kafka.Writer{
	Balancer: &kafka.LeastBytes{},
}

func InitProducer[T any](
	addresses []string,
	groupId string,
	opts ...ProducerOption[T],
) (producer *Producer[T], close func() error) {
	producer = &Producer[T]{
		brokers:    addresses,
		groupId:    groupId,
		partitions: 3,
		writer:     &DefaultWriter,
	}

	for _, opt := range opts {
		if err := opt(producer); err != nil {
			panic(err)
		}
	}

	producer.writer.Addr = kafka.TCP(addresses...)
	producer.writer.AllowAutoTopicCreation = false

	return producer, producer.writer.Close

}

func (p *Producer[T]) Produce(topics []string, messages ...*T) error {
	notPreparedTopics := findMissing(topics, p.preparedTopics)
	err := prepareTopics(p.brokers[0], p.partitions, notPreparedTopics...)
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
			kms = append(
				kms,
				kafka.Message{
					Value: b,
					Topic: topic,
				})
		}
	}

	return p.writer.WriteMessages(context.Background(), kms...)
}
