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
	conn           *kafka.Conn
	controllerConn *kafka.Conn
}

var DefaultWriter = kafka.Writer{
	Balancer: &kafka.LeastBytes{},
}

func InitProducer[T any](
	brokers []string,
	groupId string,
	opts ...ProducerOption[T],
) (producer *Producer[T], close func() error) {
	producer = &Producer[T]{
		brokers:    brokers,
		groupId:    groupId,
		partitions: 3,
		writer:     &DefaultWriter,
	}

	for _, opt := range opts {
		if err := opt(producer); err != nil {
			panic(err)
		}
	}

	// Init conn
	producer.conn = mustConnect(brokers)
	producer.controllerConn = getLeaderConn(producer.conn)

	producer.writer.Addr = kafka.TCP(brokers...)
	producer.writer.AllowAutoTopicCreation = false

	return producer, func() error {
		err := producer.writer.Close()
		if err != nil {
			return err
		}
		err = producer.conn.Close()
		if err != nil {
			return err
		}
		return err
	}
}

func (p *Producer[T]) Produce(topics []string, messages ...*T) error {
	notPreparedTopics := findMissing(topics, p.preparedTopics)
	err := prepareTopics(p.controllerConn, p.partitions, notPreparedTopics...)
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
