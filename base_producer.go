package easykafka

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"time"
)

type BaseProducerOption func(kafka *BaseProducer) error

type BaseProducer struct {
	brokers        []string
	preparedTopics []string
	partitions     uint
	retryDelay     time.Duration
	retriesCount   uint
	writer         *kafka.Writer
	conn           *kafka.Conn
	controllerConn *kafka.Conn
}

func InitBaseProducer(
	brokers []string,
	opts ...BaseProducerOption,
) (producer *BaseProducer, close func() error) {
	producer = &BaseProducer{
		brokers:      brokers,
		partitions:   3,
		writer:       &DefaultWriter,
		retryDelay:   time.Millisecond * 100,
		retriesCount: 100,
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

func (p *BaseProducer) Produce(ctx context.Context, messages ...*kafka.Message) error {
	topics := scrapTopicsFromMessages(messages)
	err := p.prepareTopics(topics...)
	if err != nil {
		return err
	}

	return p.tryWriteMessages(ctx, p.retriesCount, messages...)
}

func (p *BaseProducer) prepareTopics(topics ...string) error {
	notPreparedTopics := findMissing(topics, p.preparedTopics)
	var topicConfigs = make([]kafka.TopicConfig, len(topics))
	for i, t := range topics {
		topicConfigs[i] = kafka.TopicConfig{
			Topic:             t,
			NumPartitions:     int(p.partitions),
			ReplicationFactor: 1,
		}
	}
	err := p.controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		return err
	}

	p.preparedTopics = appendIfMissing(p.preparedTopics, notPreparedTopics...)

	return nil
}

func (p *BaseProducer) tryWriteMessages(
	ctx context.Context,
	retryCount uint,
	messages ...*kafka.Message,
) error {
	if retryCount == 0 {
		return fmt.Errorf("failed to write messages")
	}

	err := p.writer.WriteMessages(ctx, convertSlice(messages)...)
	if err != nil {
		time.Sleep(p.retryDelay)
		return p.tryWriteMessages(ctx, retryCount-1, messages...)
	}

	return nil
}
