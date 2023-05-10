package easykafka

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"time"
)

// BaseProducerOption is a function that sets some option on the producer
type BaseProducerOption func(kafka *BaseProducer) error

// BaseProducer is a wrapper around kafka.Writer
type BaseProducer struct {
	LoggerContainer
	brokers        []string
	topicStatusMap map[string]bool
	partitions     uint
	retryDelay     time.Duration
	retriesCount   uint
	writer         *kafka.Writer
	conn           *kafka.Conn
	leaderConn     *kafka.Conn
	replications   uint
}

// InitBaseProducer creates a new producer instance
func InitBaseProducer(
	brokers []string,
	opts ...BaseProducerOption,
) (producer *BaseProducer, close func() error) {
	producer = &BaseProducer{
		brokers:      brokers,
		partitions:   3,
		replications: 1,
		writer: &kafka.Writer{
			Balancer: &kafka.LeastBytes{},
		},
		retryDelay:     time.Millisecond * 500,
		retriesCount:   100,
		topicStatusMap: map[string]bool{},
	}

	for _, opt := range opts {
		if err := opt(producer); err != nil {
			panic(err)
		}
	}

	// Init connection to kafka
	producer.conn = mustConnect(brokers)
	producer.leaderConn = getLeaderConn(producer.conn)

	producer.writer.Addr = kafka.TCP(brokers...)
	producer.writer.AllowAutoTopicCreation = false
	producer.writer.Logger = producer.logger
	producer.writer.ErrorLogger = producer.errorLogger

	go producer.createMissingTopicsPeriodically()

	return producer, func() error {
		err := producer.writer.Close()
		if err != nil {
			return err
		}
		err = producer.conn.Close()
		if err != nil {
			return err
		}
		err = producer.leaderConn.Close()
		if err != nil {
			return err
		}
		return nil
	}
}

// Produce writes messages to kafka
func (p *BaseProducer) Produce(ctx context.Context, messages ...*kafka.Message) error {

	startTime := time.Now()

	topics := scrapTopicsFromMessages(messages)
	p.waitTopics(topics)

	err := p.tryWriteMessages(ctx, p.retriesCount, messages...)
	if err != nil {
		p.log("error while producing messages: %s", err)
	} else {
		p.log("produced %d messages to %d topics in %s", len(messages), len(topics), time.Since(startTime))
	}

	return err
}

// run createTopics every 1 second
func (p *BaseProducer) createMissingTopicsPeriodically() {
	for {
		err := p.createMissingTopics()
		if err != nil {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// create topics from topicsStatusMap where status is false and change status to true
func (p *BaseProducer) createMissingTopics() error {
	for topic, status := range p.topicStatusMap {
		if !status {
			p.log("topic %s not ready, preparing...", topic)
			if !p.topicExists(topic) {
				err := p.createTopic(topic)
				if err != nil {
					p.error("error while creating topic %s: %s", topic, err.Error())
					return err
				}
			}
			p.topicStatusMap[topic] = true
			p.log("topic %s ready", topic)
		}
	}
	return nil
}

// createTopic creates topic with N partition and N replication factor
func (p *BaseProducer) createTopic(topic string) error {
	p.log("creating topic %s", topic)
	return p.conn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     int(p.partitions),
		ReplicationFactor: int(p.replications),
	})
}

// wait when all topics statuses will be true, with timeout
func (p *BaseProducer) waitTopics(topics []string) {
	start := time.Now()
	p.log("waiting topics %v", topics)
	for {
		if p.topicsReady(topics) {
			p.log("topics %v ready in %s", topics, time.Since(start))
			return
		}
		time.Sleep(time.Second)
	}
}

// topicsReady checks if all topics statuses are true
func (p *BaseProducer) topicsReady(topics []string) bool {
	for _, topic := range topics {
		if _, exist := p.topicStatusMap[topic]; !exist {
			p.topicStatusMap[topic] = false
			return false
		}
	}
	return true
}

// topicExists checks if topic exists
func (p *BaseProducer) topicExists(topic string) bool {
	_, err := p.leaderConn.ReadPartitions(topic)
	return err == nil
}

// tryWriteMessages tries to write messages to kafka, if failed, wait retryDelay and try again
func (p *BaseProducer) tryWriteMessages(
	ctx context.Context,
	retryCount uint,
	messages ...*kafka.Message,
) error {
	start := time.Now()
	if retryCount == 0 {
		return fmt.Errorf("failed to write messages")
	}

	p.log("try to write messages, retry count: %d", retryCount)

	err := p.writer.WriteMessages(ctx, convertSlice(messages)...)
	if err != nil {
		time.Sleep(p.retryDelay)
		return p.tryWriteMessages(ctx, retryCount-1, messages...)
	} else {
		p.log("messages written in %s", time.Since(start))
	}

	return nil
}
