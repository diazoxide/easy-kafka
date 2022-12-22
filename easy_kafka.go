package EasyKafka

import (
	"encoding/json"
	"errors"
	"github.com/avast/retry-go"
	"github.com/segmentio/kafka-go"
	"golang.org/x/net/context"
	"log"
	"math"
	"net"
	"strconv"
)

type Kafka[T interface{}] struct {
	Logger      *log.Logger
	Addresses   []string
	TopicPrefix string
	GroupId     string
	Threads     []*Kafka[T]

	Partitions uint64
	writer     *kafka.Writer
	reader     *kafka.Reader
	dialer     *kafka.Dialer
}

func New[T interface{}](
	addresses []string,
	topicPrefix string,
	groupId string,
	partitions uint64,
	logger *log.Logger,
) *Kafka[T] {

	k := &Kafka[T]{
		Logger:      logger,
		Addresses:   addresses,
		TopicPrefix: topicPrefix,
		GroupId:     groupId,
		Partitions:  partitions,
	}

	return k

}

func (k *Kafka[T]) prepareTopics(topics ...string) error {
	conn, err := kafka.Dial("tcp", k.Addresses[0])
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		panic(err)
	}
	var controllerConn *kafka.Conn
	controllerConn, err = kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		panic(err)
	}
	defer controllerConn.Close()

	var topicConfigs = make([]kafka.TopicConfig, len(topics))
	for i, t := range topics {
		topicConfigs[i] = kafka.TopicConfig{
			Topic:             t,
			NumPartitions:     int(k.Partitions),
			ReplicationFactor: 1,
		}
	}

	return controllerConn.CreateTopics(topicConfigs...)
}

func (k *Kafka[T]) prepareWriter() {
	if k.writer == nil {
		k.writer = &kafka.Writer{
			Addr:                   kafka.TCP(k.Addresses...),
			Balancer:               kafka.CRC32Balancer{},
			AllowAutoTopicCreation: false,
		}
	}
}

func (k *Kafka[T]) prepareReader(topics []string) {
	if k.reader == nil {
		k.reader = kafka.NewReader(kafka.ReaderConfig{
			Brokers:     k.Addresses,
			GroupID:     k.GroupId,
			GroupTopics: topics,
			MinBytes:    10e6, // 10MB
			MaxBytes:    10e6, // 10MB
		})
	}
}

func (k *Kafka[T]) Produce(topic string, messages ...*T) error {
	topic = k.prepareTopicName(topic)
	err := k.prepareTopics(topic)
	if err != nil {
		return err
	}

	k.prepareWriter()

	var kms []kafka.Message
	for _, m := range messages {

		b, err := json.Marshal(m)
		if err != nil {
			return err
		}

		kms = append(kms, kafka.Message{Value: b, Topic: topic})
	}

	return retry.Do(
		func() error {
			return k.writer.WriteMessages(context.Background(), kms...)
		},
		retry.OnRetry(func(n uint, err error) {
			k.Logger.Println(err.Error())
			k.Logger.Println("Kafka retrying to write message: ", n)
		}),
		retry.Attempts(math.MaxInt8),
	)
}

func (k *Kafka[T]) readMessages(topics []string) (kafka.Message, error) {
	k.prepareReader(topics)

	if k.reader == nil {
		k.Logger.Fatalln(errors.New("reader not initialized"))
	}
	return k.reader.ReadMessage(context.Background())
}

func (k *Kafka[T]) prepareTopicName(topic string) string {
	return k.TopicPrefix + topic
}

func (k *Kafka[T]) prepareTopicNames(topics []string) []string {
	for i, t := range topics {
		topics[i] = k.prepareTopicName(t)
	}
	return topics
}

func (k *Kafka[T]) Consume(topics []string, consumerHandler func(message T) error, async bool) (err error) {
	topics = k.prepareTopicNames(topics)
	err = k.prepareTopics(topics...)

	if err != nil {
		return err
	}

	for {
		m, err := k.readMessages(topics)
		if err != nil {
			return err
		}

		k.Logger.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))

		var message T

		err = json.Unmarshal(m.Value, &message)
		if err != nil {
			return err
		}

		if async {
			go func() {
				err = consumerHandler(message)
				if err != nil {
					k.Logger.Println("Consumer error: ", err)
				}
			}()
		} else {
			err = consumerHandler(message)
			if err != nil {
				k.Logger.Println("Consumer error: ", err)
			}
		}
	}
}

func (k *Kafka[T]) Close() {
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
