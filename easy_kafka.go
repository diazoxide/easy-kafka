package easy_kafka

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
	"sync"
)

type Kafka[T interface{}] struct {
	Logger           *log.Logger
	Addresses        []string
	TopicPrefix      string
	GroupId          string
	ThreadsCount     int
	Threads          []*Kafka[T]
	Topic            string
	RetryTopicSuffix string

	Partitions  uint64
	writer      *kafka.Writer
	emailReader *kafka.Reader
	dialer      *kafka.Dialer
}

func NewKafka[T interface{}](
	addresses []string,
	topic string,
	topicPrefix string,
	groupId string,
	threadsCount int,
	partitions uint64,
	logger *log.Logger,
) *Kafka[T] {
	k := &Kafka[T]{
		Logger:           logger,
		Addresses:        addresses,
		TopicPrefix:      topicPrefix,
		GroupId:          groupId,
		ThreadsCount:     threadsCount,
		Partitions:       partitions,
		Topic:            topic,
		RetryTopicSuffix: "retry",
	}

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

	var topicConfigs = []kafka.TopicConfig{
		{
			Topic:             k.getMainTopic(),
			NumPartitions:     int(k.Partitions),
			ReplicationFactor: 1,
		},
		{
			Topic:             k.getRetryTopic(),
			NumPartitions:     int(k.Partitions),
			ReplicationFactor: 1,
		},
	}

	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		panic(err.Error())
	}

	return k

}

func (k *Kafka[T]) getMainTopic() string {
	return k.TopicPrefix + "_" + k.Topic
}

func (k *Kafka[T]) getRetryTopic() string {
	return k.getMainTopic() + "_" + k.RetryTopicSuffix
}

func (k *Kafka[T]) InitConsumer(consumerHandler func(message T) error) (err error) {
	wg := sync.WaitGroup{}
	wg.Add(k.ThreadsCount)

	for i := 0; i < k.ThreadsCount; i += 1 {
		go func(i int) {
			defer wg.Done()

			t := NewKafka[T](k.Addresses, k.Topic, k.TopicPrefix, k.GroupId, k.ThreadsCount, k.Partitions, k.Logger)

			k.Threads = append(k.Threads, t)

			err = retry.Do(
				func() error {
					err := k.consume(consumerHandler)
					return err
				},
				retry.Attempts(math.MaxInt8),
				retry.OnRetry(func(n uint, err error) {
					k.Logger.Println("Retry " + strconv.Itoa(int(n+1)) + " " + err.Error())
				}),
			)

			if err != nil {
				k.Logger.Panicln(err)
			}
			t.Close()

			if err != nil {
				return
			}
		}(i)
	}

	k.Logger.Println("Consumers count: " + strconv.Itoa(k.ThreadsCount))

	wg.Wait()

	return err
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

func (k *Kafka[T]) prepareReader() {
	if k.emailReader == nil {
		k.emailReader = kafka.NewReader(kafka.ReaderConfig{
			Brokers:     k.Addresses,
			GroupID:     k.GroupId,
			GroupTopics: []string{k.getMainTopic(), k.getRetryTopic()},
			//Topic: "^" + mainTopic + "(_retry_.*)|$",
			MinBytes: 10e3, // 10KB
			MaxBytes: 10e6, // 10MB
		})
	}
}

func (k *Kafka[T]) Produce(isRetry bool, messages ...*T) error {
	k.prepareWriter()

	var kms []kafka.Message
	for _, m := range messages {

		b, err := json.Marshal(m)
		if err != nil {
			return err
		}

		topic := k.getMainTopic()
		if isRetry {
			topic = k.getRetryTopic()
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

func (k *Kafka[T]) readMessages() (kafka.Message, error) {
	k.prepareReader()
	if k.emailReader == nil {
		k.Logger.Fatalln(errors.New("reader not initialized"))
	}
	return k.emailReader.ReadMessage(context.Background())
}

func (k *Kafka[T]) consume(consumerHandler func(message T) error) (err error) {
	for {
		m, err := k.readMessages()
		if err != nil {
			return err
		}

		k.Logger.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))

		var message T

		err = json.Unmarshal(m.Value, &message)
		if err != nil {
			return err
		}

		err = consumerHandler(message)
		if err != nil {
			k.Logger.Println("Consumer error: ", err)
		}
	}
}

func (k *Kafka[T]) Close() {
	if k.emailReader != nil {
		err := k.emailReader.Close()
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
