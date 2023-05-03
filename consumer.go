package easykafka

import (
	"encoding/json"
	"github.com/panjf2000/ants/v2"
	"github.com/segmentio/kafka-go"
	"golang.org/x/net/context"
	"io"
	"regexp"
	"sync"
	"time"
)

type ConsumerErrorHandler[T any] func(k *Consumer[T], err error)
type ConsumerTopicsListUpdatedHandler[T any] func(k *Consumer[T], topics []string)

type Consumer[T interface{}] struct {
	brokers                []string
	topics                 []string
	concurrency            uint
	maxBlockingTasks       uint
	groupId                string
	partitions             uint
	readerConfig           kafka.ReaderConfig
	onWrongMessage         *ConsumerErrorHandler[T]
	onReadError            *ConsumerErrorHandler[T]
	onFailCommit           *ConsumerErrorHandler[T]
	onTopicsListUpdated    *ConsumerTopicsListUpdatedHandler[T]
	threads                []*Consumer[T]
	oldReader              *kafka.Reader
	reader                 *kafka.Reader
	conn                   *kafka.Conn
	leaderConn             *kafka.Conn
	readerLock             sync.Mutex
	dynamicTopicsDiscovery bool
	discoveredTopics       []string
}

type ErrorHandler[T any] func(k *Consumer[T], err error)
type ConsumerOption[T any] func(kafka *Consumer[T]) error
type ConsumerHandler[T any] func(message *T, kafkaMessage *kafka.Message) error

func InitConsumer[T any](
	brokers []string,
	topicsList []string,
	groupId string,
	opts ...ConsumerOption[T],
) (consumer *Consumer[T], close func() error) {
	var err error

	consumer = &Consumer[T]{
		brokers:          brokers,
		topics:           topicsList,
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

	// Init conn
	consumer.conn = mustConnect(brokers)
	consumer.leaderConn = getLeaderConn(consumer.conn)

	// Reader configuration
	consumer.readerConfig.Brokers = consumer.brokers
	consumer.readerConfig.GroupID = consumer.groupId

	// Can't reuse connection :(
	//consumer.readerConfig.Dialer = &kafka.Dialer{
	//	DialFunc: func(ctx context.Context, network string, addr string) (net.Conn, error) {
	//		return consumer.conn, nil
	//	},
	//}

	if consumer.dynamicTopicsDiscovery {
		go consumer.dynamicDiscoveryTopics(topicsList)
	} else {
		consumer.discoveryTopicsAndUpdateReader(topicsList)
	}

	return consumer, func() error {
		err = consumer.reader.Close()
		if err != nil {
			return err
		}

		err = consumer.conn.Close()
		if err != nil {
			return err
		}
		return nil
	}
}

func (k *Consumer[T]) updateReader(topics []string) {
	conf := k.readerConfig
	conf.GroupTopics = topics
	k.readerLock.Lock()
	if k.reader != nil {
		k.reader.Close()
	}
	k.reader = kafka.NewReader(conf)
	k.readerLock.Unlock()
}

func (k *Consumer[T]) discoveryTopicsAndUpdateReader(topics []string) {
	var patterns []*regexp.Regexp
	for _, t := range topics {
		patterns = append(patterns, regexp.MustCompile(t))
	}

	partitions, err := k.leaderConn.ReadPartitions()
	if err != nil {
		panic(err)
	}

	var matchingTopics []string
	topicSet := make(map[string]struct{})
	for _, partition := range partitions {
		for _, re := range patterns {
			if re.MatchString(partition.Topic) {
				if _, ok := topicSet[partition.Topic]; !ok {
					topicSet[partition.Topic] = struct{}{}
					matchingTopics = append(matchingTopics, partition.Topic)
				}
			}
		}
	}

	if !unorderedStringsEqual(k.discoveredTopics, matchingTopics) {
		k.discoveredTopics = matchingTopics

		if k.onTopicsListUpdated != nil {
			(*k.onTopicsListUpdated)(k, matchingTopics)
		}

		k.updateReader(matchingTopics)
	}
}

func (k *Consumer[T]) dynamicDiscoveryTopics(topics []string) {
	ticker := time.NewTicker(3 * time.Second)
	for range ticker.C {
		k.discoveryTopicsAndUpdateReader(topics)
	}
}

// Consume messages.
// When the handler returns an error, the consumer does not commit the message
func (k *Consumer[T]) Consume(ctx context.Context, handler ConsumerHandler[T]) {
	pool, _ := ants.NewPool(int(k.concurrency),
		ants.WithMaxBlockingTasks(int(k.maxBlockingTasks)),
	)
	defer pool.Release()

	for {
		k.readerLock.Lock()
		r := k.reader
		k.readerLock.Unlock()
		if r == nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		m, err := r.FetchMessage(ctx)
		if err != nil {
			if k.onReadError != nil {
				(*k.onReadError)(k, err)
				continue
			}
			if err == io.EOF {
				continue
			}
			panic(err)
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
			err = handler(&message, &m)
			if err == nil {
				err = k.reader.CommitMessages(ctx, m)
				if err != nil {
					if k.onFailCommit != nil {
						(*k.onFailCommit)(k, err)
					}
				}
			}
		})

		if err != nil {
			panic(err)
		}
	}
}
