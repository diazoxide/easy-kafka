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

type Consumer[T any] struct {
	brokers                        []string
	topics                         []string
	concurrency                    uint
	maxBlockingTasks               uint
	groupId                        string
	partitions                     uint
	topicNamesRegexMatch           bool
	readerConfig                   kafka.ReaderConfig
	onWrongMessage                 *ConsumerErrorHandler[T]
	onReadError                    *ConsumerErrorHandler[T]
	onFailCommit                   *ConsumerErrorHandler[T]
	onTopicsListUpdated            *ConsumerTopicsListUpdatedHandler[T]
	threads                        []*Consumer[T]
	oldReader                      *kafka.Reader
	reader                         *kafka.Reader
	conn                           *kafka.Conn
	leaderConn                     *kafka.Conn
	readerLock                     sync.Mutex
	dynamicTopicsDiscovery         bool
	dynamicTopicsDiscoveryInterval time.Duration
	discoveredTopics               []string
}

type ErrorHandler[T any] func(k *Consumer[T], err error)
type ConsumerOption[T any] func(kafka *Consumer[T]) error
type ConsumerHandler[T any] func(message *T, kafkaMessage *kafka.Message) error

// InitConsumer creates a new consumer instance
func InitConsumer[T any](
	brokers []string,
	topicsList []string,
	groupId string,
	opts ...ConsumerOption[T],
) (consumer *Consumer[T], close func() error) {
	var err error

	consumer = &Consumer[T]{
		brokers:                        brokers,
		topics:                         topicsList,
		groupId:                        groupId,
		partitions:                     3,
		concurrency:                    3,
		maxBlockingTasks:               0,
		readerConfig:                   kafka.ReaderConfig{},
		dynamicTopicsDiscoveryInterval: 1000 * time.Millisecond,
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
		if consumer.reader == nil {
			panic("Error: maybe you have wrong topic name or topic not exists")
		}
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
		err := k.reader.Close()
		if err != nil {
			panic(err)
		}
	}
	k.reader = kafka.NewReader(conf)
	k.readerLock.Unlock()
}

// discoveryTopicsAndUpdateReader updates topics list and reader
func (k *Consumer[T]) discoveryTopicsAndUpdateReader(topics []string) {
	var matchingTopics []string

	if k.topicNamesRegexMatch {
		var patterns []*regexp.Regexp
		for _, t := range topics {
			patterns = append(patterns, regexp.MustCompile(t))
		}

		matchingTopics = matchTopicsFromConnectionByRegex(k.leaderConn, patterns...)
	} else {
		matchingTopics = matchTopicsFromConnection(k.leaderConn, topics...)
	}

	if !unorderedStringsEqual(k.discoveredTopics, matchingTopics) {
		k.discoveredTopics = matchingTopics

		if k.onTopicsListUpdated != nil {
			(*k.onTopicsListUpdated)(k, matchingTopics)
		}

		k.updateReader(matchingTopics)
	}
}

// dynamicDiscoveryTopics updates topics list every 3 seconds
func (k *Consumer[T]) dynamicDiscoveryTopics(topics []string) {
	ticker := time.NewTicker(k.dynamicTopicsDiscoveryInterval)
	for range ticker.C {
		k.discoveryTopicsAndUpdateReader(topics)
	}
}

// Consume starts consuming messages from kafka
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
