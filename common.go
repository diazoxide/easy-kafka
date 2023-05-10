package easykafka

import (
	"github.com/segmentio/kafka-go"
	"net"
	"regexp"
	"strconv"
)

// strP returns pointer to string
func strP(s string) *string {
	return &s
}

// intP returns pointer to int
func intP(i int) *int {
	return &i
}

// mustConnect connects to kafka
func mustConnect(brokers []string) *kafka.Conn {
	conn, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		panic(err)
	}
	return conn
}

// getLeaderConn returns connection to leader
func getLeaderConn(conn *kafka.Conn) *kafka.Conn {
	controller, err := conn.Controller()
	if err != nil {
		panic(err)
	}
	var controllerConn *kafka.Conn
	controllerConn, err = kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		panic(err)
	}
	return controllerConn
}

// matchTopicsFromConnectionByRegex matches topics from partitions
func matchTopicsFromConnectionByRegex(conn *kafka.Conn, patterns ...*regexp.Regexp) []string {
	partitions, err := conn.ReadPartitions()
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
	return matchingTopics
}

// matchTopicsFromConnection matches topics from partitions
func matchTopicsFromConnection(conn *kafka.Conn, topics ...string) []string {
	partitions, err := conn.ReadPartitions()
	if err != nil {
		panic(err)
	}
	var matchingTopics []string
	topicSet := make(map[string]struct{})
	for _, partition := range partitions {
		for _, t := range topics {
			if partition.Topic == t {
				if _, ok := topicSet[partition.Topic]; !ok {
					topicSet[partition.Topic] = struct{}{}
					matchingTopics = append(matchingTopics, partition.Topic)
				}
			}
		}
	}
	return matchingTopics
}

// scrapTopicsFromMessages scraps topics from messages
func scrapTopicsFromMessages(messages []*kafka.Message) []string {
	var topics []string
	for _, m := range messages {
		var found bool
		for _, t := range topics {
			if t == m.Topic {
				found = true
			}
		}
		if found {
			continue
		}
		topics = append(topics, m.Topic)
	}

	return topics
}
