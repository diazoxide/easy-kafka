package easykafka

import (
	"github.com/segmentio/kafka-go"
	"net"
	"reflect"
	"sort"
	"strconv"
)

func prepareTopics(controllerConn *kafka.Conn, partitions uint, topics ...string) error {
	var topicConfigs = make([]kafka.TopicConfig, len(topics))
	for i, t := range topics {
		topicConfigs[i] = kafka.TopicConfig{
			Topic:             t,
			NumPartitions:     int(partitions),
			ReplicationFactor: 1,
		}
	}
	return controllerConn.CreateTopics(topicConfigs...)
}

func mustConnect(brokers []string) *kafka.Conn {
	conn, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		panic(err)
	}
	return conn
}

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

func unorderedStringsEqual(a, b []string) bool {
	// Sort both slices
	sort.Strings(a)
	sort.Strings(b)

	// Compare the sorted slices
	return reflect.DeepEqual(a, b)
}
