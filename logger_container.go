package easykafka

import (
	"github.com/segmentio/kafka-go"
)

type LoggerContainer struct {
	logger      kafka.Logger
	errorLogger kafka.Logger
}

// log writes to logger
func (l *LoggerContainer) log(str string, args ...interface{}) {
	if l.logger != nil {
		l.logger.Printf(str, args...)
	}
}

// error writes to error logger
func (l *LoggerContainer) error(str string, args ...interface{}) {
	if l.errorLogger != nil {
		l.errorLogger.Printf(str, args...)
	}
}
