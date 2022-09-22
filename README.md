# Usage

```go
package main

import "github.com/diazoxide/easy-kafka"

func main() {
    k := kafka.NewKafka[messages.PageToScrapMessage](
		b.KafkaUris,
		"pages_to_scrap",
		b.KafkaTopicPrefix,
		b.KafkaGroupId,
		b.KafkaSingleConsumerThreads,
		b.KafkaPartitions,
		b.Logger,
    )
	k.InitConsumer()
}
```