# Usage

```go
package main

import (
	easyKafka "github.com/diazoxide/easy-kafka"
	"log"
	"os"
)

type MyMessageType struct {
	From    string `json:"from"`
	Content string `json:"content"`
}

func main() {
	logger := log.New(os.Stdout, "test", log.Ltime)
	k := easyKafka.New[MyMessageType](
		[]string{"kafka:9092"}, // KafkaUris,
		"my-prefix", // Topics Prefix
		"my-group", // Consumer Group
		12, // Partitions
		logger, // Logger
	)
	k.Produce( "my-topic", &MyMessageType{"test","test"} )
	k.Consume([]string{"my-topic"}, func(message MyMessageType) error{
		
		// Your code here
		
		return nil
    }, true)
	// ...
}
```

# Available methods

## Consume
## Produce