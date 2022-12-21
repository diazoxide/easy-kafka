# Usage

```go
package main

import (
	easyKafka "github.com/diazoxide/easy-kafka"
	"log"
	"os"
)

type MyMessageType struct{
	From    string `json:"from"`
	Content string `json:"content"`
}


func main() {
	logger := log.New(os.Stdout, "test", log.Ltime)
	k := easyKafka.New[MyMessageType](
		[]string{ "kafka:9092" }, // KafkaUris,
		"my-prefix",
		"my-group",
		12,
		logger,
	)
	k.Produce( ... )
	k.Consume( ... )
	...
}
```

# Available methods

## Consume
## Produce