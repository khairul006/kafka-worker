package consumer

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

func StartConsumer() {

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "transactions",
		GroupID: "trx-group",
	})

	for {

		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			panic(err)
		}

		fmt.Println("Message:", string(msg.Value))
	}
}
