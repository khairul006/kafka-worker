package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"kafka-worker/internal/processor"
	"time"

	"github.com/segmentio/kafka-go"
)

func StartConsumer(brokers []string, topic, groupID string, txProcessor *processor.TransactionProcessor) {

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  groupID,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})

	ctx := context.Background()
	fmt.Println("Kafka consumer started...")

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			fmt.Println("Error reading message:", err)
			time.Sleep(time.Second) // backoff on error
			continue
		}

		// Parse message
		var tx processor.Transaction
		err = json.Unmarshal(msg.Value, &tx)
		if err != nil {
			fmt.Println("Failed to unmarshal message:", err)
			continue
		}

		// Process transaction
		if err := txProcessor.Process(tx); err != nil {
			fmt.Println("Failed to process transaction:", err)
			continue
		}
	}
}
