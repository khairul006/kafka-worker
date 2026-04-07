package consumer

import (
	"context"
	"encoding/json"
	"kafka-worker/internal/model"
	"kafka-worker/internal/processor"
	"log"

	"github.com/segmentio/kafka-go"
)

type KafkaConsumer struct {
	reader    *kafka.Reader
	processor *processor.TransactionProcessor
}

func NewKafkaConsumer(reader *kafka.Reader, p *processor.TransactionProcessor) *KafkaConsumer {
	return &KafkaConsumer{
		reader:    reader,
		processor: p,
	}
}

func (c *KafkaConsumer) Start() {

	for {
		msg, err := c.reader.ReadMessage(context.Background())
		if err != nil {
			log.Println("kafka read error:", err)
			continue
		}

		log.Printf(
			"Consumed message topic=%s partition=%d offset=%d",
			msg.Topic,
			msg.Partition,
			msg.Offset,
		)

		log.Printf(
			"Consumed message=%s",
			msg.Value,
		)

		var event model.DebeziumEvent

		err = json.Unmarshal(msg.Value, &event)
		if err != nil {
			log.Println("json parse error:", err)
			continue
		}

		err = c.processor.Process(event)
		if err != nil {
			log.Println("process error:", err)
		}
	}
}
