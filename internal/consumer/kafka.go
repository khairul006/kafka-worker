package consumer

import (
	"context"
	"encoding/json"
	"kafka-worker/internal/model"
	"kafka-worker/internal/processor"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/segmentio/kafka-go"
)

type KafkaConsumer struct {
	reader      *kafka.Reader
	processor   *processor.TransactionProcessor
	workerCount int
	messageChan chan kafka.Message
}

func NewKafkaConsumer(reader *kafka.Reader, p *processor.TransactionProcessor, workerCount int) *KafkaConsumer {
	return &KafkaConsumer{
		reader:      reader,
		processor:   p,
		workerCount: workerCount,
		messageChan: make(chan kafka.Message, workerCount*10),
	}
}

func (c *KafkaConsumer) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Graceful shutdown on SIGINT/SIGTERM
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start worker pool
	var wg sync.WaitGroup
	for i := 0; i < c.workerCount; i++ {
		wg.Add(1)
		go c.worker(ctx, &wg, i)
	}
	log.Printf("Started %d workers", c.workerCount)

	// Message reader goroutine
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
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

				select {
				case c.messageChan <- msg:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	// Wait for shutdown signal
	go func() {
		sig := <-sigChan
		log.Printf("Received signal %v, shutting down...", sig)
		cancel()
	}()

	// Wait for all workers to finish
	wg.Wait()
	log.Println("All workers stopped")

	if err := c.reader.Close(); err != nil {
		log.Println("error closing reader:", err)
	}
}

func (c *KafkaConsumer) worker(ctx context.Context, wg *sync.WaitGroup, id int) {
	defer wg.Done()
	log.Printf("Worker %d started", id)

	for {
		select {
		case msg := <-c.messageChan:
			var event model.DebeziumEvent

			err := json.Unmarshal(msg.Value, &event)
			if err != nil {
				log.Printf("Worker %d: json parse error: %v", id, err)
				continue
			}

			err = c.processor.Process(event)
			if err != nil {
				log.Printf("Worker %d: process error: %v", id, err)
			}

		case <-ctx.Done():
			log.Printf("Worker %d stopping", id)
			return
		}
	}
}
