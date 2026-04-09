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
	"time"

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
		// Buffer size should be large enough to keep workers busy
		messageChan: make(chan kafka.Message, workerCount*100),
	}
}

func (c *KafkaConsumer) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle Graceful Shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	var wg sync.WaitGroup

	// 1. Start the Worker Pool
	for i := 1; i <= c.workerCount; i++ {
		wg.Add(1)
		go c.worker(ctx, &wg, i)
	}
	log.Printf("Started %d batch workers", c.workerCount)

	// 2. Main Reader Loop
	go func() {
		for {
			// Using FetchMessage instead of ReadMessage to allow manual commits
			// after the database batch write is successful.
			msg, err := c.reader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return // Context cancelled
				}
				log.Printf("Kafka fetch error: %v", err)
				continue
			}

			select {
			case c.messageChan <- msg:
			case <-ctx.Done():
				return
			}
		}
	}()

	// 3. Block until OS signal
	<-sigChan
	log.Println("Shutdown signal received. Cleaning up...")
	cancel() // This triggers the ctx.Done() in all workers

	wg.Wait()
	log.Println("All workers finished. Closing Kafka reader...")
	if err := c.reader.Close(); err != nil {
		log.Printf("Error closing reader: %v", err)
	}
}

func (c *KafkaConsumer) worker(ctx context.Context, wg *sync.WaitGroup, id int) {
	defer wg.Done()

	const (
		batchSize     = 10
		flushInterval = 10 * time.Second
	)

	batch := make([]model.DebeziumEvent, 0, batchSize)
	messages := make([]kafka.Message, 0, batchSize)

	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}

		// Start overall flush timer
		startFlush := time.Now()

		// 1. Database Timing
		startDB := time.Now()
		err := c.processor.ProcessBatch(ctx, batch)
		dbDuration := time.Since(startDB)

		if err != nil {
			log.Printf("[Worker %d] FATAL DB ERROR: %v", id, err)
			return
		}

		// 2. Kafka Commit Timing
		startCommit := time.Now()
		if err := c.reader.CommitMessages(ctx, messages...); err != nil {
			log.Printf("[Worker %d] Kafka commit error: %v", id, err)
		}
		commitDuration := time.Since(startCommit)

		log.Printf("[Worker %d] FLUSH COMPLETE: count=%d | db=%v | commit=%v | total=%v",
			id, len(batch), dbDuration, commitDuration, time.Since(startFlush))

		// Clear buffers
		batch = batch[:0]
		messages = messages[:0]
		ticker.Reset(flushInterval)
	}

	for {
		// Timing how long we wait for a message from the channel
		startWait := time.Now()

		select {
		case msg := <-c.messageChan:
			waitDuration := time.Since(startWait)

			// 3. JSON Unmarshal Timing
			startJson := time.Now()
			var event model.DebeziumEvent
			err := json.Unmarshal(msg.Value, &event)
			jsonDuration := time.Since(startJson)

			if err != nil {
				log.Printf("[Worker %d] JSON error: %v", id, err)
				_ = c.reader.CommitMessages(ctx, msg)
				continue
			}

			// Only log individual message timing if it's unusually slow (> 10ms)
			if jsonDuration > 10*time.Millisecond || waitDuration > 100*time.Millisecond {
				log.Printf("[Worker %d] Slow Message: wait=%v | json=%v", id, waitDuration, jsonDuration)
			}

			batch = append(batch, event)
			messages = append(messages, msg)

			if len(batch) >= batchSize {
				flush()
			}

		case <-ticker.C:
			flush()

		case <-ctx.Done():
			flush()
			return
		}
	}
}
