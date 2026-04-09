package main

import (
	"fmt"
	"kafka-worker/internal/consumer"
	"kafka-worker/internal/logger"
	"kafka-worker/internal/processor"
	"kafka-worker/internal/repository"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
)

func main() {

	logger.InitLogger()
	defer logger.Log.Sync() // Flushes buffer to file before exit

	logger.Info("Application starting...")

	// Load .env
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using system env")
	}

	// Kafka config
	kafkaBrokers := strings.Split(os.Getenv("KAFKA_BROKERS"), ",")
	kafkaTopic := os.Getenv("KAFKA_TOPIC")
	kafkaGroupID := os.Getenv("KAFKA_GROUP_ID")

	// Postgres config
	pgHost := os.Getenv("POSTGRES_HOST")

	portStr := os.Getenv("POSTGRES_PORT")
	pgPort, err := strconv.Atoi(portStr)
	if err != nil {
		log.Fatalf("Invalid POSTGRES_PORT: %v", err)
	}

	pgUser := os.Getenv("POSTGRES_USER")
	pgPassword := os.Getenv("POSTGRES_PASSWORD")
	pgDB := os.Getenv("POSTGRES_DB")

	connStr := fmt.Sprintf(
		"postgresql://%s:%s@%s:%d/%s?sslmode=disable",
		pgUser,
		pgPassword,
		pgHost,
		pgPort,
		pgDB,
	)

	// Repository
	repo, err := repository.NewPostgresRepo(connStr)
	if err != nil {
		logger.Fatal("Failed to connect to Postgres:", err)
	}
	defer repo.Close()

	// Processor
	txProcessor := processor.NewTransactionProcessor(repo)

	// Consumer
	kafkaReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: kafkaBrokers,
		Topic:   kafkaTopic,
		GroupID: kafkaGroupID,
	})

	workerCount := 5
	if wc := os.Getenv("WORKER_COUNT"); wc != "" {
		if parsed, err := strconv.Atoi(wc); err == nil {
			workerCount = parsed
		}
	}

	kafkaConsumer := consumer.NewKafkaConsumer(kafkaReader, txProcessor, workerCount)

	logger.Info("Kafka worker started successfully", workerCount)

	kafkaConsumer.Start()
}
