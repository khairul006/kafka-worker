package main

import (
	"fmt"
	"kafka-worker/internal/consumer"
	"kafka-worker/internal/processor"
	"kafka-worker/internal/repository"
	"log"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

func main() {
	// Load .env file
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using environment variables")
	}

	// Kafka config
	kafkaBrokers := []string{os.Getenv("KAFKA_BROKERS")}
	kafkaTopic := os.Getenv("KAFKA_TOPIC")
	kafkaGroupID := os.Getenv("KAFKA_GROUP_ID")

	// Postgres config
	pgHost := os.Getenv("POSTGRES_HOST")
	pgPort, _ := strconv.Atoi(os.Getenv("POSTGRES_PORT"))
	pgUser := os.Getenv("POSTGRES_USER")
	pgPassword := os.Getenv("POSTGRES_PASSWORD")
	pgDB := os.Getenv("POSTGRES_DB")

	connStr := fmt.Sprintf("postgresql://%s:%s@%s:%d/%s", pgUser, pgPassword, pgHost, pgPort, pgDB)

	// Initialize Postgres repo
	repo, err := repository.NewPostgresRepo(connStr)
	if err != nil {
		log.Fatal("Failed to connect to Postgres:", err)
	}
	defer repo.Close()

	// Initialize processor
	txProcessor := processor.NewTransactionProcessor(repo)

	// Start Kafka consumer
	consumer.StartConsumer(kafkaBrokers, kafkaTopic, kafkaGroupID, txProcessor)
}
