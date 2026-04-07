package processor

import (
	"fmt"
	"kafka-worker/internal/model"
	"kafka-worker/internal/repository"
)

type TransactionProcessor struct {
	repo *repository.PostgresRepo
}

func NewTransactionProcessor(repo *repository.PostgresRepo) *TransactionProcessor {
	return &TransactionProcessor{repo: repo}
}

func (p *TransactionProcessor) Process(event model.DebeziumEvent) error {
	// Handle create, update, and read (snapshot) operations
	if event.Op == "c" || event.Op == "u" {
		id := fmt.Sprintf("%v", event.After.ID)
		exitPlaza := event.After.ExitPlaza
		entryPlaza := event.After.EntryPlaza
		moneyValue := event.After.MoneyValue

		fmt.Printf("syncing transaction %s (op=%s)\n", id, event.Op)

		return p.repo.UpsertTransaction(id, exitPlaza, entryPlaza, moneyValue)
	}

	return nil
}
