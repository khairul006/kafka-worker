package processor

import (
	"fmt"
	"kafka-worker/internal/repository"
)

type Transaction struct {
	ID     string
	Amount float64
}

type TransactionProcessor struct {
	repo *repository.PostgresRepo
}

func NewTransactionProcessor(repo *repository.PostgresRepo) *TransactionProcessor {
	return &TransactionProcessor{repo: repo}
}

func (p *TransactionProcessor) Process(tx Transaction) error {
	// Example: do any business logic here
	fmt.Printf("Processing transaction: %+v\n", tx)
	return p.repo.UpsertTransaction(tx.ID, tx.Amount)
}
