package processor

import (
	"context"
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

// Process handles a single event (Legacy/Single use)
func (p *TransactionProcessor) Process(ctx context.Context, event model.DebeziumEvent) error {
	// Handle create (c), update (u), and read/snapshot (r) operations
	if event.Op == "c" || event.Op == "u" || event.Op == "r" {
		data := event.After
		id := fmt.Sprintf("%v", data.ID)

		return p.repo.UpsertTransaction(
			ctx,
			id,
			data.ExitPlaza,
			data.EntryPlaza,
			data.MoneyValue,
		)
	}
	return nil
}

// ProcessBatch handles multiple events at once for high performance
func (p *TransactionProcessor) ProcessBatch(ctx context.Context, events []model.DebeziumEvent) error {
	// 1. Filter and prepare data for the repository
	// We only want to upsert events that have "After" data (c, u, r)
	validEvents := make([]model.DebeziumEvent, 0, len(events))

	for _, event := range events {
		if event.Op == "c" || event.Op == "u" || event.Op == "r" {
			validEvents = append(validEvents, event)
		}
	}

	if len(validEvents) == 0 {
		return nil
	}

	// 2. Call the batch upsert in the repository
	// This will be much faster than calling p.repo.UpsertTransaction 50 times
	err := p.repo.UpsertBatch(ctx, validEvents)
	if err != nil {
		return fmt.Errorf("processor batch failed: %w", err)
	}

	return nil
}
