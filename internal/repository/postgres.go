package repository

import (
	"context"
	"fmt"
	"kafka-worker/internal/model"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresRepo struct {
	pool *pgxpool.Pool
}

func NewPostgresRepo(connString string) (*PostgresRepo, error) {
	// Best Practice: Use the context passed from main or a timeout context
	pool, err := pgxpool.New(context.Background(), connString)
	if err != nil {
		return nil, err
	}
	return &PostgresRepo{pool: pool}, nil
}

// UpsertTransaction handles a single row (Legacy/Single use)
func (r *PostgresRepo) UpsertTransaction(ctx context.Context, id, exitPlaza, entryPlaza string, amount float64) error {
	query := `
	INSERT INTO transaction_test (id, exit_plaza, entry_plaza, money_value)
	VALUES ($1, $2, $3, $4)
	ON CONFLICT (id) 
	DO UPDATE SET 
		exit_plaza = EXCLUDED.exit_plaza,
		entry_plaza = EXCLUDED.entry_plaza,
		money_value = EXCLUDED.money_value
	`
	_, err := r.pool.Exec(ctx, query, id, exitPlaza, entryPlaza, amount)
	if err != nil {
		return fmt.Errorf("failed upsert: %w", err)
	}
	return nil
}

// UpsertBatch handles multiple rows at once using pgx.Batch
// This is critical for high-volume data sync (2M rows/day)
func (r *PostgresRepo) UpsertBatch(ctx context.Context, events []model.DebeziumEvent) error {
	batch := &pgx.Batch{}

	query := `
	INSERT INTO transaction_test (id, exit_plaza, entry_plaza, money_value)
	VALUES ($1, $2, $3, $4)
	ON CONFLICT (id) 
	DO UPDATE SET 
		exit_plaza = EXCLUDED.exit_plaza,
		entry_plaza = EXCLUDED.entry_plaza,
		money_value = EXCLUDED.money_value
	`

	for _, event := range events {
		data := event.After
		// Ensure types match your schema (id as string, money as float64)
		batch.Queue(query, fmt.Sprintf("%v", data.ID), data.ExitPlaza, data.EntryPlaza, data.MoneyValue)
	}

	// SendBatch sends all queued queries in one network call
	br := r.pool.SendBatch(ctx, batch)
	defer br.Close()

	// You must call Exec/Query for each queued item to check for errors
	for i := 0; i < len(events); i++ {
		_, err := br.Exec()
		if err != nil {
			return fmt.Errorf("batch execution error at index %d: %w", i, err)
		}
	}

	return nil
}

func (r *PostgresRepo) Close() {
	if r.pool != nil {
		r.pool.Close()
	}
}
