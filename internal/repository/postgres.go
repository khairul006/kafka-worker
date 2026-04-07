package repository

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresRepo struct {
	pool *pgxpool.Pool
}

func NewPostgresRepo(connString string) (*PostgresRepo, error) {
	pool, err := pgxpool.New(context.Background(), connString)
	if err != nil {
		return nil, err
	}
	return &PostgresRepo{pool: pool}, nil
}

func (r *PostgresRepo) UpsertTransaction(id string, exitPlaza string, entryPlaza string, moneyValue float64) error {
	// simple upsert example
	query := `
	INSERT INTO transaction_test (id, exit_plaza, entry_plaza, money_value)
	VALUES ($1, $2, $3, $4)
	ON CONFLICT (id) 
	DO UPDATE SET 
		exit_plaza = EXCLUDED.exit_plaza,
		entry_plaza = EXCLUDED.entry_plaza,
		money_value = EXCLUDED.money_value
	`
	_, err := r.pool.Exec(context.Background(), query, id, exitPlaza, entryPlaza, moneyValue)
	if err != nil {
		return fmt.Errorf("failed upsert: %w", err)
	}
	return nil
}

func (r *PostgresRepo) Close() {
	r.pool.Close()
}
