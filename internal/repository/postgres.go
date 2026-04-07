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

func (r *PostgresRepo) UpsertTransaction(id string, amount float64) error {
	// simple upsert example
	query := `
	INSERT INTO transaction (id, money_value)
	VALUES ($1, $2)
	ON CONFLICT (id) DO UPDATE SET money_value = EXCLUDED.money_value
	`
	_, err := r.pool.Exec(context.Background(), query, id, amount)
	if err != nil {
		return fmt.Errorf("failed upsert: %w", err)
	}
	return nil
}

func (r *PostgresRepo) Close() {
	r.pool.Close()
}
