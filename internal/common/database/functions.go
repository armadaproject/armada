package database

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

func UniqueTableName(table string) string {
	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	return fmt.Sprintf("%s_tmp_%s", table, suffix)
}

func BatchInsert(ctx context.Context, db *pgxpool.Pool, createTmp func(pgx.Tx) error,
	insertTmp func(pgx.Tx) error, copyToDest func(pgx.Tx) error,
) error {
	return db.BeginTxFunc(ctx, pgx.TxOptions{
		IsoLevel:       pgx.ReadCommitted,
		AccessMode:     pgx.ReadWrite,
		DeferrableMode: pgx.Deferrable,
	}, func(tx pgx.Tx) error {
		// Create a temporary table to hold the staging data
		err := createTmp(tx)
		if err != nil {
			return err
		}

		err = insertTmp(tx)
		if err != nil {
			return err
		}

		err = copyToDest(tx)
		if err != nil {
			return err
		}
		return nil
	})
}
