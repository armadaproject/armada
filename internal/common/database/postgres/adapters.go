package postgres

import (
	"context"

	dbtypes "github.com/armadaproject/armada/internal/common/database/types"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresConnAdapter struct {
	*pgx.Conn
}

func (p PostgresConnAdapter) Exec(ctx context.Context, sql string, args ...any) (dbtypes.DatabaseQueryResult, error) {
	return p.Conn.Exec(ctx, sql, args)
}

func (p PostgresConnAdapter) Query(ctx context.Context, sql string, args ...any) (dbtypes.DatabaseRows, error) {
	return p.Conn.Query(ctx, sql, args)
}

func (p PostgresConnAdapter) QueryRow(ctx context.Context, sql string, args ...any) dbtypes.DatabaseRow {
	return p.Conn.QueryRow(ctx, sql, args)
}

func (p PostgresConnAdapter) BeginTx(ctx context.Context, opts dbtypes.DatabaseTxOptions) (dbtypes.DatabaseTx, error) {
	trx, err := p.Conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}

	return PostgresTrxAdapter{Tx: trx}, nil
}

func (p PostgresConnAdapter) BeginTxFunc(ctx context.Context, opts dbtypes.DatabaseTxOptions, action func(dbtypes.DatabaseTx) error) error {
	tx, err := p.Conn.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:       pgx.TxIsoLevel(opts.Isolation),
		DeferrableMode: pgx.Deferrable,
		AccessMode:     pgx.TxAccessMode(opts.AccessMode),
	})

	if err != nil {
		return err
	}

	if err := action(PostgresTrxAdapter{Tx: tx}); err != nil {
		return tx.Rollback(ctx)
	}

	return nil
}

type PostgresPoolAdapter struct {
	*pgxpool.Pool
}

func (p PostgresPoolAdapter) Exec(ctx context.Context, sql string, args ...any) (dbtypes.DatabaseQueryResult, error) {
	return p.Pool.Exec(ctx, sql, args)
}

func (p PostgresPoolAdapter) Acquire(ctx context.Context) (dbtypes.DatabaseConn, error) {
	conn, err := p.Pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}

	return PostgresConnAdapter{Conn: conn.Conn()}, nil
}

func (p PostgresPoolAdapter) BeginTx(ctx context.Context, opts dbtypes.DatabaseTxOptions) (dbtypes.DatabaseTx, error) {
	trx, err := p.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}

	return PostgresTrxAdapter{Tx: trx}, nil
}

func (p PostgresPoolAdapter) BeginTxFunc(ctx context.Context, opts dbtypes.DatabaseTxOptions, action func(dbtypes.DatabaseTx) error) error {
	tx, err := p.Pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:       pgx.TxIsoLevel(opts.Isolation),
		DeferrableMode: "x",
		AccessMode:     pgx.TxAccessMode(opts.AccessMode),
	})

	if err != nil {
		return err
	}

	if err := action(PostgresTrxAdapter{Tx: tx}); err != nil {
		return tx.Rollback(ctx)
	}

	return tx.Commit(ctx)
}

func (p PostgresPoolAdapter) Query(ctx context.Context, sql string, args ...any) (dbtypes.DatabaseRows, error) {
	return p.Pool.Query(ctx, sql, args)
}

func (p PostgresPoolAdapter) QueryRow(ctx context.Context, sql string, args ...any) dbtypes.DatabaseRow {
	return p.Pool.QueryRow(ctx, sql, args)
}

type PostgresTrxAdapter struct {
	pgx.Tx
}

func (t PostgresTrxAdapter) Exec(ctx context.Context, sql string, args ...any) (dbtypes.DatabaseQueryResult, error) {
	return t.Tx.Exec(ctx, sql, args)
}

func (t PostgresTrxAdapter) QueryRow(ctx context.Context, sql string, args ...any) dbtypes.DatabaseRow {
	return t.Tx.QueryRow(ctx, sql, args)
}

func (t PostgresTrxAdapter) Query(ctx context.Context, sql string, args ...any) (dbtypes.DatabaseRows, error) {
	return t.Tx.Query(ctx, sql, args)
}

func (t PostgresTrxAdapter) Commit(ctx context.Context) error {
	return t.Tx.Commit(ctx)
}

func (t PostgresTrxAdapter) Rollback(ctx context.Context) error {
	return t.Tx.Rollback(ctx)
}

func (t PostgresTrxAdapter) CopyFrom(ctx context.Context, tableName string, columnNames []string, data [][]any) (int64, error) {
	return t.Tx.CopyFrom(ctx, pgx.Identifier{tableName}, columnNames, pgx.CopyFromRows(data))
}
