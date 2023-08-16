package database

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// This is a temporary interface to act as a bridge between upgrading from pgx/v4 to pgx/v5
// TODO (Mo-Fatah): Remove this after https://github.com/armadaproject/armada/pull/2659 is ready to be used in the code.
type Querier interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}
