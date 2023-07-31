package types

import (
	"context"

	"github.com/armadaproject/armada/internal/armada/configuration"
)

type DatabaseConnection interface {
	Open() (DatabaseConn, error)
	GetConfig() configuration.DatabaseConfig
}

// DatabaseConn represents a connection handler interface that provides methods for managing the open connection,
// executing queries, and starting transactions.
type DatabaseConn interface {
	// Close closes the database connection. It returns any error encountered during the closing operation.
	Close(context.Context) error

	// Ping pings the database to check the connection. It returns any error encountered during the ping operation.
	Ping(context.Context) error

	// Exec executes a query that doesn't return rows. It returns any error encountered.
	Exec(context.Context, string, ...any) (DatabaseQueryResult, error)

	// Query executes a query that returns multiple rows. It returns a DatabaseRows interface that allows you to iterate over the result set, and any error encountered.
	Query(context.Context, string, ...any) (DatabaseRows, error)

	// QueryRow executes a query that returns one row. It returns a DatabaseRow interface representing the result row, and any error encountered.
	QueryRow(context.Context, string, ...any) DatabaseRow

	// BeginTx starts a transcation with the given DatabaseTxOptions, or returns an error if any occurred.
	BeginTx(context.Context, DatabaseTxOptions) (DatabaseTx, error)

	// BeginTxFunc starts a transaction and executes the given function within the transaction. It the function runs successfully, BeginTxFunc commits the transaction, otherwise it rolls back and return an errorr.
	BeginTxFunc(context.Context, DatabaseTxOptions, func(DatabaseTx) error) error
}

type DatabaseTxOptions struct {
	Isolation      string
	AccessMode     string
	DeferrableMode string
}

// DatabaseTx represents a database transaction interface that provides methods for executing queries,
// managing transactions, and performing bulk insertions.
type DatabaseTx interface {
	// Exec executes a query that doesn't return rows. It returns any error encountered.
	Exec(context.Context, string, ...any) (DatabaseQueryResult, error)

	// Query executes a query that returns multiple rows.
	// It returns a DatabaseRows interface that allows you to iterate over the result set, and any error encountered.
	Query(context.Context, string, ...any) (DatabaseRows, error)

	// QueryRow executes a query that returns one row.
	// It returns a DatabaseRow interface representing the result row, and any error encountered.
	QueryRow(context.Context, string, ...any) DatabaseRow

	// CopyFrom performs a bulk insertion of data into a specified table.
	// It accepts the table name, column names, and a slice of rows representing the data to be inserted. It returns the number of rows inserted and any error encountered.
	CopyFrom(ctx context.Context, tableName string, columnNames []string, rows [][]any) (int64, error)

	// Commit commits the transaction. It returns any error encountered during the commit operation.
	Commit(context.Context) error

	// Rollback rolls back the transaction. It returns any error encountered during the rollback operation.
	Rollback(context.Context) error
}

// DatabasePool represents a database connection pool interface that provides methods for acquiring and managing database connections.
type DatabasePool interface {
	// Acquire acquires a database connection from the pool.
	// It takes a context and returns a DatabaseConn representing the acquired connection and any encountered error.
	Acquire(context.Context) (DatabaseConn, error)

	// Ping pings the database to check the connection. It returns any error encountered during the ping operation.
	Ping(context.Context) error

	// Close closes the database connection. It returns any error encountered during the closing operation.
	Close()

	// Exec executes a query that doesn't return rows. It returns any error encountered.
	Exec(context.Context, string, ...any) (DatabaseQueryResult, error)

	// Query executes a query that returns multiple rows.
	// It returns a DatabaseRows interface that allows you to iterate over the result set, and any error encountered.
	Query(context.Context, string, ...any) (DatabaseRows, error)

	// QueryRow executes a query that returns one row. It returns a DatabaseRow interface representing the result row, and any error encountered.
	QueryRow(context.Context, string, ...any) DatabaseRow

	// BeginTx starts a transcation with the given DatabaseTxOptions, or returns an error if any occurred.
	BeginTx(context.Context, DatabaseTxOptions) (DatabaseTx, error)

	// BeginTxFunc starts a transaction and executes the given function within the transaction.
	// It the function runs successfully, BeginTxFunc commits the transaction, otherwise it rolls back and return an error.
	BeginTxFunc(context.Context, DatabaseTxOptions, func(DatabaseTx) error) error
}

// DatabaseRow represents a single row in a result set.
type DatabaseRow interface {
	// Scan reads the values from the current row into dest values positionally.
	// It returns an error if any occurred during the read operation.
	Scan(dest ...any) error
}

// DatabaseRows represents an interator over a result set.
type DatabaseRows interface {
	// Close closes the result set.
	Close()

	// Next moves the iterator to the next row in the result set, it returns false if the result set is exhausted, otherwise true.
	Next() bool

	// Err returns the error, if any, encountered during iteration over the result set.
	Err() error

	// Scan reads the values from the current row into dest values positionally. It returns an error if any occurred during the read operation.
	Scan(dest ...any) error
}

// DatabaseQueryReuslt represents the results of executing a query that doesn't return any rows.
type DatabaseQueryResult interface {
	// RowsAffected represents the number of rows that were affected by the executed query.
	RowsAffected() int64
}
