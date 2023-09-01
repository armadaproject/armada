package types

import (
	"database/sql"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common/armadacontext"
)

type DatabaseConnection interface {
	Open() (*sql.DB, error)
	GetConfig() configuration.DatabaseConfig
}

// DatabaseConn represents a connection handler interface that provides methods for managing the open connection,
// executing queries, and starting transactions.
type DatabaseConn interface {
	// Close closes the database connection. It returns any error encountered during the closing operation.
	Close(*armadacontext.ArmadaContext) error

	// Ping pings the database to check the connection. It returns any error encountered during the ping operation.
	Ping(*armadacontext.ArmadaContext) error

	// Exec executes a query that doesn't return rows. It returns any error encountered.
	Exec(*armadacontext.ArmadaContext, string, ...any) (any, error)

	// Query executes a query that returns multiple rows. It returns a DatabaseRows interface that allows you to iterate over the result set, and any error encountered.
	Query(*armadacontext.ArmadaContext, string, ...any) (DatabaseRows, error)

	// QueryRow executes a query that returns one row. It returns a DatabaseRow interface representing the result row, and any error encountered.
	QueryRow(*armadacontext.ArmadaContext, string, ...any) DatabaseRow

	// BeginTx starts a transcation with the given DatabaseTxOptions, or returns an error if any occurred.
	BeginTx(*armadacontext.ArmadaContext, DatabaseTxOptions) (DatabaseTx, error)

	// BeginTxFunc starts a transaction and executes the given function within the transaction. It the function runs successfully, BeginTxFunc commits the transaction, otherwise it rolls back and return an errorr.
	BeginTxFunc(*armadacontext.ArmadaContext, DatabaseTxOptions, func(DatabaseTx) error) error
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
	Exec(*armadacontext.ArmadaContext, string, ...any) (any, error)

	// Query executes a query that returns multiple rows.
	// It returns a DatabaseRows interface that allows you to iterate over the result set, and any error encountered.
	Query(*armadacontext.ArmadaContext, string, ...any) (DatabaseRows, error)

	// QueryRow executes a query that returns one row.
	// It returns a DatabaseRow interface representing the result row, and any error encountered.
	QueryRow(*armadacontext.ArmadaContext, string, ...any) DatabaseRow

	// CopyFrom performs a bulk insertion of data into a specified table.
	// It accepts the table name, column names, and a slice of rows representing the data to be inserted. It returns the number of rows inserted and any error encountered.
	CopyFrom(ctx *armadacontext.ArmadaContext, tableName string, columnNames []string, rows [][]any) (int64, error)

	// Commit commits the transaction. It returns any error encountered during the commit operation.
	Commit(*armadacontext.ArmadaContext) error

	// Rollback rolls back the transaction. It returns any error encountered during the rollback operation.
	Rollback(*armadacontext.ArmadaContext) error
}

// DatabasePool represents a database connection pool interface that provides methods for acquiring and managing database connections.
type DatabasePool interface {
	// Acquire acquires a database connection from the pool.
	// It takes a context and returns a DatabaseConn representing the acquired connection and any encountered error.
	Acquire(*armadacontext.ArmadaContext) (DatabaseConn, error)

	// Ping pings the database to check the connection. It returns any error encountered during the ping operation.
	Ping(*armadacontext.ArmadaContext) error

	// Close closes the database connection. It returns any error encountered during the closing operation.
	Close()

	// Exec executes a query that doesn't return rows. It returns any error encountered.
	Exec(*armadacontext.ArmadaContext, string, ...any) (any, error)

	// Query executes a query that returns multiple rows.
	// It returns a DatabaseRows interface that allows you to iterate over the result set, and any error encountered.
	Query(*armadacontext.ArmadaContext, string, ...any) (DatabaseRows, error)

	// BeginTx starts a transcation with the given DatabaseTxOptions, or returns an error if any occurred.
	BeginTx(*armadacontext.ArmadaContext, DatabaseTxOptions) (DatabaseTx, error)

	// BeginTxFunc starts a transaction and executes the given function within the transaction.
	// It the function runs successfully, BeginTxFunc commits the transaction, otherwise it rolls back and return an error.
	BeginTxFunc(*armadacontext.ArmadaContext, DatabaseTxOptions, func(DatabaseTx) error) error
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
