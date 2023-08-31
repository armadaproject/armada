# Armada Database Interfaces

## Problem Description

Open source projects should not be hard coded to a particular Database. Armada currently only allows users to use Postgres. This project is to build interfaces around our connections to Postgres so we can allow other databases.

## Solution

1. Introduce base common database interfaces that can be shared reused by all components (Lookout, Scheduler, Scheduler Ingester).
2. Add interfaces that abstracts the hardcoded Postgres configuration.
3. Add interfaces around `pgx` structs.

### Functional Specification (API Description)

#### Database Connection

Most of the components (Lookout, Scheduler, Scheduler Ingester) rely on [PostgresConfig](https://github.com/armadaproject/armada/blob/master/internal/armada/configuration/types.go#L294) to connect to external databases, we can avoid hardcoding the configuration of those components to use `PostgresConfig` but defining a generic `DatabaseConfig` interface that's when implemented will provide those components with the necessary details to connect to databases.

    /**
        Components configuration (e.g. LookoutConfiguration) can now make use of this interface instead of hardcoding PostgresConfig.
    */
    type DatabaseConfig interface {
    	GetMaxOpenConns()       int
    	GetMaxIdleConns()       int
    	GetConnMaxLifetime()    time.Duration
    	GetConnectionString()   string
    }

    type DatabaseConnection interface {
    	GetConnection()     (*sql.DB, error)
    	GetConfig()         DatabaseConfig
    }

The existing configurations can then be tweaked to use the new generic `DatabaseConfig` interface instead of hardcoding `PostgresConfig`

    type LookoutConfiguration struct {
        Postgres    PostgresConfig // this can be replaced with the new Database property
        Database    DatabaseConfig // new property
    }

#### Database Communication

Currently, most of the Armada components make use of the `github.com/jackc/pgx` Postgres client which provides APIs to interact exclusively with Postgres databases, this makes Armada tightly coupled with Postgres and makes it impossible to use other SQL dialects (e.g. MySQL).

A way to fix this would be to design database-agnostic interfaces that can abstract away the existing Postgres core implementation (pgx), and then implement adapters around `pgx` that implement those interfaces. This will allow for having a high level abstraction API for interacting with databases while maintaining the existing Postgres core implementation.
To accomplish this, we will need to define interfaces for the following features:

1.  Connection Handler

        // DatabaseConn represents a connection handler interface that provides methods for managing the open connection, executing queries, and starting transactions.
        type DatabaseConn interface {
        	// Close closes the database connection. It returns any error encountered during the closing operation.
        	Close(*context.ArmadaContext) error

        	// Ping pings the database to check the connection. It returns any error encountered during the ping operation.
        	Ping(*context.ArmadaContext) error

        	// Exec executes a query that doesn't return rows. It returns any error encountered.
        	Exec(*context.ArmadaContext, string, ...any) (any, error)

        	// Query executes a query that returns multiple rows. It returns a DatabaseRows interface that allows you to iterate over the result set, and any error encountered.
        	Query(*context.ArmadaContext, string, ...any) (DatabaseRows, error)

        	// QueryRow executes a query that returns one row. It returns a DatabaseRow interface representing the result row, and any error encountered.
        	QueryRow(*context.ArmadaContext, string, ...any) DatabaseRow

        	// BeginTx starts a transcation with the given DatabaseTxOptions, or returns an error if any occured.
        	BeginTx(*context.ArmadaContext, DatabaseTxOptions) (DatabaseTx, error)

        	// BeginTxFunc starts a transaction and executes the given function within the transaction. It the function runs successfuly, BeginTxFunc commits the transaction, otherwise it rolls back and return an errorr.
        	BeginTxFunc(*context.ArmadaContext, DatabaseTxOptions, func(DatabaseTx) error) error
        }

2.  Connection Pool

        // DatabasePool represents a database connection pool interface that provides methods for acquiring and managing database connections.
        type DatabasePool interface {
        	// Acquire acquires a database connection from the pool. It takes a context and returns a DatabaseConn representing the acquired connection and any encountered error.
        	Acquire(*context.ArmadaContext) (DatabaseConn, error)

        	// Ping pings the database to check the connection. It returns any error encountered during the ping operation.
        	Ping(*context.ArmadaContext) error

        	// Close closes the database connection. It returns any error encountered during the closing operation.
        	Close()

        	// Exec executes a query that doesn't return rows. It returns any error encountered.
        	Exec(*context.ArmadaContext, string, ...any) (any, error)

        	// Query executes a query that returns multiple rows. It returns a DatabaseRows interface that allows you to iterate over the result set, and any error encountered.
        	Query(*context.ArmadaContext, string, ...any) (DatabaseRows, error)

        	// BeginTx starts a transcation with the given DatabaseTxOptions, or returns an error if any occured.
        	BeginTx(*context.ArmadaContext, DatabaseTxOptions) (DatabaseTx, error)

        	// BeginTxFunc starts a transaction and executes the given function within the transaction. It the function runs successfuly, BeginTxFunc commits the transaction, otherwise it rolls back and return an errorr.
        	BeginTxFunc(*context.ArmadaContext, DatabaseTxOptions, func(DatabaseTx) error) error
        }

3.  Transaction

        // DatabaseTx represents a database transaction interface that provides methods for executing queries, managing transactions, and performing bulk insertions.
        type DatabaseTx interface {
        	// Exec executes a query that doesn't return rows. It returns any error encountered.
        	Exec(*context.ArmadaContext, string, ...any) (any, error)

        	// Query executes a query that returns multiple rows. It returns a DatabaseRows interface that allows you to iterate over the result set, and any error encountered.
        	Query(*context.ArmadaContext, string, ...any) (DatabaseRows, error)

        	// QueryRow executes a query that returns one row. It returns a DatabaseRow interface representing the result row, and any error encountered.
        	QueryRow(*context.ArmadaContext, string, ...any) DatabaseRow

        	// CopyFrom performs a bulk insertion of data into a specified table. It accepts the table name, column names, and a slice of rows representing the data to be inserted. It returns the number of rows inserted and any error encountered.
        	CopyFrom(ctx *context.ArmadaContext, tableName string, columnNames []string, rows [][]any) (int64, error)

        	// Commit commits the transaction. It returns any error encountered during the commit operation.
        	Commit(*context.ArmadaContext) error

        	// Rollback rolls back the transaction. It returns any error encountered during the rollback operation.
        	Rollback(*context.ArmadaContext) error
        }

4.  Result Row

        // DatabaseRow represents a single row in a result set.
        type DatabaseRow interface {
        	// Scan reads the values from the current row into dest values positionally. It returns an error if any occured during the read operation.
        	Scan(dest ...any) error
        }

5.  Resultset

        // DatabaseRows represents an interator over a result set.
        type DatabaseRows interface {
        	// Close closes the result set.
        	Close() error

        	// Next moves the iterator to the next row in the result set, it returns false if the result set is exhausted, otherwise true.
        	Next() bool

        	// Err returns the error, if any, encountered during iteration over the result set.
        	Err() error

        	// Scan reads the values from the current row into dest values positionally. It returns an error if any occured during the read operation.
        	Scan(dest ...any) error
        }

### Implementation Plan

Designing interfaces that can remove the coupling between Armada and Postgres while maintaining the existing core Postgres implementation is a requirement.

To fullfill this requirement, we can implement adapters around the `pgx` client so that it also implements the interfaces defined above.

For example, an adapter can be implemented for `pgxpool.Pool` so that it can be used with `DatabasePool`:

    type PostgresPoolAdapter struct {
    	*pgxpool.Pool
    }

    func (p PostgresPoolAdapter) Exec(ctx *context.ArmadaContext, sql string, args ...any) (any, error) {
    	return p.Pool.Exec(ctx, sql, args)
    }

    func (p PostgresPoolAdapter) BeginTxFunc(ctx *context.ArmadaContext, opts dbtypes.DatabaseTxOptions, action func(dbtypes.DatabaseTx) error) error {
    	tx, err := p.Pool.BeginTx(ctx, pgx.TxOptions{
    		IsoLevel:       pgx.TxIsoLevel(opts.Isolation),
    		DeferrableMode: opts.DeferrableMode,
    		AccessMode:     pgx.TxAccessMode(opts.AccessMode),
    	})

    	if err != nil {
    		return err
    	}

    	// PostgresTrxAdapter is the Postgres adapter for DatabaseTx interface
    	if err := action(PostgresTrxAdapter{Tx: tx}); err != nil {
    		return tx.Rollback(ctx)
    	}

    	return tx.Commit(ctx)
    }

The example above showcases the implementation of a Postgres connection pool adapter, this example implements the `DatabasePool` interface (the rest of the methods can be implemented similarly to `Exec` and `BeginTxFunc`).

This allows the components that make use `pgxpool.Pool` (e.g. Lookout) to switch to using `DatabasePool` which underneath can make use of `pgxpool.Pool` (or any other `DatabasePool` implementation) without making any changes to the core Postgres implementation.

To support new SQL dialects, we can simply introduce adapters that implement the interfaces, as well as introduce some level of flexibility into the configuration of components to allow choosing which dialect we want to use.
