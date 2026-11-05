package schema

import (
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/database"
)

// ErrUnpartitionedJobTable is returned by CheckJobTablePartitioned when the
// job table still uses the unpartitioned schema and contains data.
var ErrUnpartitionedJobTable = errors.New("The job table is still using the legacy unpartitioned schema, which is no longer supported as of v0.24.0. " +
	"Run `lookout --partitionJobTable --preflight` using this version, then migrate. " +
	"See https://armadaproject.io/docs/lookout-job-table-partitioning.")

// CheckJobTablePartitioned returns ErrUnpartitionedJobTable if the job table
// still uses the unpartitioned schema and contains data. An empty
// unpartitioned job table is allowed, because lookout --migrateDatabase
// partitions it automatically.
func CheckJobTablePartitioned(ctx *armadacontext.Context, q database.Querier) error {
	shape, _, err := DetectJobTableShape(ctx, q)
	if err != nil {
		return errors.Wrap(err, "detect job table shape")
	}
	if shape != JobTableShapeUnpartitioned {
		return nil
	}
	hasRows, err := JobTableHasRows(ctx, q)
	if err != nil {
		return errors.Wrap(err, "check whether job table has rows")
	}
	if hasRows {
		return ErrUnpartitionedJobTable
	}
	return nil
}

// partitionAdvisoryLockKey identifies the session-level advisory lock held by
// ExecutePartitionJobTable, so that a second run fails fast instead of
// queueing behind the first.
const partitionAdvisoryLockKey int64 = 0x41524d4144414a54

// lockNotAvailable is the SQLSTATE of an error caused by lock_timeout.
const lockNotAvailable = "55P03"

// ExecuteOptions configures ExecutePartitionJobTable.
type ExecuteOptions struct {
	// LockTimeout bounds each attempt to take the ACCESS EXCLUSIVE lock on
	// job, so that the conversion does not stall every reader and writer of
	// job while it queues behind a long-running query.
	LockTimeout time.Duration
	// LockAttempts is the number of attempts to take the lock.
	LockAttempts int
	// ProgressInterval is how often to log the elapsed time while converting.
	ProgressInterval time.Duration
}

// ExecutePartitionJobTable converts an unpartitioned job table, whatever its
// size, on behalf of an operator. It returns false without making changes if
// the job table is already partitioned. Otherwise it refuses unless the
// migration chain is fully applied, fails fast if another run is in
// progress, and retries taking the lock on job up to opts.LockAttempts
// times. The conversion runs in a single transaction, so a failure leaves
// the job table as it was.
//
// conn must be a single connection, because the advisory lock is held by the
// session.
func ExecutePartitionJobTable(ctx *armadacontext.Context, conn *pgx.Conn, opts ExecuteOptions) (bool, error) {
	shape, detail, err := DetectJobTableShape(ctx, conn)
	if err != nil {
		return false, errors.Wrap(err, "detect job table shape")
	}
	switch shape {
	case JobTableShapePartitioned:
		return false, nil
	case JobTableShapeUnpartitioned:
	case JobTableShapeUnexpected:
		return false, fmt.Errorf("job table is partitioned but shape does not match target: %s", detail)
	default:
		return false, fmt.Errorf("job table is %s; run lookout --migrateDatabase first", shape)
	}

	if err := CheckMigrationsApplied(ctx, conn); err != nil {
		return false, err
	}

	var acquired bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, partitionAdvisoryLockKey).Scan(&acquired); err != nil {
		return false, errors.Wrap(err, "take advisory lock")
	}
	if !acquired {
		return false, errors.New("another lookout --partitionJobTable --execute is already running against this database")
	}
	defer func() {
		if _, err := conn.Exec(ctx, `SELECT pg_advisory_unlock($1)`, partitionAdvisoryLockKey); err != nil {
			ctx.Logger().WithError(err).Warn("Failed to release advisory lock")
		}
	}()

	stopProgress := logProgress(ctx, opts.ProgressInterval)
	defer stopProgress()

	for attempt := 1; ; attempt++ {
		ctx.Infof("Waiting up to %s for the lock on the job table (attempt %d of %d)", opts.LockTimeout, attempt, opts.LockAttempts)
		_, err := applyPartitioner(ctx, conn, conversionOptions{lockTimeout: opts.LockTimeout})
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != lockNotAvailable {
			return err == nil, err
		}
		if attempt >= opts.LockAttempts {
			return false, errors.Errorf("could not lock the job table in %d attempts; run --preflight to see the sessions holding locks on it", opts.LockAttempts)
		}
		ctx.Warn("Timed out waiting for the lock on the job table; retrying")
	}
}

func logProgress(ctx *armadacontext.Context, interval time.Duration) func() {
	start := time.Now()
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				ctx.Infof("Still partitioning the job table: %s elapsed", time.Since(start).Round(time.Second))
			}
		}
	}()
	return func() { close(done) }
}

// CheckMigrationsApplied returns an error unless the database has had
// exactly the migration chain embedded in this version of Lookout applied.
// The conversion copies the latest column list, so it must not run against a
// database at any other version.
func CheckMigrationsApplied(ctx *armadacontext.Context, q database.Querier) error {
	applied, expected, err := migrationVersions(ctx, q)
	if err != nil {
		return err
	}
	switch {
	case applied < expected:
		return fmt.Errorf("the lookout database is at migration %d but this version of lookout expects %d: run lookout --migrateDatabase with this version first", applied, expected)
	case applied > expected:
		return fmt.Errorf("the lookout database is at migration %d, newer than the %d expected by this version of lookout: use the version of lookout that migrated it", applied, expected)
	}
	return nil
}

func migrationVersions(ctx *armadacontext.Context, q database.Querier) (int, int, error) {
	migrations, err := LookoutMigrations()
	if err != nil {
		return 0, 0, err
	}
	expected := migrations[len(migrations)-1].ID()

	var exists bool
	if err := q.QueryRow(ctx, `SELECT to_regclass('database_version') IS NOT NULL`).Scan(&exists); err != nil {
		return 0, 0, errors.Wrap(err, "check for database_version")
	}
	if !exists {
		return 0, expected, nil
	}
	var applied int
	if err := q.QueryRow(ctx, `SELECT last_value FROM database_version`).Scan(&applied); err != nil {
		return 0, 0, errors.Wrap(err, "read database_version")
	}
	return applied, expected, nil
}

// ConversionScript returns the SQL that ExecutePartitionJobTable runs to
// convert the unpartitioned job table in db, as a single transaction that a
// database administrator can review or run themselves.
func ConversionScript(ctx *armadacontext.Context, q database.Querier) (string, error) {
	columns, err := jobColumns(ctx, q)
	if err != nil {
		return "", err
	}
	statements := []string{"BEGIN", disableStatementTimeout, lockJobTable}
	for _, step := range conversionSteps(columns) {
		statements = append(statements, "-- "+step.description+"\n"+step.sql)
	}
	statements = append(statements, "COMMIT")

	var script strings.Builder
	for _, statement := range statements {
		script.WriteString(strings.TrimRight(strings.TrimSpace(statement), ";"))
		script.WriteString(";\n\n")
	}
	return script.String(), nil
}
