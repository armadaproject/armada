package schema

import (
	"context"
	_ "embed"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/database"
)

//go:embed target_schema.sql
var targetSchemaSQL string

//go:embed target_schema_indexes.sql
var targetSchemaIndexesSQL string

// testHookBeforeStep is called inside convertUnpartitionedToPartitioned
// before each conversion step. It is nil in production; tests in this
// package set it.
var testHookBeforeStep func(step conversionStep)

// conversionOptions configures applyPartitioner.
type conversionOptions struct {
	// onlyIfEmpty leaves an unpartitioned job table untouched if it has rows.
	onlyIfEmpty bool
	// lockTimeout bounds the wait for the ACCESS EXCLUSIVE lock on job; zero
	// waits indefinitely.
	lockTimeout time.Duration
}

// DB is the database handle needed by the partitioner. Both *pgx.Conn and
// *pgxpool.Pool satisfy it.
type DB interface {
	database.Querier
	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error)
}

// JobTableShape describes the live shape of the job table.
type JobTableShape int

const (
	JobTableShapeUnknown JobTableShape = iota
	JobTableShapeMissing
	JobTableShapeUnpartitioned
	JobTableShapePartitioned
	JobTableShapeUnexpected
)

func (s JobTableShape) String() string {
	switch s {
	case JobTableShapeMissing:
		return "missing"
	case JobTableShapeUnpartitioned:
		return "unpartitioned"
	case JobTableShapePartitioned:
		return "partitioned"
	case JobTableShapeUnexpected:
		return "unexpected"
	default:
		return "unknown"
	}
}

// PartitionJobTable converts the job table into the partitioned shape,
// whatever its size.
//
// Behaviour:
//
//   - If the job table is already partitioned with the expected shape,
//     backfills idx_job_queue_job_id if it is missing, then returns.
//   - If the job table is unpartitioned, converts it into the partitioned
//     shape in a single transaction.
//   - If the job table has an unexpected partitioned shape, or is missing
//     entirely, returns an error.
//
// The caller must have already applied the lookout migration chain.
func PartitionJobTable(ctx *armadacontext.Context, db DB) error {
	_, err := applyPartitioner(ctx, db, conversionOptions{})
	return err
}

// PartitionJobTableIfEmpty behaves like PartitionJobTable, except that it
// leaves an unpartitioned job table untouched if it contains any rows. It
// returns the shape of the job table afterwards.
//
// Emptiness is checked once without a lock, so that a populated table is
// never locked, and again under the ACCESS EXCLUSIVE lock taken for the
// conversion, so that a row committed in between prevents the conversion.
func PartitionJobTableIfEmpty(ctx *armadacontext.Context, db DB) (JobTableShape, error) {
	shape, _, err := DetectJobTableShape(ctx, db)
	if err != nil {
		return JobTableShapeUnknown, errors.Wrap(err, "detect job table shape")
	}
	if shape == JobTableShapeUnpartitioned {
		hasRows, err := JobTableHasRows(ctx, db)
		if err != nil {
			return JobTableShapeUnknown, errors.Wrap(err, "check whether job table has rows")
		}
		if hasRows {
			return JobTableShapeUnpartitioned, nil
		}
	}
	return applyPartitioner(ctx, db, conversionOptions{onlyIfEmpty: true})
}

func applyPartitioner(ctx *armadacontext.Context, db DB, opts conversionOptions) (JobTableShape, error) {
	var shape JobTableShape
	err := pgx.BeginTxFunc(ctx, db, pgx.TxOptions{}, func(tx pgx.Tx) error {
		detected, detail, err := DetectJobTableShape(ctx, tx)
		if err != nil {
			return errors.Wrap(err, "detect job table shape")
		}

		switch detected {
		case JobTableShapePartitioned:
			if err := backfillQueueJobIDIndex(ctx, tx); err != nil {
				return errors.Wrap(err, "backfill idx_job_queue_job_id")
			}
			shape = detected
			return nil
		case JobTableShapeUnpartitioned:
			shape, err = convertUnpartitionedToPartitioned(ctx, tx, opts)
			if err != nil {
				return errors.Wrap(err, "convert unpartitioned job table")
			}
			return nil
		case JobTableShapeUnexpected:
			return fmt.Errorf("job table is partitioned but shape does not match target: %s", detail)
		case JobTableShapeMissing:
			return errors.New("job table not found; run lookout migration chain first")
		default:
			return fmt.Errorf("unknown job table shape: %d", detected)
		}
	})
	if err != nil {
		return JobTableShapeUnknown, err
	}
	return shape, nil
}

// IsJobTablePartitioned reports whether the job table in the current schema
// is a partitioned table, without checking that its shape matches the
// target.
func IsJobTablePartitioned(ctx *armadacontext.Context, q database.Querier) (bool, error) {
	var isPartitioned bool
	err := q.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_partitioned_table pt
                        JOIN pg_class c ON c.oid = pt.partrelid
                        WHERE c.relname = 'job' AND c.relnamespace = current_schema()::regnamespace)`,
	).Scan(&isPartitioned)
	return isPartitioned, err
}

// DetectJobTableShape inspects the database and reports the shape of the job
// table. When the shape is JobTableShapeUnexpected, the returned detail
// string describes the first mismatch found (to aid the operator).
func DetectJobTableShape(ctx *armadacontext.Context, q database.Querier) (JobTableShape, string, error) {
	var exists bool
	if err := q.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'job' AND relkind IN ('r', 'p') AND relnamespace = current_schema()::regnamespace)`,
	).Scan(&exists); err != nil {
		return JobTableShapeUnknown, "", err
	}
	if !exists {
		return JobTableShapeMissing, "", nil
	}

	isPartitioned, err := IsJobTablePartitioned(ctx, q)
	if err != nil {
		return JobTableShapeUnknown, "", err
	}
	if !isPartitioned {
		return JobTableShapeUnpartitioned, "", nil
	}

	mismatch, err := findShapeMismatch(ctx, q)
	if err != nil {
		return JobTableShapeUnknown, "", err
	}
	if mismatch != "" {
		return JobTableShapeUnexpected, mismatch, nil
	}
	return JobTableShapePartitioned, "", nil
}

// JobTableHasRows reports whether the job table has any rows.
func JobTableHasRows(ctx *armadacontext.Context, q database.Querier) (bool, error) {
	var hasRows bool
	err := q.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM job)`).Scan(&hasRows)
	return hasRows, err
}

// findShapeMismatch returns a non-empty string describing the first shape
// mismatch found between the live partitioned job table and the target
// schema. Returns an empty string if the shape matches.
func findShapeMismatch(ctx *armadacontext.Context, q database.Querier) (string, error) {
	rows, err := q.Query(ctx, `
		SELECT c.relname FROM pg_inherits i
		JOIN pg_class c ON c.oid = i.inhrelid
		JOIN pg_class p ON p.oid = i.inhparent
		WHERE p.relname = 'job' AND p.relnamespace = current_schema()::regnamespace
		ORDER BY c.relname
	`)
	if err != nil {
		return "", err
	}
	var partitions []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			return "", err
		}
		partitions = append(partitions, name)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return "", err
	}
	expectedPartitions := []string{"job_active", "job_terminated"}
	if !slices.Equal(partitions, expectedPartitions) {
		return fmt.Sprintf("expected partitions %v, found %v", expectedPartitions, partitions), nil
	}

	expectedBounds := map[string][]int{
		"job_active":     {1, 2, 3, 8},
		"job_terminated": {4, 5, 6, 7, 9},
	}
	for partition, want := range expectedBounds {
		var expr string
		if err := q.QueryRow(ctx, `
			SELECT pg_get_expr(c.relpartbound, c.oid)
			FROM pg_class c
			WHERE c.relname = $1 AND c.relnamespace = current_schema()::regnamespace
		`, partition).Scan(&expr); err != nil {
			return "", err
		}
		got, err := extractInts(expr)
		if err != nil {
			return "", errors.Wrapf(err, "parse partition bounds for %s", partition)
		}
		slices.Sort(got)
		slices.Sort(want)
		if !slices.Equal(got, want) {
			return fmt.Sprintf("partition %s: expected bounds %v, got %v (from %q)", partition, want, got, expr), nil
		}
	}

	type column struct {
		name    string
		typ     string
		notNull bool
	}
	expectedColumns := []column{
		{"job_id", "character varying", true},
		{"queue", "character varying", true},
		{"owner", "character varying", true},
		{"jobset", "character varying", true},
		{"cpu", "bigint", true},
		{"memory", "bigint", true},
		{"ephemeral_storage", "bigint", true},
		{"gpu", "bigint", true},
		{"priority", "bigint", true},
		{"submitted", "timestamp without time zone", true},
		{"cancelled", "timestamp without time zone", false},
		{"state", "smallint", true},
		{"last_transition_time", "timestamp without time zone", true},
		{"last_transition_time_seconds", "bigint", true},
		{"job_spec", "bytea", false},
		{"duplicate", "boolean", true},
		{"priority_class", "character varying", false},
		{"latest_run_id", "character varying", false},
		{"cancel_reason", "character varying", false},
		{"namespace", "character varying", false},
		{"annotations", "jsonb", true},
		{"external_job_uri", "character varying", false},
		{"cancel_user", "character varying", false},
		{"preempt_user", "character varying", false},
		{"reprioritize_user", "character varying", false},
	}
	rows, err = q.Query(ctx, `
		SELECT column_name, data_type, is_nullable = 'NO'
		FROM information_schema.columns
		WHERE table_name = 'job' AND table_schema = current_schema()
		ORDER BY ordinal_position
	`)
	if err != nil {
		return "", err
	}
	var actualColumns []column
	for rows.Next() {
		var c column
		if err := rows.Scan(&c.name, &c.typ, &c.notNull); err != nil {
			rows.Close()
			return "", err
		}
		actualColumns = append(actualColumns, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return "", err
	}
	if len(actualColumns) != len(expectedColumns) {
		return fmt.Sprintf("expected %d columns on job, found %d", len(expectedColumns), len(actualColumns)), nil
	}
	for i, want := range expectedColumns {
		got := actualColumns[i]
		if got != want {
			return fmt.Sprintf("column %d: expected %+v, got %+v", i, want, got), nil
		}
	}

	var pkColumns string
	if err := q.QueryRow(ctx, `
		SELECT string_agg(a.attname, ',' ORDER BY x.ord)
		FROM pg_constraint c
		JOIN pg_class t ON t.oid = c.conrelid
		JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS x(attnum, ord) ON true
		JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = x.attnum
		WHERE t.relname = 'job' AND t.relnamespace = current_schema()::regnamespace AND c.contype = 'p'
	`).Scan(&pkColumns); err != nil {
		return "", err
	}
	if pkColumns != "job_id,state" {
		return fmt.Sprintf("expected primary key (job_id, state), got (%s)", pkColumns), nil
	}

	expectedParentIndexes := []string{
		"idx_job_queue_last_transition_time_seconds",
		"idx_job_queue_jobset_state",
		"idx_job_state",
		"idx_job_submitted",
		"idx_job_jobset_pattern",
		"idx_job_annotations_path",
		"idx_job_latest_run_id",
		"idx_job_queue_namespace",
		"idx_job_ltt_jobid",
	}
	for _, idx := range expectedParentIndexes {
		var exists bool
		if err := q.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = current_schema() AND tablename = 'job' AND indexname = $1)`,
			idx).Scan(&exists); err != nil {
			return "", err
		}
		if !exists {
			return fmt.Sprintf("missing index %s on job", idx), nil
		}
	}

	var activeIdxExists bool
	if err := q.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = current_schema() AND tablename = 'job_active' AND indexname = 'idx_job_active_queue_jobset')`,
	).Scan(&activeIdxExists); err != nil {
		return "", err
	}
	if !activeIdxExists {
		return "missing index idx_job_active_queue_jobset on job_active", nil
	}

	return "", nil
}

// partitionIntRegex matches integer literals inside a partition-bound
// expression like "FOR VALUES IN ('1', '2', '3', '8')" or
// "FOR VALUES IN (1, 2, 3, 8)". Postgres quotes smallint literals but
// not bigint literals in the rendered expression, so we extract the
// integers regardless of surrounding quotes or whitespace.
var partitionIntRegex = regexp.MustCompile(`-?\d+`)

// extractInts parses all signed integers out of s, returning them in the
// order they appear. Used to compare partition-bound sets without being
// sensitive to quoting or whitespace in the Postgres-rendered expression.
func extractInts(s string) ([]int, error) {
	matches := partitionIntRegex.FindAllString(s, -1)
	result := make([]int, 0, len(matches))
	for _, m := range matches {
		n, err := strconv.Atoi(m)
		if err != nil {
			return nil, err
		}
		result = append(result, n)
	}
	return result, nil
}

// backfillQueueJobIDIndex creates idx_job_queue_job_id on an already
// partitioned job table if it is missing. This index was added after the
// partitioner had already shipped, so a database partitioned by an earlier
// version of this package would otherwise be rejected as the wrong shape on
// every subsequent run. It is intentionally excluded from
// expectedParentIndexes for that reason.
func backfillQueueJobIDIndex(ctx *armadacontext.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx,
		`CREATE INDEX IF NOT EXISTS idx_job_queue_job_id ON job (queue, job_id) WITH (fillfactor = 80)`)
	return err
}

// convertUnpartitionedToPartitioned is the conversion branch: it locks job,
// then runs conversionSteps. If opts.onlyIfEmpty is set and job has any rows
// once the lock is held, it makes no changes. It returns the shape of the
// job table afterwards.
func convertUnpartitionedToPartitioned(ctx *armadacontext.Context, tx pgx.Tx, opts conversionOptions) (JobTableShape, error) {
	// The conversion must not be cancelled part-way by a statement_timeout
	// set for the role or database; it runs for as long as it needs.
	if _, err := tx.Exec(ctx, disableStatementTimeout); err != nil {
		return JobTableShapeUnknown, errors.Wrap(err, "disable statement_timeout")
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL lock_timeout = %d", opts.lockTimeout.Milliseconds())); err != nil {
		return JobTableShapeUnknown, errors.Wrap(err, "set lock_timeout")
	}
	if _, err := tx.Exec(ctx, lockJobTable); err != nil {
		return JobTableShapeUnknown, errors.Wrap(err, "lock job table")
	}
	if _, err := tx.Exec(ctx, "SET LOCAL lock_timeout = 0"); err != nil {
		return JobTableShapeUnknown, errors.Wrap(err, "reset lock_timeout")
	}

	// Re-check state under the lock: a concurrent partitioner run may have
	// completed between our initial check and acquiring the lock.
	recheck, _, err := DetectJobTableShape(ctx, tx)
	if err != nil {
		return JobTableShapeUnknown, errors.Wrap(err, "re-check job table shape after lock")
	}
	if recheck == JobTableShapePartitioned {
		return recheck, nil
	}

	if opts.onlyIfEmpty {
		hasRows, err := JobTableHasRows(ctx, tx)
		if err != nil {
			return JobTableShapeUnknown, errors.Wrap(err, "check whether job table has rows after lock")
		}
		if hasRows {
			return JobTableShapeUnpartitioned, nil
		}
	}

	columns, err := jobColumns(ctx, tx)
	if err != nil {
		return JobTableShapeUnknown, err
	}
	for _, step := range conversionSteps(columns) {
		if testHookBeforeStep != nil {
			testHookBeforeStep(step)
		}
		ctx.Info(step.description)
		if _, err := tx.Exec(ctx, step.sql); err != nil {
			return JobTableShapeUnknown, errors.Wrap(err, step.description)
		}
	}
	return JobTableShapePartitioned, nil
}

const (
	disableStatementTimeout = "SET LOCAL statement_timeout = 0"
	lockJobTable            = "LOCK TABLE job IN ACCESS EXCLUSIVE MODE"
)

type conversionStep struct {
	description string
	sql         string
}

// conversionSteps returns the statements that convert a locked,
// unpartitioned job table with the given columns. They create job_new as a
// partitioned table using target_schema.sql, copy the rows from job (letting
// Postgres route them to the correct partition), build the primary key and
// indexes from target_schema_indexes.sql, drop the old job, and rename
// job_new plus its partitions and parent indexes to their final names.
//
// Copying every column of job means that a column missing from
// target_schema.sql fails the copy rather than silently dropping its data.
// Columns of job_new that job lacks, which exist when the conversion runs
// before the migration that adds them, take their defaults.
func conversionSteps(columns []string) []conversionStep {
	columnList := strings.Join(columns, ", ")
	return []conversionStep{
		{"Creating the partitioned table job_new", renderTargetSchema(targetSchemaSQL)},
		{"Copying rows from job into job_new", "INSERT INTO job_new (" + columnList + ") SELECT " + columnList + " FROM job"},
		{"Building the primary key and indexes of job_new", renderTargetSchema(targetSchemaIndexesSQL)},
		// no CASCADE: loud failure for a destructive operation is safer than silently dropping dependent objects
		{"Dropping the unpartitioned job table", "DROP TABLE job"},
		{"Renaming job_new, its partitions and its indexes", strings.Join(renames, ";\n")},
	}
}

func renderTargetSchema(sql string) string {
	return strings.ReplaceAll(sql, "{{TABLE}}", "job_new")
}

var renames = []string{
	`ALTER TABLE job_new RENAME TO job`,
	`ALTER TABLE job_new_active RENAME TO job_active`,
	`ALTER TABLE job_new_terminated RENAME TO job_terminated`,
	`ALTER INDEX job_new_pkey RENAME TO job_pkey`,
	`ALTER INDEX idx_job_new_queue_last_transition_time_seconds RENAME TO idx_job_queue_last_transition_time_seconds`,
	`ALTER INDEX idx_job_new_queue_job_id RENAME TO idx_job_queue_job_id`,
	`ALTER INDEX idx_job_new_queue_jobset_state RENAME TO idx_job_queue_jobset_state`,
	`ALTER INDEX idx_job_new_state RENAME TO idx_job_state`,
	`ALTER INDEX idx_job_new_submitted RENAME TO idx_job_submitted`,
	`ALTER INDEX idx_job_new_jobset_pattern RENAME TO idx_job_jobset_pattern`,
	`ALTER INDEX idx_job_new_annotations_path RENAME TO idx_job_annotations_path`,
	`ALTER INDEX idx_job_new_latest_run_id RENAME TO idx_job_latest_run_id`,
	`ALTER INDEX idx_job_new_queue_namespace RENAME TO idx_job_queue_namespace`,
	`ALTER INDEX idx_job_new_ltt_jobid RENAME TO idx_job_ltt_jobid`,
	`ALTER INDEX idx_job_new_active_queue_jobset RENAME TO idx_job_active_queue_jobset`,
}

// jobColumns returns the quoted names of the columns of job, in order.
func jobColumns(ctx *armadacontext.Context, q database.Querier) ([]string, error) {
	rows, err := q.Query(ctx, `
		SELECT attname FROM pg_attribute
		WHERE attrelid = 'job'::regclass AND attnum > 0 AND NOT attisdropped
		ORDER BY attnum
	`)
	if err != nil {
		return nil, errors.Wrap(err, "list job columns")
	}
	names, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, errors.Wrap(err, "list job columns")
	}
	columns := make([]string, len(names))
	for i, name := range names {
		columns[i] = pgx.Identifier{name}.Sanitize()
	}
	return columns, nil
}
