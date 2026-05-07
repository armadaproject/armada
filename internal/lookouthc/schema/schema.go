package schema

import (
	_ "embed"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common/armadacontext"
)

//go:embed target_schema.sql
var targetSchemaSQL string

// testHookAfterCopy is called inside convertUnpartitionedToPartitioned after
// the INSERT SELECT and before DROP TABLE. It is nil in production; tests set
// it via the exported HookAfterCopy pointer in export_test.go.
var testHookAfterCopy func()

// ApplyPartitioner runs the experimental hot-cold partitioner against db.
//
// Behaviour:
//
//   - If the job table is already partitioned with the expected shape, returns
//     nil without making changes.
//   - If the job table is unpartitioned (e.g., just created by the lookout
//     migration chain), converts it into the partitioned shape in a single
//     transaction.
//   - If the job table exists but has an unexpected partitioned shape, or is
//     missing entirely, returns an error.
//
// The partitioner assumes the caller has already applied the lookout
// migration chain, so the job table either exists as an unpartitioned table
// (fresh chain) or as the partitioned result of a previous partitioner run.
func ApplyPartitioner(ctx *armadacontext.Context, db *pgxpool.Pool) error {
	return pgx.BeginTxFunc(ctx, db, pgx.TxOptions{}, func(tx pgx.Tx) error {
		state, detail, err := detectJobTableState(ctx, tx)
		if err != nil {
			return errors.Wrap(err, "detect job table state")
		}

		switch state {
		case jobStateAlreadyPartitioned:
			return nil
		case jobStateUnpartitioned:
			if err := convertUnpartitionedToPartitioned(ctx, tx); err != nil {
				return errors.Wrap(err, "convert unpartitioned job table")
			}
			return nil
		case jobStatePartitionedWrongShape:
			return fmt.Errorf("job table is partitioned but shape does not match target: %s", detail)
		case jobStateNoJobTable:
			return errors.New("job table not found; run lookout migration chain first")
		default:
			return fmt.Errorf("unknown job table state: %d", state)
		}
	})
}

type jobTableState int

const (
	jobStateUnknown jobTableState = iota
	jobStateNoJobTable
	jobStateUnpartitioned
	jobStateAlreadyPartitioned
	jobStatePartitionedWrongShape
)

// detectJobTableState inspects the database and reports which of the four
// known states the job table is in. When the state is
// jobStatePartitionedWrongShape, the returned detail string describes the
// first mismatch found (to aid the operator).
func detectJobTableState(ctx *armadacontext.Context, q pgx.Tx) (jobTableState, string, error) {
	var exists bool
	if err := q.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'job' AND relkind IN ('r', 'p') AND relnamespace = 'public'::regnamespace)`,
	).Scan(&exists); err != nil {
		return jobStateUnknown, "", err
	}
	if !exists {
		return jobStateNoJobTable, "", nil
	}

	var isPartitioned bool
	if err := q.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_partitioned_table pt
                        JOIN pg_class c ON c.oid = pt.partrelid
                        WHERE c.relname = 'job' AND c.relnamespace = 'public'::regnamespace)`,
	).Scan(&isPartitioned); err != nil {
		return jobStateUnknown, "", err
	}
	if !isPartitioned {
		return jobStateUnpartitioned, "", nil
	}

	mismatch, err := findShapeMismatch(ctx, q)
	if err != nil {
		return jobStateUnknown, "", err
	}
	if mismatch != "" {
		return jobStatePartitionedWrongShape, mismatch, nil
	}
	return jobStateAlreadyPartitioned, "", nil
}

// findShapeMismatch returns a non-empty string describing the first shape
// mismatch found between the live partitioned job table and the target
// schema. Returns an empty string if the shape matches.
func findShapeMismatch(ctx *armadacontext.Context, q pgx.Tx) (string, error) {
	rows, err := q.Query(ctx, `
		SELECT c.relname FROM pg_inherits i
		JOIN pg_class c ON c.oid = i.inhrelid
		JOIN pg_class p ON p.oid = i.inhparent
		WHERE p.relname = 'job' AND p.relnamespace = 'public'::regnamespace
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
			WHERE c.relname = $1 AND c.relnamespace = 'public'::regnamespace
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
	}
	rows, err = q.Query(ctx, `
		SELECT column_name, data_type, is_nullable = 'NO'
		FROM information_schema.columns
		WHERE table_name = 'job' AND table_schema = 'public'
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
		WHERE t.relname = 'job' AND t.relnamespace = 'public'::regnamespace AND c.contype = 'p'
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
			`SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND tablename = 'job' AND indexname = $1)`,
			idx).Scan(&exists); err != nil {
			return "", err
		}
		if !exists {
			return fmt.Sprintf("missing index %s on job", idx), nil
		}
	}

	var activeIdxExists bool
	if err := q.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND tablename = 'job_active' AND indexname = 'idx_job_active_queue_jobset')`,
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

// convertUnpartitionedToPartitioned is the conversion branch: it creates
// job_new as a partitioned table using target_schema.sql, copies data from
// the existing unpartitioned job into job_new (letting Postgres route rows
// to the correct partition), drops the old job, and renames job_new plus
// its partitions and parent indexes to their final names.
func convertUnpartitionedToPartitioned(ctx *armadacontext.Context, tx pgx.Tx) error {
	if _, err := tx.Exec(ctx, `LOCK TABLE job IN ACCESS EXCLUSIVE MODE`); err != nil {
		return errors.Wrap(err, "lock job table")
	}

	ddl := strings.ReplaceAll(targetSchemaSQL, "{{TABLE}}", "job_new")
	if _, err := tx.Exec(ctx, ddl); err != nil {
		return errors.Wrap(err, "create job_new partitioned table")
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO job_new (
			job_id, queue, owner, jobset, cpu, memory, ephemeral_storage, gpu,
			priority, submitted, cancelled, state, last_transition_time,
			last_transition_time_seconds, job_spec, duplicate, priority_class,
			latest_run_id, cancel_reason, namespace, annotations,
			external_job_uri, cancel_user
		)
		SELECT
			job_id, queue, owner, jobset, cpu, memory, ephemeral_storage, gpu,
			priority, submitted, cancelled, state, last_transition_time,
			last_transition_time_seconds, job_spec, duplicate, priority_class,
			latest_run_id, cancel_reason, namespace, annotations,
			external_job_uri, cancel_user
		FROM job
	`); err != nil {
		return errors.Wrap(err, "copy rows from job to job_new")
	}

	if testHookAfterCopy != nil {
		testHookAfterCopy()
	}

	// no CASCADE: loud failure for a destructive operation is safer than silently dropping dependent objects
	if _, err := tx.Exec(ctx, `DROP TABLE job`); err != nil {
		return errors.Wrap(err, "drop old job table")
	}

	renames := []string{
		`ALTER TABLE job_new RENAME TO job`,
		`ALTER TABLE job_new_active RENAME TO job_active`,
		`ALTER TABLE job_new_terminated RENAME TO job_terminated`,
		`ALTER INDEX job_new_pkey RENAME TO job_pkey`,
		`ALTER INDEX idx_job_new_queue_last_transition_time_seconds RENAME TO idx_job_queue_last_transition_time_seconds`,
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
	for _, sqlStmt := range renames {
		if _, err := tx.Exec(ctx, sqlStmt); err != nil {
			return errors.Wrapf(err, "rename: %s", sqlStmt)
		}
	}

	return nil
}
