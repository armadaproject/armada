package schema

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common/armadacontext"
)

const (
	minimumPostgresVersionNum   = 110000
	supportedPostgresVersionNum = 140000
	// requiredFreeDiskHeadroom is the margin added to the estimated size of
	// the new table, to allow for the estimates being low.
	requiredFreeDiskHeadroom = 0.2
)

// PreflightReport describes the job table and the database settings that
// affect converting it to the partitioned shape.
type PreflightReport struct {
	Shape             JobTableShape
	ShapeDetail       string
	ServerVersion     string
	ServerVersionNum  int
	AppliedMigration  int
	ExpectedMigration int
	HeapBytes         int64
	ToastBytes        int64
	IndexBytes        int64
	// LargestIndexBytes estimates the size of idx_job_queue_job_id, which the
	// unpartitioned table lacks, from the largest index it does have.
	LargestIndexBytes int64
	// EstimatedRows is the planner's estimate, or -1 if job has never been
	// analysed.
	EstimatedRows int64
	// ActiveFraction is the estimated fraction of rows in active states, or
	// nil if there are no statistics for the state column.
	ActiveFraction  *float64
	LockHolders     []LockHolder
	TimeoutSettings []string
}

// LockHolder is a session holding or waiting for a lock on job, which LOCK
// TABLE has to wait for.
type LockHolder struct {
	Pid            int
	User           string
	Application    string
	State          string
	Mode           string
	Granted        bool
	TransactionAge time.Duration
	Query          string
}

// Preflight inspects db without changing it. It takes no lock that would
// block other sessions, and gives up after a few seconds if the job table is
// locked, for example by a conversion in progress.
func Preflight(ctx *armadacontext.Context, db DB) (*PreflightReport, error) {
	report := &PreflightReport{}
	err := pgx.BeginTxFunc(ctx, db, pgx.TxOptions{AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SET LOCAL lock_timeout = '5s'"); err != nil {
			return err
		}
		var err error
		report.Shape, report.ShapeDetail, err = DetectJobTableShape(ctx, tx)
		if err != nil {
			return errors.Wrap(err, "detect job table shape")
		}
		if err := tx.QueryRow(ctx, `SELECT current_setting('server_version'), current_setting('server_version_num')::int`).
			Scan(&report.ServerVersion, &report.ServerVersionNum); err != nil {
			return errors.Wrap(err, "read server version")
		}
		report.AppliedMigration, report.ExpectedMigration, err = migrationVersions(ctx, tx)
		if err != nil {
			return err
		}
		if report.Shape != JobTableShapeUnpartitioned {
			return nil
		}
		if err := readJobTableSize(ctx, tx, report); err != nil {
			return err
		}
		if report.ActiveFraction, err = readActiveFraction(ctx, tx); err != nil {
			return err
		}
		if report.LockHolders, err = readLockHolders(ctx, tx); err != nil {
			return err
		}
		report.TimeoutSettings, err = readTimeoutSettings(ctx, tx)
		return err
	})
	if err != nil {
		return nil, errors.Wrap(err, "preflight")
	}
	return report, nil
}

func readJobTableSize(ctx *armadacontext.Context, tx pgx.Tx, report *PreflightReport) error {
	err := tx.QueryRow(ctx, `
		SELECT pg_relation_size(c.oid), coalesce(pg_total_relation_size(nullif(c.reltoastrelid, 0)), 0),
		       pg_indexes_size(c.oid), c.reltuples::bigint,
		       (SELECT coalesce(max(pg_relation_size(i.indexrelid)), 0) FROM pg_index i WHERE i.indrelid = c.oid)
		FROM pg_class c WHERE c.oid = 'job'::regclass
	`).Scan(&report.HeapBytes, &report.ToastBytes, &report.IndexBytes, &report.EstimatedRows, &report.LargestIndexBytes)
	return errors.Wrap(err, "read job table size")
}

func readActiveFraction(ctx *armadacontext.Context, tx pgx.Tx) (*float64, error) {
	var states []int16
	var frequencies []float32
	err := tx.QueryRow(ctx, `
		SELECT most_common_vals::text::smallint[], most_common_freqs
		FROM pg_stats WHERE schemaname = current_schema() AND tablename = 'job' AND attname = 'state'
	`).Scan(&states, &frequencies)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, errors.Wrap(err, "read job state statistics")
	}
	if len(states) == 0 {
		return nil, nil
	}
	var active, total float64
	for i, state := range states {
		total += float64(frequencies[i])
		switch state {
		case 1, 2, 3, 8:
			active += float64(frequencies[i])
		}
	}
	fraction := active / total
	return &fraction, nil
}

func readLockHolders(ctx *armadacontext.Context, tx pgx.Tx) ([]LockHolder, error) {
	rows, err := tx.Query(ctx, `
		SELECT a.pid, coalesce(a.usename, ''), coalesce(a.application_name, ''), coalesce(a.state, ''), l.mode, l.granted,
		       coalesce(extract(epoch FROM now() - a.xact_start), 0)::float8, left(coalesce(a.query, ''), 200)
		FROM pg_locks l
		JOIN pg_stat_activity a ON a.pid = l.pid
		WHERE l.locktype = 'relation'
		AND l.database = (SELECT oid FROM pg_database WHERE datname = current_database())
		AND l.relation = 'job'::regclass
		AND a.pid <> pg_backend_pid()
		ORDER BY a.xact_start
	`)
	if err != nil {
		return nil, errors.Wrap(err, "read locks on job")
	}
	holders, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (LockHolder, error) {
		var h LockHolder
		var ageSeconds float64
		err := row.Scan(&h.Pid, &h.User, &h.Application, &h.State, &h.Mode, &h.Granted, &ageSeconds, &h.Query)
		h.TransactionAge = time.Duration(ageSeconds * float64(time.Second))
		return h, err
	})
	return holders, errors.Wrap(err, "read locks on job")
}

// readTimeoutSettings lists the non-zero statement_timeout and lock_timeout
// settings in effect for this session, other than those set by the session
// itself, and those set for any role in this database or in all databases. This command cannot see which role the
// Lookout ingester uses, so it lists every role.
func readTimeoutSettings(ctx *armadacontext.Context, tx pgx.Tx) ([]string, error) {
	rows, err := tx.Query(ctx, `
		SELECT format('%s = %s (in effect for this session; source: %s)', name, current_setting(name), source)
		FROM pg_settings
		WHERE name IN ('statement_timeout', 'lock_timeout') AND setting <> '0' AND source <> 'session'
		UNION ALL
		SELECT format('%s (set for %s in %s)', config,
		              coalesce('role ' || r.rolname, 'all roles'), coalesce('database ' || d.datname, 'all databases'))
		FROM pg_db_role_setting s
		CROSS JOIN LATERAL unnest(s.setconfig) AS config
		LEFT JOIN pg_roles r ON r.oid = s.setrole
		LEFT JOIN pg_database d ON d.oid = s.setdatabase
		WHERE s.setdatabase IN (0, (SELECT oid FROM pg_database WHERE datname = current_database()))
		AND split_part(config, '=', 1) IN ('statement_timeout', 'lock_timeout')
		AND split_part(config, '=', 2) <> '0'
	`)
	if err != nil {
		return nil, errors.Wrap(err, "read timeout settings")
	}
	settings, err := pgx.CollectRows(rows, pgx.RowTo[string])
	return settings, errors.Wrap(err, "read timeout settings")
}

// RequiredFreeBytes estimates the free disk the conversion needs: a full
// copy of the heap, TOAST data and indexes, plus idx_job_queue_job_id, plus
// headroom.
func (r *PreflightReport) RequiredFreeBytes() int64 {
	return int64(float64(r.newTableBytes()) * (1 + requiredFreeDiskHeadroom))
}

func (r *PreflightReport) newTableBytes() int64 {
	return r.HeapBytes + r.ToastBytes + r.IndexBytes + r.LargestIndexBytes
}

// Blockers returns the reasons why the conversion cannot run, if any.
func (r *PreflightReport) Blockers() []string {
	var blockers []string
	switch r.Shape {
	case JobTableShapeUnpartitioned, JobTableShapePartitioned:
	case JobTableShapeUnexpected:
		blockers = append(blockers, "the job table is partitioned but does not have the expected shape: "+r.ShapeDetail)
	default:
		blockers = append(blockers, fmt.Sprintf("the job table is %s: run lookout --migrateDatabase first", r.Shape))
	}
	if r.ServerVersionNum < minimumPostgresVersionNum {
		blockers = append(blockers, fmt.Sprintf("Postgres %s is too old: partitioning the job table needs Postgres 11 or later", r.ServerVersion))
	}
	if r.Shape == JobTableShapeUnpartitioned && r.AppliedMigration != r.ExpectedMigration {
		blockers = append(blockers, fmt.Sprintf("the database is at migration %d but this version of lookout expects %d: run lookout --migrateDatabase with this version first",
			r.AppliedMigration, r.ExpectedMigration))
	}
	return blockers
}

// Write writes the report for an operator to read.
func (r *PreflightReport) Write(w io.Writer) error {
	var b strings.Builder
	b.WriteString("Lookout job table partitioning preflight\n\n")

	if r.Shape == JobTableShapePartitioned {
		b.WriteString("The job table is already partitioned. There is nothing to do.\n")
		_, err := io.WriteString(w, b.String())
		return err
	}

	fmt.Fprintf(&b, "Job table:          %s\n", r.Shape)
	postgresStatus := "OK"
	if r.ServerVersionNum < minimumPostgresVersionNum {
		postgresStatus = "not supported: Postgres 11 or later is required"
	} else if r.ServerVersionNum < supportedPostgresVersionNum {
		postgresStatus = "WARNING: Postgres 14 or later is supported; older versions are untested"
	}
	fmt.Fprintf(&b, "Postgres:           %s (%s)\n", r.ServerVersion, postgresStatus)
	fmt.Fprintf(&b, "Migration chain:    at %d, this version expects %d\n", r.AppliedMigration, r.ExpectedMigration)

	if r.Shape == JobTableShapeUnpartitioned {
		fmt.Fprintf(&b, "Size of job:        %s (heap %s, TOAST %s, indexes %s)\n",
			formatBytes(r.HeapBytes+r.ToastBytes+r.IndexBytes), formatBytes(r.HeapBytes), formatBytes(r.ToastBytes), formatBytes(r.IndexBytes))
		if r.EstimatedRows >= 0 {
			fmt.Fprintf(&b, "Estimated rows:     %d (from planner statistics)\n", r.EstimatedRows)
		} else {
			b.WriteString("Estimated rows:     unknown: run ANALYZE job for an estimate\n")
		}
		if r.ActiveFraction != nil {
			fmt.Fprintf(&b, "Estimated split:    %.1f%% active, %.1f%% terminated\n", *r.ActiveFraction*100, (1-*r.ActiveFraction)*100)
		} else {
			b.WriteString("Estimated split:    unknown: run ANALYZE job for an estimate\n")
		}

		fmt.Fprintf(&b, "\nRequired free disk: at least %s\n", formatBytes(r.RequiredFreeBytes()))
		fmt.Fprintf(&b, "  This is a full copy of the heap, TOAST data and indexes, plus the new index idx_job_queue_job_id\n"+
			"  (estimated at up to %s), plus %.0f%% headroom. The old table is freed only when the migration commits.\n"+
			"  Postgres cannot see free filesystem space, so check it yourself.\n",
			formatBytes(r.LargestIndexBytes), requiredFreeDiskHeadroom*100)
		fmt.Fprintf(&b, "WAL:                the migration writes roughly %s of WAL, which affects replicas, WAL archiving and backup storage.\n",
			formatBytes(r.newTableBytes()))
		b.WriteString("Duration:           no estimate is available yet. Run the migration against a copy of your database to measure it.\n")

		b.WriteString("\nSessions holding or waiting for locks on job, which the migration must wait for before it can start:\n")
		if len(r.LockHolders) == 0 {
			b.WriteString("  none\n")
		}
		for _, h := range r.LockHolders {
			status := "holds"
			if !h.Granted {
				status = "waits for"
			}
			fmt.Fprintf(&b, "  pid %d (user %q, application %q, %s) %s %s; transaction open for %s: %s\n",
				h.Pid, h.User, h.Application, h.State, status, h.Mode, h.TransactionAge.Round(time.Second), h.Query)
		}

		b.WriteString("\nNon-zero statement_timeout and lock_timeout settings:\n")
		if len(r.TimeoutSettings) == 0 {
			b.WriteString("  none\n")
		}
		for _, setting := range r.TimeoutSettings {
			fmt.Fprintf(&b, "  %s\n", setting)
		}
		if len(r.TimeoutSettings) > 0 {
			b.WriteString("  WARNING: if one of these applies to the Lookout ingester and its fatalInsertionErrors match the resulting\n" +
				"  error, the ingester drops events while the migration runs. In that case, scale the ingester to zero for\n" +
				"  the duration of the migration.\n")
		}

		b.WriteString("\nPulsar: the Lookout ingester's subscription backlog builds up while the migration runs. Make sure Pulsar\n" +
			"retains it for longer than the migration takes: neither a message TTL nor a backlog quota with an eviction\n" +
			"policy may be shorter than the migration. This command cannot check Pulsar.\n")
	}

	blockers := r.Blockers()
	if len(blockers) == 0 {
		b.WriteString("\nResult: ready. Run lookout --partitionJobTable --execute to migrate.\n")
	} else {
		b.WriteString("\nResult: NOT READY\n")
		for _, blocker := range blockers {
			fmt.Fprintf(&b, "  - %s\n", blocker)
		}
	}
	_, err := io.WriteString(w, b.String())
	return err
}

func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	value, exponent := float64(bytes)/unit, 0
	for value >= unit && exponent < 4 {
		value /= unit
		exponent++
	}
	return fmt.Sprintf("%.1f %ciB", value, "KMGTP"[exponent])
}
