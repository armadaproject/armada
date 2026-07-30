package queryapi

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	log "github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/server/queryapi/database"
)

// defaultMirrorMaxInFlight bounds concurrently replayed mirror queries when no
// limit is configured.
const defaultMirrorMaxInFlight = 100

// mirrorTimeout bounds how long a single replayed mirror query may run before
// it is abandoned. Mirror work uses a detached context so it is not cancelled
// when the originating request returns.
const mirrorTimeout = 30 * time.Second

// QueryDB is the subset of *pgxpool.Pool that QueryApi depends on. Both the
// real pool and the mirroring wrapper satisfy it.
type QueryDB interface {
	database.DBTX
	BeginTx(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error)
}

// mirroringDB wraps a primary QueryDB and asynchronously replays every query
// against a mirror pool (a second database) for performance evaluation.
// Results from the primary are returned unchanged; mirror results and errors
// are discarded. Replaying is fire-and-forget and never blocks or affects the
// primary query path.
type mirroringDB struct {
	primary  QueryDB
	mirror   *pgxpool.Pool
	inFlight chan struct{}
}

// NewMirroringDB returns a QueryDB that delegates to primary and additionally
// replays each query against mirror in a bounded, fire-and-forget manner.
func NewMirroringDB(primary QueryDB, mirror *pgxpool.Pool, maxInFlight int) QueryDB {
	if maxInFlight <= 0 {
		maxInFlight = defaultMirrorMaxInFlight
	}
	return &mirroringDB{
		primary:  primary,
		mirror:   mirror,
		inFlight: make(chan struct{}, maxInFlight),
	}
}

func (m *mirroringDB) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	m.replay(sql, args)
	return m.primary.Exec(ctx, sql, args...)
}

func (m *mirroringDB) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	m.replay(sql, args)
	return m.primary.Query(ctx, sql, args...)
}

func (m *mirroringDB) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	m.replay(sql, args)
	return m.primary.QueryRow(ctx, sql, args...)
}

// BeginTx returns the real primary transaction wrapped so that statements
// issued within it are also mirrored. The mirror replays each statement
// non-transactionally; it only needs representative read load, not
// transactional fidelity.
func (m *mirroringDB) BeginTx(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error) {
	tx, err := m.primary.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &mirroringTx{Tx: tx, db: m}, nil
}

// replay schedules the given query to run against the mirror pool in the
// background. It is dropped if the in-flight bound is reached, so a slow mirror
// database can never leak goroutines or slow the primary path.
func (m *mirroringDB) replay(sql string, args []any) {
	select {
	case m.inFlight <- struct{}{}:
	default:
		log.Debug("dropping mirrored query; in-flight bound reached")
		return
	}
	// Copy the args slice so the background goroutine never shares mutable
	// state with the synchronous primary call.
	argsCopy := append([]any(nil), args...)
	go func() {
		defer func() { <-m.inFlight }()
		ctx, cancel := context.WithTimeout(context.Background(), mirrorTimeout)
		defer cancel()
		rows, err := m.mirror.Query(ctx, sql, argsCopy...)
		if err != nil {
			log.WithError(err).Debug("mirrored query failed")
			return
		}
		// Drain and close so the connection is returned to the pool; results
		// are intentionally discarded.
		for rows.Next() {
		}
		rows.Close()
	}()
}

// mirroringTx wraps a primary pgx.Tx, mirroring statements issued within it
// while delegating all other transaction behaviour (Commit, Rollback, etc.) to
// the embedded transaction.
type mirroringTx struct {
	pgx.Tx
	db *mirroringDB
}

func (t *mirroringTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	t.db.replay(sql, args)
	return t.Tx.Exec(ctx, sql, args...)
}

func (t *mirroringTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	t.db.replay(sql, args)
	return t.Tx.Query(ctx, sql, args...)
}

func (t *mirroringTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	t.db.replay(sql, args)
	return t.Tx.QueryRow(ctx, sql, args...)
}
