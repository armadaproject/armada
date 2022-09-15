package scheduler

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/common/logging"
)

// LeaderElection is used by the scheduler to ensure only one instance
// is creating leases at a time.
//
// The leader election process is based on timeouts.
// The leader continually writes its id into postgres.
// Non-leader instances monitor the last_modified value set by postgres for that row.
// If last_modified exceeds some threshold, a non-leader tries to become leader.
// It does so by, in a transaction, marking its own id as the leader.
type LeaderElection struct {
	Db *pgxpool.Pool
	// Id uniquely identifying this leader in this epoch.
	Id uuid.UUID
	// Interval between database operations.
	// Used both while leader and follower.
	Interval time.Duration
	// Time after which this instance may try to become leader
	// if there has been no activity from the current leader.
	Timeout time.Duration
	// Optional logger.
	// If not provided, the default logrus logger is used.
	Logger *logrus.Entry
}

func NewLeaderElection(db *pgxpool.Pool) *LeaderElection {
	return &LeaderElection{
		Id:       uuid.New(),
		Db:       db,
		Interval: 10 * time.Second,
		Timeout:  5 * time.Minute,
	}
}

// BecomeLeader returns once this instance has become the leader.
//
// Checks once per interval if the current leader
// has gone missing and, if so, tries to become leader.
func (srv *LeaderElection) BecomeLeader(ctx context.Context) error {
	log := ctxlogrus.Extract(ctx)
	ticker := time.NewTicker(srv.Interval)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			isLeader, err := srv.tryBecomeLeader(ctx)
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.SerializationFailure {
				// Another instance took leadership concurrently with this one,
				// so the tx of this instance aborted.
				break
			} else if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.LockNotAvailable {
				// Another instance was already holding the table lock,
				// so we need to wait for it to release it.
				break
			} else if err != nil {
				logging.WithStacktrace(log, err).Error("error while trying to become leader")
			} else if isLeader {
				return nil
			}
		}
	}
}

func (srv *LeaderElection) tryBecomeLeader(ctx context.Context) (bool, error) {
	// Update the timestamp associated with this replica.
	// Upserting causes the last_modified field to be set automatically.
	//
	// We do this to make postgres generate a timestamp;
	// to reduce the chance of clock drift, we only use timestamps generated by postgres.
	records := make([]interface{}, 1)
	records[0] = Leaderelection{
		ID:       srv.Id,
		IsLeader: false,
	}
	err := Upsert(ctx, srv.Db, "leaderelection", LeaderelectionSchema(), records)
	if err != nil {
		return false, err
	}

	// Read back the timestamp postgres generated.
	standby, err := New(srv.Db).SelectReplicaById(ctx, srv.Id)
	if err != nil {
		return false, errors.WithStack(err)
	}

	// We need to use the RepeatableRead isolation level to ensure lost
	// updates (i.e., concurrent modification of the leader row) aborts the tx.
	isLeader := false
	err = srv.Db.BeginTxFunc(ctx, pgx.TxOptions{
		IsoLevel:       pgx.RepeatableRead,
		AccessMode:     pgx.ReadWrite,
		DeferrableMode: pgx.Deferrable,
	}, func(tx pgx.Tx) error {

		// Lock the table for writing.
		// Since txs are aborted on lost update under RepeatableRead,
		// all txs may be aborted when many instances concurrently try to become leader.
		// This lock ensures only 1 instance tries to update the leader row at a time.
		_, err := tx.Exec(ctx, "LOCK TABLE leaderelection IN EXCLUSIVE MODE;")
		if err != nil {
			return errors.Wrap(err, "failed to acquire leaderelection lock")
		}

		// Read the timestamp of the leader.
		leader, err := New(tx).SelectLeader(ctx)
		if errors.Is(err, pgx.ErrNoRows) {
			// If there's no leader, try to take leadership.
			err := srv.takeLeadership(ctx, tx, leader.ID)
			if err == nil {
				isLeader = true
			}
			return err
		} else if err != nil {
			return err
		}

		// If we're already the leader, return.
		if leader.ID == standby.ID {
			isLeader = true
			return nil
		}

		// If the difference is greater than timeout, try to take leadership
		duration := standby.LastModified.Sub(leader.LastModified)
		if duration > srv.Timeout {
			err := srv.takeLeadership(ctx, tx, leader.ID)
			if err == nil {
				isLeader = true
			}
			return err
		}

		return nil
	})

	return isLeader, err
}

func (srv *LeaderElection) takeLeadership(ctx context.Context, tx pgx.Tx, leaderId uuid.UUID) error {
	// Upsert starts a transaction,
	// which should fail if another transaction concurrently tries to become leader.
	records := make([]interface{}, 2)
	records[0] = Leaderelection{
		ID:       srv.Id,
		IsLeader: true,
	}
	records[1] = Leaderelection{
		ID:       leaderId,
		IsLeader: false,
	}

	// This updates last_modified of the replica that is no longer the leader.
	// That's fine, since this instance is taking over leadership anyway.
	return CopyProtocolUpsert(ctx, tx, "leaderelection", LeaderelectionSchema(), records)
}

type ErrLostLeadership struct {
	Id uuid.UUID
}

func (err *ErrLostLeadership) Error() string {
	return fmt.Sprintf("instance with id %s has taken over leadership", err.Id)
}

// StayLeader writes into postgres every interval to ensure no other replica attempts to become leader.
func (srv *LeaderElection) StayLeader(ctx context.Context) error {
	log := ctxlogrus.Extract(ctx)
	ticker := time.NewTicker(srv.Interval)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			err := srv.stayLeaderIteration(ctx)
			if errors.Is(err, &ErrLostLeadership{}) {
				return err
			} else if err != nil {
				logging.WithStacktrace(log, err).Error("error staying leader")
			}
		}
	}
}

func (srv *LeaderElection) stayLeaderIteration(ctx context.Context) error {
	return srv.Db.BeginTxFunc(ctx, pgx.TxOptions{
		// Need to use the RepeatableRead isolation level to ensure
		// the tx is aborted on concurrent modification.
		IsoLevel:       pgx.RepeatableRead,
		AccessMode:     pgx.ReadWrite,
		DeferrableMode: pgx.Deferrable,
	}, func(tx pgx.Tx) error {
		// Check that we're still the leader.
		queries := New(tx)
		leader, err := queries.SelectLeader(ctx)
		if err != nil {
			return errors.WithStack(err)
		}
		if leader.ID != srv.Id {
			return errors.WithStack(&ErrLostLeadership{
				Id: leader.ID,
			})
		}

		// Perform an upsert to trigger updating last_modified.
		records := make([]interface{}, 1)
		records[0] = Leaderelection{
			ID:       srv.Id,
			IsLeader: true,
		}
		return CopyProtocolUpsert(ctx, tx, "leaderelection", LeaderelectionSchema(), records)
	})
}
