package scheduler

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
)

type LeaderElection struct {
	Db *pgxpool.Pool
	// Id uniquely identifying this leader in this epoch.
	Id uuid.UUID
	// Interval between database operations.
	// Used both when trying to become leader and when leader and periodically
	// writing into postgres to ensure no other replica tries to become leader.
	Interval time.Duration
	// Time after which this instance may try to become leader
	// if there has been no activity from the current leader.
	Timeout time.Duration
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
// This function checks once per interval if the current leader
// has gone missing and, if so, tries to become leader.
func (srv *LeaderElection) BecomeLeader(ctx context.Context) error {
	ticker := time.NewTicker(srv.Interval)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			isLeader, err := srv.tryBecomeLeader(ctx)
			if err != nil {
				return err
			}
			if isLeader {
				return nil
			}
		}
	}
}

func (srv *LeaderElection) tryBecomeLeader(ctx context.Context) (bool, error) {
	// Update the timestamp associated with this replica.
	// Upserting causes the last_modified field to be updated automatically.
	records := make([]interface{}, 1)
	records[0] = &Leaderelection{
		ID:       srv.Id,
		IsLeader: false,
	}
	err := Upsert(ctx, srv.Db, "leaderelection", LeaderelectionSchema(), records)
	if err != nil {
		return false, err
	}

	// Read back the timestamp postgres generated.
	// To avoid issues due to clock drift, we rely only on postgres generate timestamps.
	queries := New(srv.Db)
	standby, err := queries.SelectReplicaById(ctx, srv.Id)
	if err != nil {
		return false, errors.WithStack(err)
	}

	// Read the timestamp of the leader.
	leader, err := queries.SelectLeader(ctx)
	if errors.Is(err, pgx.ErrNoRows) {
		// If there's no leader, try to take leadership.
		return srv.takeLeadership(ctx)
	} else if err != nil {
		return false, errors.WithStack(err)
	}

	// If we're already the leader, return.
	if leader.ID == standby.ID {
		return true, nil
	}

	// Try to become leader if the difference is greater than timeout.
	// Upsert starts a transaction, which may fail if another instance is trying to become leader.
	duration := standby.LastModified.Sub(leader.LastModified)
	if duration > srv.Timeout {
		return srv.takeLeadership(ctx)
	}

	return false, nil
}

func (srv *LeaderElection) takeLeadership(ctx context.Context) (bool, error) {
	records := make([]interface{}, 2)
	records[0] = &Leaderelection{
		ID:       srv.Id,
		IsLeader: true,
	}
	records[1] = &Leaderelection{
		ID:       srv.Id,
		IsLeader: false,
	}

	// This updates last_modified of the replica that is no longer the leader.
	// That's fine, since this instance is taking over leadership anyway.
	err := Upsert(ctx, srv.Db, "leaderelection", LeaderelectionSchema(), records)
	if err != nil {
		return false, err
	}

	// We've successfully become leader.
	return true, nil
}

// StayLeader writes into postgres every interval to ensure no other replica attempts to become leader.
func (srv *LeaderElection) StayLeader(ctx context.Context, db *pgxpool.Pool, id uuid.UUID, interval time.Duration) error {
	ticker := time.NewTicker(srv.Interval)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:

		}
	}
}

func (srv *LeaderElection) stayLeaderIteration(ctx context.Context) error {
	return srv.Db.BeginTxFunc(ctx, pgx.TxOptions{
		IsoLevel:       pgx.ReadCommitted,
		AccessMode:     pgx.ReadWrite,
		DeferrableMode: pgx.Deferrable,
	}, func(tx pgx.Tx) error {
		// Make sure we're still the leader.
		queries := New(tx)
		leader, err := queries.SelectLeader(ctx)
		if err != nil {
			return errors.WithStack(err)
		}
		if leader.ID != srv.Id {
			return errors.New("another instance has taken over leadership")
		}

		// Perform an upsert to trigger updating last_modified.
		records := make([]interface{}, 1)
		records[0] = &Leaderelection{
			ID:       srv.Id,
			IsLeader: true,
		}
		return CopyProtocolUpsert(ctx, tx, "leaderelection", LeaderelectionSchema(), records)
	})
}
