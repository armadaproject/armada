package scheduler

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"
)

const (
	leaderElectionTimeout  time.Duration = time.Second
	leaderElectionInterval time.Duration = 10 * time.Millisecond
)

func TestLeaderElection(t *testing.T) {
	numInstances := 10
	for i := 0; i < 10; i++ {
		err := withSetup(func(_ *Queries, db *pgxpool.Pool) error {
			// Create several instances that all try to become leader.
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			g, ctx := errgroup.WithContext(ctx)
			cancels := make([]context.CancelFunc, numInstances)
			ch := make(chan int, numInstances)
			for i := 0; i < numInstances; i++ {
				ctxi, c := context.WithCancel(ctx)
				cancels[i] = c
				i := i
				g.Go(func() error {
					return tryBecomeLeaderService(ctxi, db, i, ch)
				})
			}

			// Wait for one instance to take leadership.
			_, ok := <-ch
			if !ok {
				return errors.New("unexpected channel close")
			}

			// Wait to ensure no other instance also claims leadership.
			timer := time.NewTimer(10 * leaderElectionInterval)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ch:
				return errors.New("unexpected leader change")
			case <-timer.C:
			}

			cancel()
			err := g.Wait()
			if err != nil && !errors.Is(err, context.Canceled) {
				return err
			}
			return nil
		})
		assert.NoError(t, err)
	}
}

func TestLeaderElectionTimeout(t *testing.T) {
	numInstances := 2
	err := withSetup(func(_ *Queries, db *pgxpool.Pool) error {
		// Create several instances that all try to become leader.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		g, ctx := errgroup.WithContext(ctx)
		cancels := make([]context.CancelFunc, numInstances)
		ch := make(chan int, numInstances)
		for i := 0; i < numInstances; i++ {
			ctxi, c := context.WithCancel(ctx)
			cancels[i] = c
			i := i
			g.Go(func() error {
				return tryBecomeLeaderService(ctxi, db, i, ch)
			})
		}

		// Then cancel the leader to test that another instance claims leadership.
		for j := 0; j < numInstances; j++ {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case i, ok := <-ch:
				if !ok {
					return errors.New("unexpected channel close")
				}
				if cancels[i] == nil {
					return errors.Errorf("tried to cancel %d-th instance twice", i)
				}

				// Wait to ensure no other instance also claims leadership.
				timer := time.NewTimer(2 * leaderElectionTimeout)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-ch:
					return errors.New("unexpected leader change")
				case <-timer.C:
				}

				// Cancel the leader to give another instance the chance to claim leadership.
				cancels[i]()
				cancels[i] = nil
			}
		}

		cancel()
		return g.Wait()
	})
	assert.NoError(t, err)
}

func tryBecomeLeaderService(ctx context.Context, db *pgxpool.Pool, i int, ch chan int) error {
	leaderElection := NewLeaderElection(db)
	leaderElection.Interval = leaderElectionInterval
	leaderElection.Timeout = leaderElectionTimeout

	fmt.Printf("[%d] tryBecomeLeaderService started\n", i)
	defer fmt.Printf("[%d] tryBecomeLeaderService exited\n", i)
	err := leaderElection.BecomeLeader(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("[%d] became leader\n", i)

	// We're leader. Put i on ch to let the caller know.
	// Stay leader until ctx is cancelled.
	ch <- i
	err = leaderElection.StayLeader(ctx)
	if errors.Is(err, context.Canceled) {
		return nil
	} else if err != nil {
		return err
	}
	return nil
}
