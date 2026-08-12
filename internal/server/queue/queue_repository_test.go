package queue

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/database/lookout"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client/queue"
)

var (
	queueA = queue.Queue{
		Name:                              "queueA",
		PriorityFactor:                    1000,
		Permissions:                       []queue.Permissions{},
		ResourceLimitsByPriorityClassName: map[string]api.PriorityClassResourceLimits{},
	}
	queueB = queue.Queue{
		Name:                              "queueB",
		PriorityFactor:                    2000,
		Permissions:                       []queue.Permissions{},
		ResourceLimitsByPriorityClassName: map[string]api.PriorityClassResourceLimits{},
	}
	twoQueues = []queue.Queue{queueA, queueB}
)

func TestGetAllQueues(t *testing.T) {
	tests := map[string]struct {
		queues []queue.Queue
	}{
		"Empty Database": {
			queues: []queue.Queue{},
		},
		"One Queue": {
			queues: []queue.Queue{queueA},
		},
		"Two Queues": {
			queues: []queue.Queue{queueA, queueB},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			withQueueRepo(t, func(ctx *armadacontext.Context, repo *PostgresQueueRepository) {
				for _, q := range tc.queues {
					require.NoError(t, repo.CreateQueue(ctx, q))
				}
				fetched, err := repo.GetAllQueues(ctx)
				assert.NoError(t, err)
				assert.Equal(t, tc.queues, fetched)
			})
		})
	}
}

func TestDeleteQueue(t *testing.T) {
	tests := map[string]struct {
		intialQueues  []queue.Queue
		queueToDelete string
	}{
		"Empty Database": {
			queueToDelete: "queueA",
		},
		"QueueNot present": {
			intialQueues:  twoQueues,
			queueToDelete: "queueC",
		},
		"Delete Queue": {
			intialQueues:  twoQueues,
			queueToDelete: "queueA",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			withQueueRepo(t, func(ctx *armadacontext.Context, repo *PostgresQueueRepository) {
				for _, q := range tc.intialQueues {
					require.NoError(t, repo.CreateQueue(ctx, q))
				}
				require.NoError(t, repo.DeleteQueue(ctx, tc.queueToDelete))

				_, err := repo.GetQueue(ctx, tc.queueToDelete)
				assert.Equal(t, &ErrQueueNotFound{QueueName: tc.queueToDelete}, err)
			})
		})
	}
}

func TestGetAndUpdateQueue(t *testing.T) {
	tests := map[string]struct {
		intialQueues  []queue.Queue
		queueToUpdate queue.Queue
	}{
		"Empty Database": {
			queueToUpdate: queueA,
		},
		"Queue Doesn't Exist": {
			intialQueues: twoQueues,
			queueToUpdate: queue.Queue{
				Name:                              "queueC",
				Permissions:                       []queue.Permissions{},
				PriorityFactor:                    1,
				ResourceLimitsByPriorityClassName: map[string]api.PriorityClassResourceLimits{},
			},
		},
		"Queue Does Exist": {
			intialQueues: twoQueues,
			queueToUpdate: queue.Queue{
				Name:                              "queueA",
				PriorityFactor:                    queueA.PriorityFactor + 100,
				Permissions:                       []queue.Permissions{},
				ResourceLimitsByPriorityClassName: map[string]api.PriorityClassResourceLimits{},
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			withQueueRepo(t, func(ctx *armadacontext.Context, repo *PostgresQueueRepository) {
				for _, q := range tc.intialQueues {
					require.NoError(t, repo.CreateQueue(ctx, q))
				}
				assert.NoError(t, repo.UpdateQueue(ctx, tc.queueToUpdate))
				fetched, err := repo.GetQueue(ctx, tc.queueToUpdate.Name)
				require.NoError(t, err)
				assert.Equal(t, tc.queueToUpdate, fetched)
			})
		})
	}
}

// withQueueRepo runs action against a repository on a fresh migrated database.
func withQueueRepo(t *testing.T, action func(ctx *armadacontext.Context, repo *PostgresQueueRepository)) {
	t.Helper()
	withQueueRepoAndPolicies(t, nil, action)
}

// withQueueRepoAndPolicies also creates the named policies, so a queue can attach to them.
func withQueueRepoAndPolicies(t *testing.T, policies []string, action func(ctx *armadacontext.Context, repo *PostgresQueueRepository)) {
	t.Helper()
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 20*time.Second)
	t.Cleanup(cancel)
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		for _, name := range policies {
			_, err := db.Exec(ctx, "INSERT INTO retry_policy (name, definition) VALUES ($1, $2)", name, []byte{})
			require.NoError(t, err)
		}
		action(ctx, NewPostgresQueueRepository(db))
		return nil
	})
	require.NoError(t, err)
}

func TestErrUnknownRetryPolicies_Error(t *testing.T) {
	tests := map[string]struct {
		names []string
		want  string
	}{
		"no names":      {want: "one or more referenced retry policies do not exist"},
		"one name":      {names: []string{"p1"}, want: `retry policies do not exist: "p1"`},
		"stray space":   {names: []string{" p1"}, want: `retry policies do not exist: " p1"`},
		"several names": {names: []string{"p1", "p2"}, want: `retry policies do not exist: "p1", "p2"`},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, (&ErrUnknownRetryPolicies{Names: tc.names}).Error())
		})
	}
}

// Attachments must read back exactly as written, through both read paths.
// Each case creates the queue with the first list, then updates with the rest.
func TestQueueRetryPolicies_WriteThenRead(t *testing.T) {
	tests := map[string]struct {
		writes [][]string
		want   []string
	}{
		"submitted order is preserved, not sorted": {
			writes: [][]string{{"p2", "p1"}},
			want:   []string{"p2", "p1"},
		},
		"a repeated name collapses onto its first position": {
			writes: [][]string{{"p2", "p1", "p2"}},
			want:   []string{"p2", "p1"},
		},
		"an update replaces the previous attachments": {
			writes: [][]string{{"p1"}, {"p2"}},
			want:   []string{"p2"},
		},
		"an update without policies clears them": {
			writes: [][]string{{"p1"}, nil},
			want:   nil,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			withQueueRepoAndPolicies(t, []string{"p1", "p2"}, func(ctx *armadacontext.Context, repo *PostgresQueueRepository) {
				for i, write := range tc.writes {
					q := queue.Queue{Name: "q1", PriorityFactor: 1, RetryPolicies: write}
					if i == 0 {
						require.NoError(t, repo.CreateQueue(ctx, q))
					} else {
						require.NoError(t, repo.UpdateQueue(ctx, q))
					}
				}

				fetched, err := repo.GetQueue(ctx, "q1")
				require.NoError(t, err)
				assert.Equal(t, tc.want, fetched.RetryPolicies, "GetQueue")

				all, err := repo.GetAllQueues(ctx)
				require.NoError(t, err)
				require.Len(t, all, 1)
				assert.Equal(t, tc.want, all[0].RetryPolicies, "GetAllQueues")
			})
		})
	}
}

func TestQueueRetryPolicies_UnknownPolicyRejectedWithNames(t *testing.T) {
	withQueueRepoAndPolicies(t, []string{"p1"}, func(ctx *armadacontext.Context, repo *PostgresQueueRepository) {
		q := queue.Queue{Name: "q1", PriorityFactor: 1, RetryPolicies: []string{"p1", "ghost"}}
		err := repo.CreateQueue(ctx, q)
		var eu *ErrUnknownRetryPolicies
		require.ErrorAs(t, err, &eu)
		assert.Equal(t, []string{"ghost"}, eu.Names)

		// The failed write must not leave partial state behind.
		_, err = repo.GetQueue(ctx, "q1")
		assert.IsType(t, &ErrQueueNotFound{}, err)
	})
}

// syncRetryPolicies deletes before it inserts, so a rejection escaping the
// transaction would wipe the queue's existing attachments.
func TestQueueRetryPolicies_FailedUpdatePreservesAttachments(t *testing.T) {
	withQueueRepoAndPolicies(t, []string{"p1", "p2"}, func(ctx *armadacontext.Context, repo *PostgresQueueRepository) {
		original := queue.Queue{Name: "q1", PriorityFactor: 7, RetryPolicies: []string{"p1", "p2"}}
		require.NoError(t, repo.CreateQueue(ctx, original))

		err := repo.UpdateQueue(ctx, queue.Queue{Name: "q1", PriorityFactor: 9, RetryPolicies: []string{"p1", "ghost"}})
		var eu *ErrUnknownRetryPolicies
		require.ErrorAs(t, err, &eu)
		assert.Equal(t, []string{"ghost"}, eu.Names)

		fetched, err := repo.GetQueue(ctx, "q1")
		require.NoError(t, err)
		assert.Equal(t, []string{"p1", "p2"}, fetched.RetryPolicies, "attachments must survive a rejected update")
		assert.Equal(t, queue.PriorityFactor(7), fetched.PriorityFactor, "the queue definition must survive too")
	})
}

// Deleting a queue must take its attachment rows with it, otherwise a queue
// recreated under the same name would inherit the old policies.
func TestQueueRetryPolicies_QueueDeleteCascades(t *testing.T) {
	withQueueRepoAndPolicies(t, []string{"p1"}, func(ctx *armadacontext.Context, repo *PostgresQueueRepository) {
		require.NoError(t, repo.CreateQueue(ctx, queue.Queue{Name: "q1", PriorityFactor: 1, RetryPolicies: []string{"p1"}}))
		require.NoError(t, repo.DeleteQueue(ctx, "q1"))

		require.NoError(t, repo.CreateQueue(ctx, queue.Queue{Name: "q1", PriorityFactor: 1}))
		fetched, err := repo.GetQueue(ctx, "q1")
		require.NoError(t, err)
		assert.Empty(t, fetched.RetryPolicies, "a recreated queue must not inherit the deleted queue's policies")
	})
}

// Reads compose this field from the join table, so only inspecting the stored
// bytes can catch the blob carrying policy names as well.
func TestQueueRetryPolicies_NotPersistedInQueueDefinition(t *testing.T) {
	withQueueRepoAndPolicies(t, []string{"p1"}, func(ctx *armadacontext.Context, repo *PostgresQueueRepository) {
		require.NoError(t, repo.CreateQueue(ctx, queue.Queue{Name: "q1", PriorityFactor: 1, RetryPolicies: []string{"p1"}}))

		var definition []byte
		require.NoError(t, repo.db.QueryRow(ctx, "SELECT definition FROM queue WHERE name = $1", "q1").Scan(&definition))
		assert.NotContains(t, string(definition), "p1", "proto stores strings verbatim, so a hit means the blob still carries the attachment")
	})
}

func TestQueueRetryPolicies_CordonPreservesAttachments(t *testing.T) {
	withQueueRepoAndPolicies(t, []string{"p1"}, func(ctx *armadacontext.Context, repo *PostgresQueueRepository) {
		require.NoError(t, repo.CreateQueue(ctx, queue.Queue{Name: "q1", PriorityFactor: 1, RetryPolicies: []string{"p1"}}))
		require.NoError(t, repo.CordonQueue(ctx, "q1"))

		fetched, err := repo.GetQueue(ctx, "q1")
		require.NoError(t, err)
		assert.True(t, fetched.Cordoned)
		assert.Equal(t, []string{"p1"}, fetched.RetryPolicies)

		require.NoError(t, repo.UncordonQueue(ctx, "q1"))
		fetched, err = repo.GetQueue(ctx, "q1")
		require.NoError(t, err)
		assert.False(t, fetched.Cordoned)
		assert.Equal(t, []string{"p1"}, fetched.RetryPolicies)
	})
}

// upsertQueue recognises the FK violation by constraint name, so the migration
// names it explicitly and this guards against a silent rename.
func TestQueueRetryPolicies_ForeignKeyConstraintIsNamed(t *testing.T) {
	withQueueRepo(t, func(ctx *armadacontext.Context, repo *PostgresQueueRepository) {
		var count int
		require.NoError(t, repo.db.QueryRow(ctx,
			"SELECT count(*) FROM pg_constraint WHERE conname = $1 AND contype = 'f'",
			retryPolicyForeignKey).Scan(&count))
		assert.Equal(t, 1, count, "upsertQueue matches on this exact constraint name")
	})
}
