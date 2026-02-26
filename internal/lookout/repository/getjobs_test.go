package repository

import (
	"fmt"
	"testing"
	"time"

	clock "k8s.io/utils/clock/testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/common/database/lookout"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/lookout/model"
	"github.com/armadaproject/armada/internal/lookoutingester/instructions"
	"github.com/armadaproject/armada/internal/lookoutingester/lookoutdb"
	"github.com/armadaproject/armada/internal/lookoutingester/metrics"
)

const (
	jobId      = "01f3j0g1md4qx7z5qb148qnh4d"
	queue      = "queue-1"
	jobSet     = "job-set-1"
	cluster    = "cluster-1"
	pool       = "pool-1"
	owner      = "user-1"
	cancelUser = "canceluser"
	namespace  = "namespace-1"
	priority   = 12

	userAnnotationPrefix = "armadaproject.io/"
)

var (
	baseTime, _      = time.Parse("2006-01-02T15:04:05.000Z", "2022-03-01T15:04:05.000Z")
	cpu              = resource.MustParse("15")
	memory           = resource.MustParse("48Gi")
	ephemeralStorage = resource.MustParse("100Gi")
	gpu              = resource.MustParse("8")
	priorityClass    = "default"
	runId            = "123e4567-e89b-12d3-a456-426614174001"
	node             = "node-1"
	basicJobOpts     = &JobOptions{
		Priority:         priority,
		PriorityClass:    priorityClass,
		Cpu:              cpu,
		Memory:           memory,
		EphemeralStorage: ephemeralStorage,
		Gpu:              gpu,
	}
)

func withGetJobsSetup(f func(*instructions.InstructionConverter, *lookoutdb.LookoutDb, *SqlGetJobsRepository, *clock.FakeClock) error) error {
	testClock := clock.NewFakeClock(time.Now())
	return lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		converter := instructions.NewInstructionConverter(metrics.Get().Metrics, userAnnotationPrefix, []string{}, &compress.NoOpCompressor{})
		store := lookoutdb.NewLookoutDb(db, nil, metrics.Get(), 10, 10)
		repo := NewSqlGetJobsRepository(db)
		repo.clock = testClock
		return f(converter, store, repo, testClock)
	})
}

func TestGetJobsSingle(t *testing.T) {
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository, testClock *clock.FakeClock) error {
		job := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				JobId:            jobId,
				Priority:         priority,
				PriorityClass:    "other-than-default",
				Cpu:              cpu,
				Memory:           memory,
				EphemeralStorage: ephemeralStorage,
				Gpu:              gpu,
				Annotations: map[string]string{
					"step_path": "/1/2/3",
					"hello":     "world",
				},
			}).
			Lease(runId, cluster, node, pool, baseTime).
			Pending(runId, cluster, baseTime).
			Running(runId, node, baseTime).
			RunSucceeded(runId, baseTime).
			Succeeded(baseTime).
			Build().
			Job()

		result, err := repo.GetJobs(armadacontext.TODO(), []*model.Filter{}, false, &model.Order{}, 0, 1)
		require.NoError(t, err)
		require.Len(t, result.Jobs, 1)
		assert.Equal(t, job, result.Jobs[0])
		return nil
	})
	require.NoError(t, err)
}

func TestGetJobsMultipleRuns(t *testing.T) {
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository, testClock *clock.FakeClock) error {
		firstRunId := uuid.NewString()
		secondRunId := uuid.NewString()

		job := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, baseTime, basicJobOpts).
			Lease(firstRunId, cluster, node, pool, baseTime).
			Pending(firstRunId, cluster, baseTime).
			Lease(secondRunId, cluster, node, pool, baseTime.Add(time.Second)).
			Pending(secondRunId, cluster, baseTime.Add(time.Second)).
			Lease(runId, cluster, node, pool, baseTime.Add(2*time.Second)).
			Pending(runId, cluster, baseTime.Add(2*time.Second)).
			Running(runId, node, baseTime.Add(2*time.Second)).
			RunSucceeded(runId, baseTime.Add(2*time.Second)).
			Succeeded(baseTime.Add(2 * time.Second)).
			Build().
			Job()

		// Runs should be sorted from oldest -> newest
		result, err := repo.GetJobs(armadacontext.TODO(), []*model.Filter{}, false, &model.Order{}, 0, 1)
		require.NoError(t, err)
		require.Len(t, result.Jobs, 1)
		assert.Equal(t, job, result.Jobs[0])
		return nil
	})
	require.NoError(t, err)
}

func TestOrderByUnsupportedField(t *testing.T) {
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository, testClock *clock.FakeClock) error {
		_, err := repo.GetJobs(
			armadacontext.TODO(),
			[]*model.Filter{},
			false,
			&model.Order{
				Field:     "someField",
				Direction: "ASC",
			},
			0,
			10,
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "column for field someField not found")
		return nil
	})
	require.NoError(t, err)
}

func TestOrderByUnsupportedDirection(t *testing.T) {
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository, testClock *clock.FakeClock) error {
		_, err := repo.GetJobs(
			armadacontext.TODO(),
			[]*model.Filter{},
			false,
			&model.Order{
				Field:     "jobId",
				Direction: "INTERLEAVED",
			},
			0,
			10,
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "direction INTERLEAVED is not a valid sort direction")
		return nil
	})
	require.NoError(t, err)
}

// Since job ids are ULIDs, it is comparable to sorting by submission time
func TestGetJobsOrderByJobId(t *testing.T) {
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository, testClock *clock.FakeClock) error {
		firstId := "01f3j0g1md4qx7z5qb148qnh4d"
		secondId := "01f3j0g1md4qx7z5qb148qnjjj"
		thirdId := "01f3j0g1md4qx7z5qb148qnmmm"

		third := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				JobId: thirdId,
			}).
			Build().
			Job()

		second := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				JobId: secondId,
			}).
			Build().
			Job()

		first := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				JobId: firstId,
			}).
			Build().
			Job()

		t.Run("ascending order", func(t *testing.T) {
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{},
				false,
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, 3)
			assert.Equal(t, first, result.Jobs[0])
			assert.Equal(t, second, result.Jobs[1])
			assert.Equal(t, third, result.Jobs[2])
		})

		t.Run("descending order", func(t *testing.T) {
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{},
				false,
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionDesc,
				},
				0,
				10,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, 3)
			assert.Equal(t, third, result.Jobs[0])
			assert.Equal(t, second, result.Jobs[1])
			assert.Equal(t, first, result.Jobs[2])
		})

		return nil
	})
	require.NoError(t, err)
}

func TestGetJobsOrderBySubmissionTime(t *testing.T) {
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository, testClock *clock.FakeClock) error {
		third := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, baseTime.Add(3*time.Second), basicJobOpts).
			Build().
			Job()

		second := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, baseTime.Add(2*time.Second), basicJobOpts).
			Build().
			Job()

		first := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, baseTime, basicJobOpts).
			Build().
			Job()

		t.Run("ascending order", func(t *testing.T) {
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{},
				false,
				&model.Order{
					Field:     "submitted",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, 3)
			assert.Equal(t, first, result.Jobs[0])
			assert.Equal(t, second, result.Jobs[1])
			assert.Equal(t, third, result.Jobs[2])
		})

		t.Run("descending order", func(t *testing.T) {
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{},
				false,
				&model.Order{
					Field:     "submitted",
					Direction: model.DirectionDesc,
				},
				0,
				10,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, 3)
			assert.Equal(t, third, result.Jobs[0])
			assert.Equal(t, second, result.Jobs[1])
			assert.Equal(t, first, result.Jobs[2])
		})

		return nil
	})
	require.NoError(t, err)
}

func TestGetJobsOrderByLastTransitionTime(t *testing.T) {
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository, testClock *clock.FakeClock) error {
		runId1 := uuid.NewString()
		third := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, baseTime, basicJobOpts).
			Lease(runId1, cluster, node, pool, baseTime).
			Pending(runId1, cluster, baseTime).
			Running(runId1, node, baseTime.Add(3*time.Minute)).
			Build().
			Job()

		runId2 := uuid.NewString()
		second := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, baseTime, basicJobOpts).
			Lease(runId2, cluster, node, pool, baseTime.Add(2*time.Minute)).
			Pending(runId2, cluster, baseTime.Add(2*time.Minute)).
			Build().
			Job()

		first := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, baseTime, basicJobOpts).
			Build().
			Job()

		t.Run("ascending order", func(t *testing.T) {
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{},
				false,
				&model.Order{
					Field:     "lastTransitionTime",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, 3)
			assert.Equal(t, first, result.Jobs[0])
			assert.Equal(t, second, result.Jobs[1])
			assert.Equal(t, third, result.Jobs[2])
		})

		t.Run("descending order", func(t *testing.T) {
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{},
				false,
				&model.Order{
					Field:     "lastTransitionTime",
					Direction: model.DirectionDesc,
				},
				0,
				10,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, 3)
			assert.Equal(t, third, result.Jobs[0])
			assert.Equal(t, second, result.Jobs[1])
			assert.Equal(t, first, result.Jobs[2])
		})

		return nil
	})
	require.NoError(t, err)
}

func TestFilterByUnsupportedField(t *testing.T) {
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository, testClock *clock.FakeClock) error {
		_, err := repo.GetJobs(
			armadacontext.TODO(),
			[]*model.Filter{{
				Field: "someField",
				Match: model.MatchExact,
				Value: "something",
			}},
			false,
			&model.Order{},
			0,
			10,
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "column for field someField not found")
		return nil
	})
	require.NoError(t, err)
}

func TestFilterByUnsupportedMatch(t *testing.T) {
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository, testClock *clock.FakeClock) error {
		_, err := repo.GetJobs(
			armadacontext.TODO(),
			[]*model.Filter{{
				Field: "jobId",
				Match: model.MatchLessThan,
				Value: "something",
			}},
			false,
			&model.Order{},
			0,
			10,
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), fmt.Sprintf("%s is not supported for field jobId", model.MatchLessThan))

		return nil
	})
	require.NoError(t, err)
}

func TestGetJobsById(t *testing.T) {
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository, testClock *clock.FakeClock) error {
		job := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{JobId: jobId}).
			Build().
			Job()

		_ = NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{JobId: "01f3j0g1md4qx7z5qb148qnaaa"}).
			Build().
			Job()

		_ = NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{JobId: "01f3j0g1md4qx7z5qb148qnbbb"}).
			Build().
			Job()

		t.Run("exact", func(t *testing.T) {
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{{
					Field: "jobId",
					Match: model.MatchExact,
					Value: jobId,
				}},
				false,
				&model.Order{},
				0,
				10,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, 1)
			assert.Equal(t, job, result.Jobs[0])
		})

		return nil
	})
	require.NoError(t, err)
}

func TestGetJobsByQueue(t *testing.T) {
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository, testClock *clock.FakeClock) error {
		job := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, baseTime, basicJobOpts).
			Build().
			Job()

		job2 := NewJobSimulatorWithClock(converter, store, testClock).
			Submit("queue-2", jobSet, owner, namespace, baseTime, basicJobOpts).
			Build().
			Job()

		job3 := NewJobSimulatorWithClock(converter, store, testClock).
			Submit("queue-3", jobSet, owner, namespace, baseTime, basicJobOpts).
			Build().
			Job()

		job4 := NewJobSimulatorWithClock(converter, store, testClock).
			Submit("other-queue", jobSet, owner, namespace, baseTime, basicJobOpts).
			Build().
			Job()

		_ = NewJobSimulatorWithClock(converter, store, testClock).
			Submit("something-else", jobSet, owner, namespace, baseTime, basicJobOpts).
			Build().
			Job()

		t.Run("exact", func(t *testing.T) {
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{{
					Field: "queue",
					Match: model.MatchExact,
					Value: queue,
				}},
				false,
				&model.Order{},
				0,
				10,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, 1)
			assert.Equal(t, job, result.Jobs[0])
		})

		t.Run("startsWith", func(t *testing.T) {
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{{
					Field: "queue",
					Match: model.MatchStartsWith,
					Value: "queue-",
				}},
				false,
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, 3)
			assert.Equal(t, job, result.Jobs[0])
			assert.Equal(t, job2, result.Jobs[1])
			assert.Equal(t, job3, result.Jobs[2])
		})

		t.Run("contains", func(t *testing.T) {
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{{
					Field: "queue",
					Match: model.MatchContains,
					Value: "queue",
				}},
				false,
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, 4)
			assert.Equal(t, job, result.Jobs[0])
			assert.Equal(t, job2, result.Jobs[1])
			assert.Equal(t, job3, result.Jobs[2])
			assert.Equal(t, job4, result.Jobs[3])
		})

		return nil
	})
	require.NoError(t, err)
}

func TestGetJobsByJobSet(t *testing.T) {
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository, testClock *clock.FakeClock) error {
		job := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, "job\\set\\1", owner, namespace, baseTime, basicJobOpts).
			Build().
			Job()

		job2 := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, "job\\set\\2", owner, namespace, baseTime, basicJobOpts).
			Build().
			Job()

		job3 := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, "job\\set\\3", owner, namespace, baseTime, basicJobOpts).
			Build().
			Job()

		job4 := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, "other-job\\set", owner, namespace, baseTime, basicJobOpts).
			Build().
			Job()

		_ = NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, "something-else", owner, namespace, baseTime, basicJobOpts).
			Build().
			Job()

		t.Run("exact", func(t *testing.T) {
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{{
					Field: "jobSet",
					Match: model.MatchExact,
					Value: "job\\set\\1",
				}},
				false,
				&model.Order{},
				0,
				10,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, 1)
			assert.Equal(t, job, result.Jobs[0])
		})

		t.Run("startsWith", func(t *testing.T) {
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{{
					Field: "jobSet",
					Match: model.MatchStartsWith,
					Value: "job\\set\\",
				}},
				false,
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, 3)
			assert.Equal(t, job, result.Jobs[0])
			assert.Equal(t, job2, result.Jobs[1])
			assert.Equal(t, job3, result.Jobs[2])
		})

		t.Run("contains", func(t *testing.T) {
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{{
					Field: "jobSet",
					Match: model.MatchContains,
					Value: "job\\set",
				}},
				false,
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, 4)
			assert.Equal(t, job, result.Jobs[0])
			assert.Equal(t, job2, result.Jobs[1])
			assert.Equal(t, job3, result.Jobs[2])
			assert.Equal(t, job4, result.Jobs[3])
		})

		return nil
	})
	require.NoError(t, err)
}

func TestGetJobsByOwner(t *testing.T) {
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository, testClock *clock.FakeClock) error {
		job := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, baseTime, basicJobOpts).
			Build().
			Job()

		job2 := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, "user-2", namespace, baseTime, basicJobOpts).
			Build().
			Job()

		job3 := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, "user-3", namespace, baseTime, basicJobOpts).
			Build().
			Job()

		job4 := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, "other-user", namespace, baseTime, basicJobOpts).
			Build().
			Job()

		_ = NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, "something-else", namespace, baseTime, basicJobOpts).
			Build().
			Job()

		t.Run("exact", func(t *testing.T) {
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{{
					Field: "owner",
					Match: model.MatchExact,
					Value: owner,
				}},
				false,
				&model.Order{},
				0,
				10,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, 1)
			assert.Equal(t, job, result.Jobs[0])
		})

		t.Run("startsWith", func(t *testing.T) {
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{{
					Field: "owner",
					Match: model.MatchStartsWith,
					Value: "user-",
				}},
				false,
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, 3)
			assert.Equal(t, job, result.Jobs[0])
			assert.Equal(t, job2, result.Jobs[1])
			assert.Equal(t, job3, result.Jobs[2])
		})

		t.Run("contains", func(t *testing.T) {
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{{
					Field: "owner",
					Match: model.MatchContains,
					Value: "user",
				}},
				false,
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, 4)
			assert.Equal(t, job, result.Jobs[0])
			assert.Equal(t, job2, result.Jobs[1])
			assert.Equal(t, job3, result.Jobs[2])
			assert.Equal(t, job4, result.Jobs[3])
		})

		return nil
	})
	require.NoError(t, err)
}

func TestGetJobsByState(t *testing.T) {
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository, testClock *clock.FakeClock) error {
		queued := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, baseTime, basicJobOpts).
			Build().
			Job()

		runId1 := uuid.NewString()
		pending := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, baseTime, basicJobOpts).
			Lease(runId1, cluster, node, pool, baseTime).
			Pending(runId1, cluster, baseTime).
			Build().
			Job()

		runId2 := uuid.NewString()
		running := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, baseTime, basicJobOpts).
			Lease(runId2, cluster, node, pool, baseTime).
			Pending(runId2, cluster, baseTime).
			Running(runId2, node, baseTime).
			Build().
			Job()

		runId3 := uuid.NewString()
		_ = NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, baseTime, basicJobOpts).
			Lease(runId3, cluster, node, pool, baseTime).
			Pending(runId3, cluster, baseTime).
			Running(runId3, node, baseTime).
			Succeeded(baseTime).
			Build().
			Job()

		t.Run("exact", func(t *testing.T) {
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{{
					Field: "state",
					Match: model.MatchExact,
					Value: string(lookout.JobRunning),
				}},
				false,
				&model.Order{},
				0,
				10,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, 1)
			assert.Equal(t, running, result.Jobs[0])
		})

		t.Run("anyOf", func(t *testing.T) {
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{{
					Field: "state",
					Match: model.MatchAnyOf,
					Value: []string{
						string(lookout.JobQueued),
						string(lookout.JobPending),
						string(lookout.JobRunning),
					},
				}},
				false,
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, 3)
			assert.Equal(t, queued, result.Jobs[0])
			assert.Equal(t, pending, result.Jobs[1])
			assert.Equal(t, running, result.Jobs[2])
		})

		return nil
	})
	require.NoError(t, err)
}

func TestGetJobsByAnnotation(t *testing.T) {
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository, testClock *clock.FakeClock) error {
		job1 := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				Annotations: map[string]string{
					"annotation-key-1": "annotation-value-1",
					"annotation-key-2": "annotation-value-3",
				},
			}).
			Build().
			Job()

		job2 := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				Annotations: map[string]string{
					"annotation-key-1": "annotation-value-2",
				},
			}).
			Build().
			Job()

		job3 := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				Annotations: map[string]string{
					"annotation-key-1": "annotation-value-3",
				},
			}).
			Build().
			Job()

		_ = NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				Annotations: map[string]string{
					"annotation-key-2": "annotation-value-1",
				},
			}).
			Build().
			Job()

		job5 := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				Annotations: map[string]string{
					"annotation-key-1": "annotation-value-6",
					"annotation-key-2": "annotation-value-4",
				},
			}).
			Build().
			Job()

		t.Run("exact", func(t *testing.T) {
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{{
					Field:        "annotation-key-1",
					Match:        model.MatchExact,
					Value:        "annotation-value-1",
					IsAnnotation: true,
				}},
				false,
				&model.Order{},
				0,
				10,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, 1)
			assert.Equal(t, job1, result.Jobs[0])
		})

		t.Run("exact, multiple annotations", func(t *testing.T) {
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{
					{
						Field:        "annotation-key-1",
						Match:        model.MatchExact,
						Value:        "annotation-value-1",
						IsAnnotation: true,
					},
					{
						Field:        "annotation-key-2",
						Match:        model.MatchExact,
						Value:        "annotation-value-3",
						IsAnnotation: true,
					},
				},
				false,
				&model.Order{},
				0,
				10,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, 1)
			assert.Equal(t, job1, result.Jobs[0])
		})

		t.Run("startsWith, multiple annotations", func(t *testing.T) {
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{
					{
						Field:        "annotation-key-1",
						Match:        model.MatchStartsWith,
						Value:        "annotation-value-",
						IsAnnotation: true,
					},
					{
						Field:        "annotation-key-2",
						Match:        model.MatchStartsWith,
						Value:        "annotation-value-",
						IsAnnotation: true,
					},
				},
				false,
				&model.Order{},
				0,
				10,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, 2)
			assert.Equal(t, job1, result.Jobs[0])
			assert.Equal(t, job5, result.Jobs[1])
		})

		t.Run("contains, multiple annotations", func(t *testing.T) {
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{
					{
						Field:        "annotation-key-1",
						Match:        model.MatchContains,
						Value:        "value",
						IsAnnotation: true,
					},
					{
						Field:        "annotation-key-2",
						Match:        model.MatchContains,
						Value:        "value",
						IsAnnotation: true,
					},
				},
				false,
				&model.Order{},
				0,
				10,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, 2)
			assert.Equal(t, job1, result.Jobs[0])
			assert.Equal(t, job5, result.Jobs[1])
		})

		t.Run("exists", func(t *testing.T) {
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{
					{
						Field:        "annotation-key-1",
						Match:        model.MatchExists,
						IsAnnotation: true,
					},
				},
				false,
				&model.Order{},
				0,
				10,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, 4)
			assert.Equal(t, job1, result.Jobs[0])
			assert.Equal(t, job2, result.Jobs[1])
			assert.Equal(t, job3, result.Jobs[2])
			assert.Equal(t, job5, result.Jobs[3])
		})

		return nil
	})
	require.NoError(t, err)
}

func TestGetJobsByCpu(t *testing.T) {
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository, testClock *clock.FakeClock) error {
		job1 := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				Cpu: resource.MustParse("1"),
			}).
			Build().
			Job()

		job2 := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				Cpu: resource.MustParse("3"),
			}).
			Build().
			Job()

		job3 := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				Cpu: resource.MustParse("5"),
			}).
			Build().
			Job()

		job4 := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				Cpu: resource.MustParse("10"),
			}).
			Build().
			Job()

		t.Run("exact", func(t *testing.T) {
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{{
					Field: "cpu",
					Match: model.MatchExact,
					Value: 3000,
				}},
				false,
				&model.Order{},
				0,
				10,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, 1)
			assert.Equal(t, job2, result.Jobs[0])
		})

		t.Run("greaterThan", func(t *testing.T) {
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{{
					Field: "cpu",
					Match: model.MatchGreaterThan,
					Value: 3000,
				}},
				false,
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, 2)
			assert.Equal(t, job3, result.Jobs[0])
			assert.Equal(t, job4, result.Jobs[1])
		})

		t.Run("lessThan", func(t *testing.T) {
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{{
					Field: "cpu",
					Match: model.MatchLessThan,
					Value: 5000,
				}},
				false,
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, 2)
			assert.Equal(t, job1, result.Jobs[0])
			assert.Equal(t, job2, result.Jobs[1])
		})

		t.Run("greaterThanOrEqualTo", func(t *testing.T) {
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{{
					Field: "cpu",
					Match: model.MatchGreaterThanOrEqualTo,
					Value: 3000,
				}},
				false,
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, 3)
			assert.Equal(t, job2, result.Jobs[0])
			assert.Equal(t, job3, result.Jobs[1])
			assert.Equal(t, job4, result.Jobs[2])
		})

		t.Run("lessThanOrEqualTo", func(t *testing.T) {
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{{
					Field: "cpu",
					Match: model.MatchLessThanOrEqualTo,
					Value: 5000,
				}},
				false,
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, 3)
			assert.Equal(t, job1, result.Jobs[0])
			assert.Equal(t, job2, result.Jobs[1])
			assert.Equal(t, job3, result.Jobs[2])
		})

		return nil
	})
	require.NoError(t, err)
}

func TestGetJobsByMemory(t *testing.T) {
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository, testClock *clock.FakeClock) error {
		job1 := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				Memory: resource.MustParse("1000"),
			}).
			Build().
			Job()

		job2 := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				Memory: resource.MustParse("3000"),
			}).
			Build().
			Job()

		job3 := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				Memory: resource.MustParse("5000"),
			}).
			Build().
			Job()

		job4 := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				Memory: resource.MustParse("10000"),
			}).
			Build().
			Job()

		t.Run("exact", func(t *testing.T) {
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{{
					Field: "memory",
					Match: model.MatchExact,
					Value: 3000,
				}},
				false,
				&model.Order{},
				0,
				10,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, 1)
			assert.Equal(t, job2, result.Jobs[0])
		})

		t.Run("greaterThan", func(t *testing.T) {
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{{
					Field: "memory",
					Match: model.MatchGreaterThan,
					Value: 3000,
				}},
				false,
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, 2)
			assert.Equal(t, job3, result.Jobs[0])
			assert.Equal(t, job4, result.Jobs[1])
		})

		t.Run("lessThan", func(t *testing.T) {
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{{
					Field: "memory",
					Match: model.MatchLessThan,
					Value: 5000,
				}},
				false,
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, 2)
			assert.Equal(t, job1, result.Jobs[0])
			assert.Equal(t, job2, result.Jobs[1])
		})

		t.Run("greaterThanOrEqualTo", func(t *testing.T) {
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{{
					Field: "memory",
					Match: model.MatchGreaterThanOrEqualTo,
					Value: 3000,
				}},
				false,
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, 3)
			assert.Equal(t, job2, result.Jobs[0])
			assert.Equal(t, job3, result.Jobs[1])
			assert.Equal(t, job4, result.Jobs[2])
		})

		t.Run("lessThanOrEqualTo", func(t *testing.T) {
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{{
					Field: "memory",
					Match: model.MatchLessThanOrEqualTo,
					Value: 5000,
				}},
				false,
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, 3)
			assert.Equal(t, job1, result.Jobs[0])
			assert.Equal(t, job2, result.Jobs[1])
			assert.Equal(t, job3, result.Jobs[2])
		})

		return nil
	})
	require.NoError(t, err)
}

func TestGetJobsByEphemeralStorage(t *testing.T) {
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository, testClock *clock.FakeClock) error {
		job1 := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				EphemeralStorage: resource.MustParse("1000"),
			}).
			Build().
			Job()

		job2 := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				EphemeralStorage: resource.MustParse("3000"),
			}).
			Build().
			Job()

		job3 := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				EphemeralStorage: resource.MustParse("5000"),
			}).
			Build().
			Job()

		job4 := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				EphemeralStorage: resource.MustParse("10000"),
			}).
			Build().
			Job()

		t.Run("exact", func(t *testing.T) {
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{{
					Field: "ephemeralStorage",
					Match: model.MatchExact,
					Value: 3000,
				}},
				false,
				&model.Order{},
				0,
				10,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, 1)
			assert.Equal(t, job2, result.Jobs[0])
		})

		t.Run("greaterThan", func(t *testing.T) {
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{{
					Field: "ephemeralStorage",
					Match: model.MatchGreaterThan,
					Value: 3000,
				}},
				false,
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, 2)
			assert.Equal(t, job3, result.Jobs[0])
			assert.Equal(t, job4, result.Jobs[1])
		})

		t.Run("lessThan", func(t *testing.T) {
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{{
					Field: "ephemeralStorage",
					Match: model.MatchLessThan,
					Value: 5000,
				}},
				false,
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, 2)
			assert.Equal(t, job1, result.Jobs[0])
			assert.Equal(t, job2, result.Jobs[1])
		})

		t.Run("greaterThanOrEqualTo", func(t *testing.T) {
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{{
					Field: "ephemeralStorage",
					Match: model.MatchGreaterThanOrEqualTo,
					Value: 3000,
				}},
				false,
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, 3)
			assert.Equal(t, job2, result.Jobs[0])
			assert.Equal(t, job3, result.Jobs[1])
			assert.Equal(t, job4, result.Jobs[2])
		})

		t.Run("lessThanOrEqualTo", func(t *testing.T) {
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{{
					Field: "ephemeralStorage",
					Match: model.MatchLessThanOrEqualTo,
					Value: 5000,
				}},
				false,
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, 3)
			assert.Equal(t, job1, result.Jobs[0])
			assert.Equal(t, job2, result.Jobs[1])
			assert.Equal(t, job3, result.Jobs[2])
		})

		return nil
	})
	require.NoError(t, err)
}

func TestGetJobsByGpu(t *testing.T) {
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository, testClock *clock.FakeClock) error {
		job1 := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				Gpu: resource.MustParse("1"),
			}).
			Build().
			Job()

		job2 := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				Gpu: resource.MustParse("3"),
			}).
			Build().
			Job()

		job3 := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				Gpu: resource.MustParse("5"),
			}).
			Build().
			Job()

		job4 := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				Gpu: resource.MustParse("8"),
			}).
			Build().
			Job()

		t.Run("exact", func(t *testing.T) {
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{{
					Field: "gpu",
					Match: model.MatchExact,
					Value: 3,
				}},
				false,
				&model.Order{},
				0,
				10,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, 1)
			assert.Equal(t, job2, result.Jobs[0])
		})

		t.Run("greaterThan", func(t *testing.T) {
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{{
					Field: "gpu",
					Match: model.MatchGreaterThan,
					Value: 3,
				}},
				false,
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, 2)
			assert.Equal(t, job3, result.Jobs[0])
			assert.Equal(t, job4, result.Jobs[1])
		})

		t.Run("lessThan", func(t *testing.T) {
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{{
					Field: "gpu",
					Match: model.MatchLessThan,
					Value: 5,
				}},
				false,
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, 2)
			assert.Equal(t, job1, result.Jobs[0])
			assert.Equal(t, job2, result.Jobs[1])
		})

		t.Run("greaterThanOrEqualTo", func(t *testing.T) {
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{{
					Field: "gpu",
					Match: model.MatchGreaterThanOrEqualTo,
					Value: 3,
				}},
				false,
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, 3)
			assert.Equal(t, job2, result.Jobs[0])
			assert.Equal(t, job3, result.Jobs[1])
			assert.Equal(t, job4, result.Jobs[2])
		})

		t.Run("lessThanOrEqualTo", func(t *testing.T) {
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{{
					Field: "gpu",
					Match: model.MatchLessThanOrEqualTo,
					Value: 5,
				}},
				false,
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, 3)
			assert.Equal(t, job1, result.Jobs[0])
			assert.Equal(t, job2, result.Jobs[1])
			assert.Equal(t, job3, result.Jobs[2])
		})

		return nil
	})
	require.NoError(t, err)
}

func TestGetJobsByPriority(t *testing.T) {
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository, testClock *clock.FakeClock) error {
		job1 := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				Priority: 10,
			}).
			Build().
			Job()

		job2 := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				Priority: 20,
			}).
			Build().
			Job()

		job3 := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				Priority: 30,
			}).
			Build().
			Job()

		job4 := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				Priority: 40,
			}).
			Build().
			Job()

		t.Run("exact", func(t *testing.T) {
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{{
					Field: "priority",
					Match: model.MatchExact,
					Value: 20,
				}},
				false,
				&model.Order{},
				0,
				10,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, 1)
			assert.Equal(t, job2, result.Jobs[0])
		})

		t.Run("greaterThan", func(t *testing.T) {
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{{
					Field: "priority",
					Match: model.MatchGreaterThan,
					Value: 20,
				}},
				false,
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, 2)
			assert.Equal(t, job3, result.Jobs[0])
			assert.Equal(t, job4, result.Jobs[1])
		})

		t.Run("lessThan", func(t *testing.T) {
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{{
					Field: "priority",
					Match: model.MatchLessThan,
					Value: 30,
				}},
				false,
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, 2)
			assert.Equal(t, job1, result.Jobs[0])
			assert.Equal(t, job2, result.Jobs[1])
		})

		t.Run("greaterThanOrEqualTo", func(t *testing.T) {
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{{
					Field: "priority",
					Match: model.MatchGreaterThanOrEqualTo,
					Value: 20,
				}},
				false,
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, 3)
			assert.Equal(t, job2, result.Jobs[0])
			assert.Equal(t, job3, result.Jobs[1])
			assert.Equal(t, job4, result.Jobs[2])
		})

		t.Run("lessThanOrEqualTo", func(t *testing.T) {
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{{
					Field: "priority",
					Match: model.MatchLessThanOrEqualTo,
					Value: 30,
				}},
				false,
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, 3)
			assert.Equal(t, job1, result.Jobs[0])
			assert.Equal(t, job2, result.Jobs[1])
			assert.Equal(t, job3, result.Jobs[2])
		})

		return nil
	})
	require.NoError(t, err)
}

func TestGetJobsByPriorityClass(t *testing.T) {
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository, testClock *clock.FakeClock) error {
		job := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				PriorityClass: "priority-class-1",
			}).
			Build().
			Job()

		job2 := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				PriorityClass: "priority-class-2",
			}).
			Build().
			Job()

		job3 := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				PriorityClass: "priority-class-3",
			}).
			Build().
			Job()

		job4 := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				PriorityClass: "other-priority-class",
			}).
			Build().
			Job()

		_ = NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				PriorityClass: "something-else",
			}).
			Build().
			Job()

		t.Run("exact", func(t *testing.T) {
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{{
					Field: "priorityClass",
					Match: model.MatchExact,
					Value: "priority-class-1",
				}},
				false,
				&model.Order{},
				0,
				10,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, 1)
			assert.Equal(t, job, result.Jobs[0])
		})

		t.Run("startsWith", func(t *testing.T) {
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{{
					Field: "priorityClass",
					Match: model.MatchStartsWith,
					Value: "priority-class-",
				}},
				false,
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, 3)
			assert.Equal(t, job, result.Jobs[0])
			assert.Equal(t, job2, result.Jobs[1])
			assert.Equal(t, job3, result.Jobs[2])
		})

		t.Run("contains", func(t *testing.T) {
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{{
					Field: "priorityClass",
					Match: model.MatchContains,
					Value: "priority-class",
				}},
				false,
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, 4)
			assert.Equal(t, job, result.Jobs[0])
			assert.Equal(t, job2, result.Jobs[1])
			assert.Equal(t, job3, result.Jobs[2])
			assert.Equal(t, job4, result.Jobs[3])
		})

		return nil
	})
	require.NoError(t, err)
}

func TestGetJobsBySubmittedTime(t *testing.T) {
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository, testClock *clock.FakeClock) error {
		timeBase := time.Date(2022, 3, 1, 15, 4, 5, 0, time.UTC)

		job1 := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, timeBase, basicJobOpts).
			Build().
			Job()

		job2 := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, timeBase.Add(24*time.Hour), basicJobOpts).
			Build().
			Job()

		job3 := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, timeBase.Add(48*time.Hour), basicJobOpts).
			Build().
			Job()

		job4 := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, timeBase.Add(72*time.Hour), basicJobOpts).
			Build().
			Job()

		t.Run("greaterThan", func(t *testing.T) {
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{{
					Field: "submitted",
					Match: model.MatchGreaterThan,
					Value: timeBase.Add(24 * time.Hour),
				}},
				false,
				&model.Order{
					Field:     "submitted",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, 2)
			assert.Equal(t, job3, result.Jobs[0])
			assert.Equal(t, job4, result.Jobs[1])
		})

		t.Run("lessThan", func(t *testing.T) {
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{{
					Field: "submitted",
					Match: model.MatchLessThan,
					Value: timeBase.Add(48 * time.Hour),
				}},
				false,
				&model.Order{
					Field:     "submitted",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, 2)
			assert.Equal(t, job1, result.Jobs[0])
			assert.Equal(t, job2, result.Jobs[1])
		})

		t.Run("greaterThanOrEqualTo", func(t *testing.T) {
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{{
					Field: "submitted",
					Match: model.MatchGreaterThanOrEqualTo,
					Value: timeBase.Add(24 * time.Hour),
				}},
				false,
				&model.Order{
					Field:     "submitted",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, 3)
			assert.Equal(t, job2, result.Jobs[0])
			assert.Equal(t, job3, result.Jobs[1])
			assert.Equal(t, job4, result.Jobs[2])
		})

		t.Run("lessThanOrEqualTo", func(t *testing.T) {
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{{
					Field: "submitted",
					Match: model.MatchLessThanOrEqualTo,
					Value: timeBase.Add(48 * time.Hour),
				}},
				false,
				&model.Order{
					Field:     "submitted",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, 3)
			assert.Equal(t, job1, result.Jobs[0])
			assert.Equal(t, job2, result.Jobs[1])
			assert.Equal(t, job3, result.Jobs[2])
		})

		t.Run("between two dates", func(t *testing.T) {
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{
					{
						Field: "submitted",
						Match: model.MatchGreaterThanOrEqualTo,
						Value: timeBase.Add(24 * time.Hour),
					},
					{
						Field: "submitted",
						Match: model.MatchLessThanOrEqualTo,
						Value: timeBase.Add(48 * time.Hour),
					},
				},
				false,
				&model.Order{
					Field:     "submitted",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, 2)
			assert.Equal(t, job2, result.Jobs[0])
			assert.Equal(t, job3, result.Jobs[1])
		})

		return nil
	})
	require.NoError(t, err)
}

func TestGetJobsSkip(t *testing.T) {
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository, testClock *clock.FakeClock) error {
		nJobs := 15
		jobs := make([]*model.Job, nJobs)
		for i := 0; i < nJobs; i++ {
			jobId := util.NewULID()
			jobs[i] = NewJobSimulatorWithClock(converter, store, testClock).
				Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{JobId: jobId}).
				Build().
				Job()
		}

		t.Run("skip 3", func(t *testing.T) {
			skip := 3
			take := 5
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{},
				false,
				&model.Order{
					Field:     "jobId",
					Direction: "ASC",
				},
				skip,
				take,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, take)
			assert.Equal(t, jobs[skip:skip+take], result.Jobs)
		})

		t.Run("skip 7", func(t *testing.T) {
			skip := 7
			take := 5
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{},
				false,
				&model.Order{
					Field:     "jobId",
					Direction: "ASC",
				},
				skip,
				take,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, take)
			assert.Equal(t, jobs[skip:skip+take], result.Jobs)
		})

		t.Run("skip 13", func(t *testing.T) {
			skip := 13
			take := 5
			result, err := repo.GetJobs(
				armadacontext.TODO(),
				[]*model.Filter{},
				false,
				&model.Order{
					Field:     "jobId",
					Direction: "ASC",
				},
				skip,
				take,
			)
			require.NoError(t, err)
			require.Len(t, result.Jobs, 2)
			assert.Equal(t, jobs[skip:], result.Jobs)
		})

		return nil
	})
	require.NoError(t, err)
}

func TestGetJobsComplex(t *testing.T) {
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository, testClock *clock.FakeClock) error {
		nJobs := 15
		jobs := make([]*model.Job, nJobs)
		for i := 0; i < nJobs; i++ {
			jobId := util.NewULID()
			jobs[i] = NewJobSimulatorWithClock(converter, store, testClock).
				Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
					JobId: jobId,
					Annotations: map[string]string{
						"a": "value-1",
						"b": "value-2",
					},
				}).
				Build().
				Job()
		}

		for i := 0; i < nJobs; i++ {
			NewJobSimulatorWithClock(converter, store, testClock).
				Submit("other-queue", jobSet, owner, namespace, baseTime, &JobOptions{
					JobId: util.NewULID(),
					Annotations: map[string]string{
						"a": "value-1",
						"b": "value-2",
					},
				}).
				Build().
				Job()
		}

		skip := 8
		take := 5
		result, err := repo.GetJobs(
			armadacontext.TODO(),
			[]*model.Filter{
				{
					Field: "queue",
					Match: "exact",
					Value: queue,
				},
				{
					Field:        "a",
					Match:        "exact",
					Value:        "value-1",
					IsAnnotation: true,
				},
				{
					Field:        "b",
					Match:        "exact",
					Value:        "value-2",
					IsAnnotation: true,
				},
			},
			false,
			&model.Order{
				Field:     "jobId",
				Direction: "ASC",
			},
			skip,
			take,
		)
		require.NoError(t, err)
		require.Len(t, result.Jobs, take)
		assert.Equal(t, jobs[skip:skip+take], result.Jobs)

		return nil
	})
	require.NoError(t, err)
}

func TestGetJobsActiveJobSet(t *testing.T) {
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository, testClock *clock.FakeClock) error {
		activeJobSet1 := NewJobSimulatorWithClock(converter, store, testClock).
			Submit("queue-1", "job-set-1", owner, namespace, baseTime, &JobOptions{}).
			Build().
			Job()

		inactiveJobSet1 := NewJobSimulatorWithClock(converter, store, testClock).
			Submit("queue-1", "job-set-1", owner, namespace, baseTime, &JobOptions{}).
			Cancelled(baseTime.Add(1*time.Minute), cancelUser).
			Build().
			Job()

		NewJobSimulatorWithClock(converter, store, testClock).
			Submit("queue-2", "job-set-2", owner, namespace, baseTime, &JobOptions{}).
			Cancelled(baseTime.Add(1*time.Minute), cancelUser).
			Build().
			Job()

		result, err := repo.GetJobs(
			armadacontext.TODO(),
			[]*model.Filter{},
			true,
			&model.Order{
				Field:     "jobId",
				Direction: "ASC",
			},
			0,
			10,
		)
		require.NoError(t, err)
		require.Len(t, result.Jobs, 2)
		assert.Equal(t, []*model.Job{
			activeJobSet1,
			inactiveJobSet1,
		}, result.Jobs)

		return nil
	})
	require.NoError(t, err)
}

func TestGetJobsWithLatestRunDetails(t *testing.T) {
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository, testClock *clock.FakeClock) error {
		runIdLatest := uuid.NewString()
		// Simulate job submission and multiple runs, with the latest run being successful
		firstRunId := uuid.NewString()
		NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, baseTime, basicJobOpts).
			Lease(firstRunId, "first-cluster", "first-node", "first-pool", baseTime).
			Pending(firstRunId, "first-cluster", baseTime).
			Running(firstRunId, "first-node", baseTime.Add(time.Minute)).
			Lease(runIdLatest, "latest-cluster", "latest-node", "latest-pool", baseTime.Add(2*time.Minute)).
			Pending(runIdLatest, "latest-cluster", baseTime.Add(2*time.Minute)).
			Running(runIdLatest, "latest-node", baseTime.Add(3*time.Minute)).
			RunSucceeded(runIdLatest, baseTime.Add(4*time.Minute)).
			Build().
			Job()

		result, err := repo.GetJobs(armadacontext.TODO(), []*model.Filter{}, false, &model.Order{}, 0, 10)
		require.NoError(t, err)
		require.Len(t, result.Jobs, 1)

		// Adjusting assertions to dereference pointer fields
		if assert.NotNil(t, result.Jobs[0].Node) {
			assert.Equal(t, "latest-node", *result.Jobs[0].Node)
		}
		if assert.NotNil(t, result.Jobs[0].ExitCode) {
			assert.Equal(t, int32(0), *result.Jobs[0].ExitCode)
		}
		if assert.NotNil(t, result.Jobs[0].Cluster) {
			assert.Equal(t, "latest-cluster", result.Jobs[0].Cluster)
		}

		return nil
	})
	require.NoError(t, err)
}

func TestGetJobsWithSpecificRunDetails(t *testing.T) {
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository, testClock *clock.FakeClock) error {
		runIdSpecific := uuid.NewString()
		// Simulate job submission and a specific failed run
		NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, baseTime, basicJobOpts).
			Lease(runIdSpecific, "specific-cluster", "specific-node", "specific-pool", baseTime).
			Pending(runIdSpecific, "specific-cluster", baseTime).
			Running(runIdSpecific, "specific-node", baseTime.Add(time.Minute)).
			RunFailed(runIdSpecific, "specific-node", 2, "Specific failure message", "", baseTime.Add(2*time.Minute)).
			Build().
			Job()

		result, err := repo.GetJobs(armadacontext.TODO(), []*model.Filter{}, false, &model.Order{}, 0, 10)
		require.NoError(t, err)
		require.Len(t, result.Jobs, 1)

		// Adjusting assertions to dereference pointer fields
		if assert.NotNil(t, result.Jobs[0].Node) {
			assert.Equal(t, "specific-node", *result.Jobs[0].Node)
		}
		if assert.NotNil(t, result.Jobs[0].ExitCode) {
			assert.Equal(t, int32(2), *result.Jobs[0].ExitCode)
		}
		if assert.NotNil(t, result.Jobs[0].Cluster) {
			assert.Equal(t, "specific-cluster", result.Jobs[0].Cluster)
		}

		return nil
	})
	require.NoError(t, err)
}

func TestJobRuntimeWhenNoStartOrEnd(t *testing.T) {
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository, testClock *clock.FakeClock) error {
		runId := uuid.NewString()

		NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, time.Now(), basicJobOpts).
			Lease(runId, "cluster", "node", "pool", time.Now()).
			Build().
			Job()

		result, err := repo.GetJobs(armadacontext.TODO(), []*model.Filter{}, false, &model.Order{}, 0, 10)
		require.NoError(t, err)
		require.Len(t, result.Jobs, 1)

		actualRuntime := result.Jobs[0].RuntimeSeconds
		expectedRuntime := int32(0) // Runtime should be 0 when job is just leased
		assert.Equal(t, expectedRuntime, actualRuntime)

		return nil
	})
	require.NoError(t, err)
}

func TestJobRuntimeWhenStartedButNotFinishedWithClock(t *testing.T) {
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository, testClock *clock.FakeClock) error {
		runId := uuid.NewString()
		startTime := testClock.Now().UTC()
		runningTime := startTime.Add(time.Minute)

		NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, startTime, basicJobOpts).
			Lease(runId, "cluster", "node", "pool", startTime).
			Pending(runId, "cluster", startTime).
			Running(runId, "node", runningTime).
			Build().
			Job()

		// Increment time by 5 mins
		testClock.SetTime(testClock.Now().Add(time.Minute * 5))

		result, err := repo.GetJobs(armadacontext.TODO(), []*model.Filter{}, false, &model.Order{}, 0, 10)
		require.NoError(t, err)
		require.Len(t, result.Jobs, 1)

		actualRuntime := result.Jobs[0].RuntimeSeconds
		expectedRuntime := int32(240) // We incremented time by 5 mins, but the run started 1 min after start time
		assert.Equal(t, expectedRuntime, actualRuntime)

		return nil
	})
	require.NoError(t, err)
}

func TestJobRuntimeWhenRunFinishedWithClock(t *testing.T) {
	clk := clock.NewFakeClock(time.Now())
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository, testClock *clock.FakeClock) error {
		runId := uuid.NewString()
		startTime := testClock.Now()
		endTime := startTime.Add(5 * time.Minute)
		runningTime := startTime.Add(time.Minute)

		NewJobSimulatorWithClock(converter, store, clk).
			Submit(queue, jobSet, owner, namespace, startTime, basicJobOpts).
			Lease(runId, "specific-cluster", "specific-node", "pool", startTime).
			Pending(runId, "cluster", startTime).
			Running(runId, "node", runningTime).
			RunFailed(runId, "node", 1, "failed", "debug", endTime).
			Build().
			Job()

		// Increment time by 10 mins
		testClock.SetTime(testClock.Now().Add(time.Minute * 10))

		result, err := repo.GetJobs(armadacontext.TODO(), []*model.Filter{}, false, &model.Order{}, 0, 10)
		require.NoError(t, err)
		require.Len(t, result.Jobs, 1)

		actualRuntime := result.Jobs[0].RuntimeSeconds
		expectedRuntime := int32(endTime.Sub(runningTime).Seconds())
		assert.Equal(t, expectedRuntime, actualRuntime)

		return nil
	})
	require.NoError(t, err)
}

func TestGetJobsByNodeOfLatestRun(t *testing.T) {
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository, testClock *clock.FakeClock) error {
		// Create job that had multiple runs on different nodes, with latest on node-3
		firstRunId := uuid.NewString()
		secondRunId := uuid.NewString()
		latestRunId := uuid.NewString()

		jobWithMultipleRuns := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, baseTime, basicJobOpts).
			Lease(firstRunId, "cluster-1", "node-1", "pool-1", baseTime).
			Pending(firstRunId, "cluster-1", baseTime).
			Running(firstRunId, "node-1", baseTime.Add(time.Minute)).
			RunFailed(firstRunId, "node-1", 1, "failed", "", baseTime.Add(2*time.Minute)).
			Lease(secondRunId, "cluster-2", "node-2", "pool-2", baseTime.Add(3*time.Minute)).
			Pending(secondRunId, "cluster-2", baseTime.Add(3*time.Minute)).
			Running(secondRunId, "node-2", baseTime.Add(4*time.Minute)).
			RunFailed(secondRunId, "node-2", 1, "failed", "", baseTime.Add(5*time.Minute)).
			Lease(latestRunId, "cluster-3", "node-3", "pool-3", baseTime.Add(6*time.Minute)).
			Pending(latestRunId, "cluster-3", baseTime.Add(6*time.Minute)).
			Running(latestRunId, "node-3", baseTime.Add(7*time.Minute)).
			RunSucceeded(latestRunId, baseTime.Add(8*time.Minute)).
			Succeeded(baseTime.Add(8 * time.Minute)).
			Build().
			Job()

		// Create job with a single run on a different node
		differentNodeRunId := uuid.NewString()
		differentNodeJob := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, "job-set-2", owner, namespace, baseTime, basicJobOpts).
			Lease(differentNodeRunId, "cluster-4", "node-4", "pool-4", baseTime).
			Pending(differentNodeRunId, "cluster-4", baseTime).
			Running(differentNodeRunId, "node-4", baseTime.Add(time.Minute)).
			RunSucceeded(differentNodeRunId, baseTime.Add(2*time.Minute)).
			Succeeded(baseTime.Add(2 * time.Minute)).
			Build().
			Job()

		// Create job with no runs
		NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, "job-set-3", owner, namespace, baseTime, basicJobOpts).
			Build().
			Job()

		// Test filtering by latest run's node
		result, err := repo.GetJobs(
			armadacontext.TODO(),
			[]*model.Filter{
				{
					Field: "node",
					Match: model.MatchExact,
					Value: "node-3",
				},
			},
			false,
			&model.Order{},
			0,
			10,
		)

		require.NoError(t, err)
		require.Len(t, result.Jobs, 1)
		assert.Equal(t, jobWithMultipleRuns, result.Jobs[0])

		// Test filtering by a different node
		result, err = repo.GetJobs(
			armadacontext.TODO(),
			[]*model.Filter{
				{
					Field: "node",
					Match: model.MatchExact,
					Value: "node-4",
				},
			},
			false,
			&model.Order{},
			0,
			10,
		)

		require.NoError(t, err)
		require.Len(t, result.Jobs, 1)
		assert.Equal(t, differentNodeJob, result.Jobs[0])

		// Test filtering by a non-existent node
		result, err = repo.GetJobs(
			armadacontext.TODO(),
			[]*model.Filter{
				{
					Field: "node",
					Match: model.MatchExact,
					Value: "node-does-not-exist",
				},
			},
			false,
			&model.Order{},
			0,
			10,
		)

		require.NoError(t, err)
		require.Len(t, result.Jobs, 0)

		return nil
	})
	require.NoError(t, err)
}

func TestGetJobsByClusterOfLatestRun(t *testing.T) {
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository, testClock *clock.FakeClock) error {
		// Create job that had multiple runs on different clusters, with latest on cluster-3
		firstRunId := uuid.NewString()
		secondRunId := uuid.NewString()
		latestRunId := uuid.NewString()

		jobWithMultipleRuns := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, baseTime, basicJobOpts).
			Lease(firstRunId, "cluster-1", "node-1", "pool-1", baseTime).
			Pending(firstRunId, "cluster-1", baseTime).
			Running(firstRunId, "node-1", baseTime.Add(time.Minute)).
			RunFailed(firstRunId, "node-1", 1, "failed", "", baseTime.Add(2*time.Minute)).
			Lease(secondRunId, "cluster-2", "node-2", "pool-2", baseTime.Add(3*time.Minute)).
			Pending(secondRunId, "cluster-2", baseTime.Add(3*time.Minute)).
			Running(secondRunId, "node-2", baseTime.Add(4*time.Minute)).
			RunFailed(secondRunId, "node-2", 1, "failed", "", baseTime.Add(5*time.Minute)).
			Lease(latestRunId, "cluster-3", "node-3", "pool-3", baseTime.Add(6*time.Minute)).
			Pending(latestRunId, "cluster-3", baseTime.Add(6*time.Minute)).
			Running(latestRunId, "node-3", baseTime.Add(7*time.Minute)).
			RunSucceeded(latestRunId, baseTime.Add(8*time.Minute)).
			Succeeded(baseTime.Add(8 * time.Minute)).
			Build().
			Job()

		// Create job with a single run on a different cluster
		differentClusterRunId := uuid.NewString()
		differentClusterJob := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, "job-set-2", owner, namespace, baseTime, basicJobOpts).
			Lease(differentClusterRunId, "cluster-4", "node-4", "pool-4", baseTime).
			Pending(differentClusterRunId, "cluster-4", baseTime).
			Running(differentClusterRunId, "node-4", baseTime.Add(time.Minute)).
			RunSucceeded(differentClusterRunId, baseTime.Add(2*time.Minute)).
			Succeeded(baseTime.Add(2 * time.Minute)).
			Build().
			Job()

		// Create job with no runs
		NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, "job-set-3", owner, namespace, baseTime, basicJobOpts).
			Build().
			Job()

		// Test filtering by latest run's cluster
		result, err := repo.GetJobs(
			armadacontext.TODO(),
			[]*model.Filter{
				{
					Field: "cluster",
					Match: model.MatchExact,
					Value: "cluster-3",
				},
			},
			false,
			&model.Order{},
			0,
			10,
		)

		require.NoError(t, err)
		require.Len(t, result.Jobs, 1)
		assert.Equal(t, jobWithMultipleRuns, result.Jobs[0])

		// Test filtering by a different cluster
		result, err = repo.GetJobs(
			armadacontext.TODO(),
			[]*model.Filter{
				{
					Field: "cluster",
					Match: model.MatchExact,
					Value: "cluster-4",
				},
			},
			false,
			&model.Order{},
			0,
			10,
		)

		require.NoError(t, err)
		require.Len(t, result.Jobs, 1)
		assert.Equal(t, differentClusterJob, result.Jobs[0])

		// Test filtering by a non-existent cluster
		result, err = repo.GetJobs(
			armadacontext.TODO(),
			[]*model.Filter{
				{
					Field: "cluster",
					Match: model.MatchExact,
					Value: "cluster-does-not-exist",
				},
			},
			false,
			&model.Order{},
			0,
			10,
		)

		require.NoError(t, err)
		require.Len(t, result.Jobs, 0)

		return nil
	})
	require.NoError(t, err)
}

func TestGetJobsByPoolOfLatestRun(t *testing.T) {
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository, testClock *clock.FakeClock) error {
		// Create job that had multiple runs on different pools, with latest on pool-3
		firstRunId := uuid.NewString()
		secondRunId := uuid.NewString()
		latestRunId := uuid.NewString()

		jobWithMultipleRuns := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, baseTime, basicJobOpts).
			Lease(firstRunId, "cluster-1", "node-1", "pool-1", baseTime).
			Pending(firstRunId, "cluster-1", baseTime).
			Running(firstRunId, "node-1", baseTime.Add(time.Minute)).
			RunFailed(firstRunId, "node-1", 1, "failed", "", baseTime.Add(2*time.Minute)).
			Lease(secondRunId, "cluster-2", "node-2", "pool-2", baseTime.Add(3*time.Minute)).
			Pending(secondRunId, "cluster-2", baseTime.Add(3*time.Minute)).
			Running(secondRunId, "node-2", baseTime.Add(4*time.Minute)).
			RunFailed(secondRunId, "node-2", 1, "failed", "", baseTime.Add(5*time.Minute)).
			Lease(latestRunId, "cluster-3", "node-3", "pool-3", baseTime.Add(6*time.Minute)).
			Pending(latestRunId, "cluster-3", baseTime.Add(6*time.Minute)).
			Running(latestRunId, "node-3", baseTime.Add(7*time.Minute)).
			RunSucceeded(latestRunId, baseTime.Add(8*time.Minute)).
			Succeeded(baseTime.Add(8 * time.Minute)).
			Build().
			Job()

		// Create job with a single run on a different pool
		differentPoolRunId := uuid.NewString()
		differentPoolJob := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, "job-set-2", owner, namespace, baseTime, basicJobOpts).
			Lease(differentPoolRunId, "cluster-4", "node-4", "pool-4", baseTime).
			Pending(differentPoolRunId, "cluster-4", baseTime).
			Running(differentPoolRunId, "node-4", baseTime.Add(time.Minute)).
			RunSucceeded(differentPoolRunId, baseTime.Add(2*time.Minute)).
			Succeeded(baseTime.Add(2 * time.Minute)).
			Build().
			Job()

		// Create another job on pool-3
		anotherPool3RunId := uuid.NewString()
		anotherPool3Job := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, "job-set-3", owner, namespace, baseTime, basicJobOpts).
			Lease(anotherPool3RunId, "cluster-5", "node-5", "pool-3", baseTime).
			Pending(anotherPool3RunId, "cluster-5", baseTime).
			Running(anotherPool3RunId, "node-5", baseTime.Add(time.Minute)).
			RunSucceeded(anotherPool3RunId, baseTime.Add(2*time.Minute)).
			Succeeded(baseTime.Add(2 * time.Minute)).
			Build().
			Job()

		// Create job with no runs
		NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, "job-set-4", owner, namespace, baseTime, basicJobOpts).
			Build().
			Job()

		// Test filtering by latest run's pool (MatchExact)
		result, err := repo.GetJobs(
			armadacontext.TODO(),
			[]*model.Filter{
				{
					Field: "pool",
					Match: model.MatchExact,
					Value: "pool-3",
				},
			},
			false,
			&model.Order{},
			0,
			10,
		)

		require.NoError(t, err)
		require.Len(t, result.Jobs, 2)
		jobIds := []string{result.Jobs[0].JobId, result.Jobs[1].JobId}
		assert.Contains(t, jobIds, jobWithMultipleRuns.JobId)
		assert.Contains(t, jobIds, anotherPool3Job.JobId)
		// Verify that Pool field is populated on Job struct
		for _, job := range result.Jobs {
			assert.NotNil(t, job.Pool)
			assert.Equal(t, "pool-3", *job.Pool)
		}

		// Test filtering by a different pool
		result, err = repo.GetJobs(
			armadacontext.TODO(),
			[]*model.Filter{
				{
					Field: "pool",
					Match: model.MatchExact,
					Value: "pool-4",
				},
			},
			false,
			&model.Order{},
			0,
			10,
		)

		require.NoError(t, err)
		require.Len(t, result.Jobs, 1)
		assert.Equal(t, differentPoolJob.JobId, result.Jobs[0].JobId)
		// Verify that Pool field is populated on Job struct
		assert.NotNil(t, result.Jobs[0].Pool)
		assert.Equal(t, "pool-4", *result.Jobs[0].Pool)

		// Test filtering by non-existent pool
		result, err = repo.GetJobs(
			armadacontext.TODO(),
			[]*model.Filter{
				{
					Field: "pool",
					Match: model.MatchExact,
					Value: "pool-does-not-exist",
				},
			},
			false,
			&model.Order{},
			0,
			10,
		)

		require.NoError(t, err)
		require.Len(t, result.Jobs, 0)

		// Test filtering by multiple pools using MatchAnyOf
		result, err = repo.GetJobs(
			armadacontext.TODO(),
			[]*model.Filter{
				{
					Field: "pool",
					Match: model.MatchAnyOf,
					Value: []interface{}{"pool-3", "pool-4"},
				},
			},
			false,
			&model.Order{},
			0,
			10,
		)

		require.NoError(t, err)
		require.Len(t, result.Jobs, 3)
		jobIds = []string{result.Jobs[0].JobId, result.Jobs[1].JobId, result.Jobs[2].JobId}
		assert.Contains(t, jobIds, jobWithMultipleRuns.JobId)
		assert.Contains(t, jobIds, differentPoolJob.JobId)
		assert.Contains(t, jobIds, anotherPool3Job.JobId)

		return nil
	})
	require.NoError(t, err)
}

func TestGetJobsIncludesIngressAddresses(t *testing.T) {
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository, testClock *clock.FakeClock) error {
		runId := uuid.NewString()
		ingressAddresses := map[int32]string{
			80: "ingress.example.com",
		}

		job := NewJobSimulatorWithClock(converter, store, testClock).
			Submit(queue, jobSet, owner, namespace, baseTime, basicJobOpts).
			Lease(runId, cluster, node, pool, baseTime).
			Pending(runId, cluster, baseTime).
			Running(runId, node, baseTime.Add(time.Minute)).
			IngressInfo(runId, ingressAddresses, baseTime.Add(2*time.Minute)).
			RunSucceeded(runId, baseTime.Add(3*time.Minute)).
			Succeeded(baseTime.Add(3 * time.Minute)).
			Build().
			Job()

		result, err := repo.GetJobs(armadacontext.TODO(), []*model.Filter{}, false, &model.Order{}, 0, 10)
		require.NoError(t, err)
		require.Len(t, result.Jobs, 1)
		assert.Equal(t, job, result.Jobs[0])
		require.Len(t, result.Jobs[0].Runs, 1)
		assert.Equal(t, ingressAddresses, result.Jobs[0].Runs[0].IngressAddresses)
		return nil
	})
	require.NoError(t, err)
}
