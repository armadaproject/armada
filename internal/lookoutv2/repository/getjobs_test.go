package repository

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/common/database/lookout"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/lookoutingesterv2/instructions"
	"github.com/armadaproject/armada/internal/lookoutingesterv2/lookoutdb"
	"github.com/armadaproject/armada/internal/lookoutingesterv2/metrics"
	"github.com/armadaproject/armada/internal/lookoutv2/model"
)

const (
	jobId     = "01f3j0g1md4qx7z5qb148qnh4d"
	queue     = "queue-1"
	jobSet    = "job-set-1"
	cluster   = "cluster-1"
	owner     = "user-1"
	namespace = "namespace-1"
	priority  = 12

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

func withGetJobsSetup(f func(*instructions.InstructionConverter, *lookoutdb.LookoutDb, *SqlGetJobsRepository) error) error {
	for _, useJsonbBackend := range []bool{false, true} {
		if err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
			converter := instructions.NewInstructionConverter(metrics.Get(), userAnnotationPrefix, &compress.NoOpCompressor{}, true)
			store := lookoutdb.NewLookoutDb(db, nil, metrics.Get(), 10)
			repo := NewSqlGetJobsRepository(db, useJsonbBackend)
			return f(converter, store, repo)
		}); err != nil {
			return err
		}
	}
	return nil
}

func TestGetJobsSingle(t *testing.T) {
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository) error {
		job := NewJobSimulator(converter, store).
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
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository) error {
		job := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, namespace, baseTime, basicJobOpts).
			Pending(uuid.NewString(), cluster, baseTime).
			Pending(uuid.NewString(), cluster, baseTime.Add(time.Second)).
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
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository) error {
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
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository) error {
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
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository) error {
		firstId := "01f3j0g1md4qx7z5qb148qnh4d"
		secondId := "01f3j0g1md4qx7z5qb148qnjjj"
		thirdId := "01f3j0g1md4qx7z5qb148qnmmm"

		third := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				JobId: thirdId,
			}).
			Build().
			Job()

		second := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				JobId: secondId,
			}).
			Build().
			Job()

		first := NewJobSimulator(converter, store).
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
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository) error {
		third := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, namespace, baseTime.Add(3*time.Second), basicJobOpts).
			Build().
			Job()

		second := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, namespace, baseTime.Add(2*time.Second), basicJobOpts).
			Build().
			Job()

		first := NewJobSimulator(converter, store).
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
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository) error {
		runId1 := uuid.NewString()
		third := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, namespace, baseTime, basicJobOpts).
			Pending(runId1, cluster, baseTime).
			Running(runId1, cluster, baseTime.Add(3*time.Minute)).
			Build().
			Job()

		second := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, namespace, baseTime, basicJobOpts).
			Pending(uuid.NewString(), cluster, baseTime.Add(2*time.Minute)).
			Build().
			Job()

		first := NewJobSimulator(converter, store).
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
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository) error {
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
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository) error {
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
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository) error {
		job := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{JobId: jobId}).
			Build().
			Job()

		_ = NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{JobId: "01f3j0g1md4qx7z5qb148qnaaa"}).
			Build().
			Job()

		_ = NewJobSimulator(converter, store).
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
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository) error {
		job := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, namespace, baseTime, basicJobOpts).
			Build().
			Job()

		job2 := NewJobSimulator(converter, store).
			Submit("queue-2", jobSet, owner, namespace, baseTime, basicJobOpts).
			Build().
			Job()

		job3 := NewJobSimulator(converter, store).
			Submit("queue-3", jobSet, owner, namespace, baseTime, basicJobOpts).
			Build().
			Job()

		job4 := NewJobSimulator(converter, store).
			Submit("other-queue", jobSet, owner, namespace, baseTime, basicJobOpts).
			Build().
			Job()

		_ = NewJobSimulator(converter, store).
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
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository) error {
		job := NewJobSimulator(converter, store).
			Submit(queue, "job\\set\\1", owner, namespace, baseTime, basicJobOpts).
			Build().
			Job()

		job2 := NewJobSimulator(converter, store).
			Submit(queue, "job\\set\\2", owner, namespace, baseTime, basicJobOpts).
			Build().
			Job()

		job3 := NewJobSimulator(converter, store).
			Submit(queue, "job\\set\\3", owner, namespace, baseTime, basicJobOpts).
			Build().
			Job()

		job4 := NewJobSimulator(converter, store).
			Submit(queue, "other-job\\set", owner, namespace, baseTime, basicJobOpts).
			Build().
			Job()

		_ = NewJobSimulator(converter, store).
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
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository) error {
		job := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, namespace, baseTime, basicJobOpts).
			Build().
			Job()

		job2 := NewJobSimulator(converter, store).
			Submit(queue, jobSet, "user-2", namespace, baseTime, basicJobOpts).
			Build().
			Job()

		job3 := NewJobSimulator(converter, store).
			Submit(queue, jobSet, "user-3", namespace, baseTime, basicJobOpts).
			Build().
			Job()

		job4 := NewJobSimulator(converter, store).
			Submit(queue, jobSet, "other-user", namespace, baseTime, basicJobOpts).
			Build().
			Job()

		_ = NewJobSimulator(converter, store).
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
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository) error {
		queued := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, namespace, baseTime, basicJobOpts).
			Build().
			Job()

		pending := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, namespace, baseTime, basicJobOpts).
			Pending(uuid.NewString(), cluster, baseTime).
			Build().
			Job()

		runId2 := uuid.NewString()
		running := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, namespace, baseTime, basicJobOpts).
			Pending(runId2, cluster, baseTime).
			Running(runId2, node, baseTime).
			Build().
			Job()

		runId3 := uuid.NewString()
		_ = NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, namespace, baseTime, basicJobOpts).
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
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository) error {
		job1 := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				Annotations: map[string]string{
					"annotation-key-1": "annotation-value-1",
					"annotation-key-2": "annotation-value-3",
				},
			}).
			Build().
			Job()

		job2 := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				Annotations: map[string]string{
					"annotation-key-1": "annotation-value-2",
				},
			}).
			Build().
			Job()

		job3 := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				Annotations: map[string]string{
					"annotation-key-1": "annotation-value-3",
				},
			}).
			Build().
			Job()

		_ = NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				Annotations: map[string]string{
					"annotation-key-2": "annotation-value-1",
				},
			}).
			Build().
			Job()

		job5 := NewJobSimulator(converter, store).
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
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository) error {
		job1 := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				Cpu: resource.MustParse("1"),
			}).
			Build().
			Job()

		job2 := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				Cpu: resource.MustParse("3"),
			}).
			Build().
			Job()

		job3 := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				Cpu: resource.MustParse("5"),
			}).
			Build().
			Job()

		job4 := NewJobSimulator(converter, store).
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
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository) error {
		job1 := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				Memory: resource.MustParse("1000"),
			}).
			Build().
			Job()

		job2 := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				Memory: resource.MustParse("3000"),
			}).
			Build().
			Job()

		job3 := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				Memory: resource.MustParse("5000"),
			}).
			Build().
			Job()

		job4 := NewJobSimulator(converter, store).
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
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository) error {
		job1 := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				EphemeralStorage: resource.MustParse("1000"),
			}).
			Build().
			Job()

		job2 := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				EphemeralStorage: resource.MustParse("3000"),
			}).
			Build().
			Job()

		job3 := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				EphemeralStorage: resource.MustParse("5000"),
			}).
			Build().
			Job()

		job4 := NewJobSimulator(converter, store).
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
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository) error {
		job1 := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				Gpu: resource.MustParse("1"),
			}).
			Build().
			Job()

		job2 := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				Gpu: resource.MustParse("3"),
			}).
			Build().
			Job()

		job3 := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				Gpu: resource.MustParse("5"),
			}).
			Build().
			Job()

		job4 := NewJobSimulator(converter, store).
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
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository) error {
		job1 := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				Priority: 10,
			}).
			Build().
			Job()

		job2 := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				Priority: 20,
			}).
			Build().
			Job()

		job3 := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				Priority: 30,
			}).
			Build().
			Job()

		job4 := NewJobSimulator(converter, store).
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
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository) error {
		job := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				PriorityClass: "priority-class-1",
			}).
			Build().
			Job()

		job2 := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				PriorityClass: "priority-class-2",
			}).
			Build().
			Job()

		job3 := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				PriorityClass: "priority-class-3",
			}).
			Build().
			Job()

		job4 := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				PriorityClass: "other-priority-class",
			}).
			Build().
			Job()

		_ = NewJobSimulator(converter, store).
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

func TestGetJobsSkip(t *testing.T) {
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository) error {
		nJobs := 15
		jobs := make([]*model.Job, nJobs)
		for i := 0; i < nJobs; i++ {
			jobId := util.NewULID()
			jobs[i] = NewJobSimulator(converter, store).
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
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository) error {
		nJobs := 15
		jobs := make([]*model.Job, nJobs)
		for i := 0; i < nJobs; i++ {
			jobId := util.NewULID()
			jobs[i] = NewJobSimulator(converter, store).
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
			NewJobSimulator(converter, store).
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
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository) error {
		activeJobSet1 := NewJobSimulator(converter, store).
			Submit("queue-1", "job-set-1", owner, namespace, baseTime, &JobOptions{}).
			Build().
			Job()

		inactiveJobSet1 := NewJobSimulator(converter, store).
			Submit("queue-1", "job-set-1", owner, namespace, baseTime, &JobOptions{}).
			Cancelled(baseTime.Add(1 * time.Minute)).
			Build().
			Job()

		NewJobSimulator(converter, store).
			Submit("queue-2", "job-set-2", owner, namespace, baseTime, &JobOptions{}).
			Cancelled(baseTime.Add(1 * time.Minute)).
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
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository) error {
		runIdLatest := uuid.NewString()
		// Simulate job submission and multiple runs, with the latest run being successful
		NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, namespace, baseTime, basicJobOpts).
			Pending(uuid.NewString(), "first-cluster", baseTime).
			Running(uuid.NewString(), "first-node", baseTime.Add(time.Minute)).
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
	err := withGetJobsSetup(func(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, repo *SqlGetJobsRepository) error {
		runIdSpecific := uuid.NewString()
		// Simulate job submission and a specific failed run
		NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, namespace, baseTime, basicJobOpts).
			Pending(runIdSpecific, "specific-cluster", baseTime).
			Running(runIdSpecific, "specific-node", baseTime.Add(time.Minute)).
			RunFailed(runIdSpecific, "specific-node", 2, "Specific failure message", baseTime.Add(2*time.Minute)).
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
