package repository

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/common/database/lookout"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/lookoutingesterv2/instructions"
	"github.com/armadaproject/armada/internal/lookoutingesterv2/lookoutdb"
	"github.com/armadaproject/armada/internal/lookoutingesterv2/metrics"
	"github.com/armadaproject/armada/internal/lookoutv2/model"
)

const (
	jobId    = "01f3j0g1md4qx7z5qb148qnh4d"
	queue    = "queue-1"
	jobSet   = "job-set-1"
	cluster  = "cluster-1"
	owner    = "user-1"
	priority = 12

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

func TestGetJobsSingle(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		converter := instructions.NewInstructionConverter(metrics.Get(), userAnnotationPrefix, &compress.NoOpCompressor{}, true)
		store := lookoutdb.NewLookoutDb(db, metrics.Get(), 3, 10)

		job := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, baseTime, &JobOptions{
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

		repo := NewSqlGetJobsRepository(db)
		result, err := repo.GetJobs(context.TODO(), []*model.Filter{}, &model.Order{}, 0, 1)
		assert.NoError(t, err)
		assert.Len(t, result.Jobs, 1)
		assert.Equal(t, 1, result.Count)
		assert.Equal(t, job, result.Jobs[0])
		return nil
	})
	assert.NoError(t, err)
}

func TestGetJobsMultipleRuns(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		converter := instructions.NewInstructionConverter(metrics.Get(), userAnnotationPrefix, &compress.NoOpCompressor{}, true)
		store := lookoutdb.NewLookoutDb(db, metrics.Get(), 3, 10)

		job := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, baseTime, basicJobOpts).
			Pending(uuid.NewString(), cluster, baseTime).
			Pending(uuid.NewString(), cluster, baseTime.Add(time.Second)).
			Pending(runId, cluster, baseTime.Add(2*time.Second)).
			Running(runId, node, baseTime.Add(2*time.Second)).
			RunSucceeded(runId, baseTime.Add(2*time.Second)).
			Succeeded(baseTime.Add(2 * time.Second)).
			Build().
			Job()

		// Runs should be sorted from oldest -> newest
		repo := NewSqlGetJobsRepository(db)
		result, err := repo.GetJobs(context.TODO(), []*model.Filter{}, &model.Order{}, 0, 1)
		assert.NoError(t, err)
		assert.Len(t, result.Jobs, 1)
		assert.Equal(t, 1, result.Count)
		assert.Equal(t, job, result.Jobs[0])
		return nil
	})
	assert.NoError(t, err)
}

func TestOrderByUnsupportedField(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		repo := NewSqlGetJobsRepository(db)
		_, err := repo.GetJobs(
			context.TODO(),
			[]*model.Filter{},
			&model.Order{
				Field:     "someField",
				Direction: "ASC",
			},
			0,
			10,
		)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "column for field someField not found")
		return nil
	})
	assert.NoError(t, err)
}

func TestOrderByUnsupportedDirection(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		repo := NewSqlGetJobsRepository(db)
		_, err := repo.GetJobs(
			context.TODO(),
			[]*model.Filter{},
			&model.Order{
				Field:     "jobId",
				Direction: "INTERLEAVED",
			},
			0,
			10,
		)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "direction INTERLEAVED is not a valid sort direction")
		return nil
	})
	assert.NoError(t, err)
}

// Since job ids are ULIDs, it is comparable to sorting by submission time
func TestGetJobsOrderByJobId(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		converter := instructions.NewInstructionConverter(metrics.Get(), userAnnotationPrefix, &compress.NoOpCompressor{}, true)
		store := lookoutdb.NewLookoutDb(db, metrics.Get(), 3, 10)

		firstId := "01f3j0g1md4qx7z5qb148qnh4d"
		secondId := "01f3j0g1md4qx7z5qb148qnjjj"
		thirdId := "01f3j0g1md4qx7z5qb148qnmmm"

		third := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, baseTime, &JobOptions{
				JobId: thirdId,
			}).
			Build().
			Job()

		second := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, baseTime, &JobOptions{
				JobId: secondId,
			}).
			Build().
			Job()

		first := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, baseTime, &JobOptions{
				JobId: firstId,
			}).
			Build().
			Job()

		repo := NewSqlGetJobsRepository(db)

		t.Run("ascending order", func(t *testing.T) {
			result, err := repo.GetJobs(
				context.TODO(),
				[]*model.Filter{},
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			assert.NoError(t, err)
			assert.Len(t, result.Jobs, 3)
			assert.Equal(t, 3, result.Count)
			assert.Equal(t, first, result.Jobs[0])
			assert.Equal(t, second, result.Jobs[1])
			assert.Equal(t, third, result.Jobs[2])
		})

		t.Run("descending order", func(t *testing.T) {
			result, err := repo.GetJobs(
				context.TODO(),
				[]*model.Filter{},
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionDesc,
				},
				0,
				10,
			)
			assert.NoError(t, err)
			assert.Len(t, result.Jobs, 3)
			assert.Equal(t, 3, result.Count)
			assert.Equal(t, third, result.Jobs[0])
			assert.Equal(t, second, result.Jobs[1])
			assert.Equal(t, first, result.Jobs[2])
		})

		return nil
	})
	assert.NoError(t, err)
}

func TestGetJobsOrderBySubmissionTime(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		converter := instructions.NewInstructionConverter(metrics.Get(), userAnnotationPrefix, &compress.NoOpCompressor{}, true)
		store := lookoutdb.NewLookoutDb(db, metrics.Get(), 3, 10)

		third := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, baseTime.Add(3*time.Second), basicJobOpts).
			Build().
			Job()

		second := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, baseTime.Add(2*time.Second), basicJobOpts).
			Build().
			Job()

		first := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, baseTime, basicJobOpts).
			Build().
			Job()

		repo := NewSqlGetJobsRepository(db)

		t.Run("ascending order", func(t *testing.T) {
			result, err := repo.GetJobs(
				context.TODO(),
				[]*model.Filter{},
				&model.Order{
					Field:     "submitted",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			assert.NoError(t, err)
			assert.Len(t, result.Jobs, 3)
			assert.Equal(t, 3, result.Count)
			assert.Equal(t, first, result.Jobs[0])
			assert.Equal(t, second, result.Jobs[1])
			assert.Equal(t, third, result.Jobs[2])
		})

		t.Run("descending order", func(t *testing.T) {
			result, err := repo.GetJobs(
				context.TODO(),
				[]*model.Filter{},
				&model.Order{
					Field:     "submitted",
					Direction: model.DirectionDesc,
				},
				0,
				10,
			)
			assert.NoError(t, err)
			assert.Len(t, result.Jobs, 3)
			assert.Equal(t, 3, result.Count)
			assert.Equal(t, third, result.Jobs[0])
			assert.Equal(t, second, result.Jobs[1])
			assert.Equal(t, first, result.Jobs[2])
		})

		return nil
	})
	assert.NoError(t, err)
}

func TestGetJobsOrderByLastTransitionTime(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		converter := instructions.NewInstructionConverter(metrics.Get(), userAnnotationPrefix, &compress.NoOpCompressor{}, true)
		store := lookoutdb.NewLookoutDb(db, metrics.Get(), 3, 10)

		runId1 := uuid.NewString()
		third := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, baseTime, basicJobOpts).
			Pending(runId1, cluster, baseTime).
			Running(runId1, cluster, baseTime.Add(3*time.Minute)).
			Build().
			Job()

		second := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, baseTime, basicJobOpts).
			Pending(uuid.NewString(), cluster, baseTime.Add(2*time.Minute)).
			Build().
			Job()

		first := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, baseTime, basicJobOpts).
			Build().
			Job()

		repo := NewSqlGetJobsRepository(db)

		t.Run("ascending order", func(t *testing.T) {
			result, err := repo.GetJobs(
				context.TODO(),
				[]*model.Filter{},
				&model.Order{
					Field:     "lastTransitionTime",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			assert.NoError(t, err)
			assert.Len(t, result.Jobs, 3)
			assert.Equal(t, 3, result.Count)
			assert.Equal(t, first, result.Jobs[0])
			assert.Equal(t, second, result.Jobs[1])
			assert.Equal(t, third, result.Jobs[2])
		})

		t.Run("descending order", func(t *testing.T) {
			result, err := repo.GetJobs(
				context.TODO(),
				[]*model.Filter{},
				&model.Order{
					Field:     "lastTransitionTime",
					Direction: model.DirectionDesc,
				},
				0,
				10,
			)
			assert.NoError(t, err)
			assert.Len(t, result.Jobs, 3)
			assert.Equal(t, 3, result.Count)
			assert.Equal(t, third, result.Jobs[0])
			assert.Equal(t, second, result.Jobs[1])
			assert.Equal(t, first, result.Jobs[2])
		})

		return nil
	})
	assert.NoError(t, err)
}

func TestFilterByUnsupportedField(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		repo := NewSqlGetJobsRepository(db)
		_, err := repo.GetJobs(
			context.TODO(),
			[]*model.Filter{{
				Field: "someField",
				Match: model.MatchExact,
				Value: "something",
			}},
			&model.Order{},
			0,
			10,
		)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "column for field someField not found")
		return nil
	})
	assert.NoError(t, err)
}

func TestFilterByUnsupportedMatch(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		repo := NewSqlGetJobsRepository(db)

		_, err := repo.GetJobs(
			context.TODO(),
			[]*model.Filter{{
				Field: "jobId",
				Match: model.MatchLessThan,
				Value: "something",
			}},
			&model.Order{},
			0,
			10,
		)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), fmt.Sprintf("%s is not supported for field jobId", model.MatchLessThan))

		return nil
	})
	assert.NoError(t, err)
}

func TestGetJobsById(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		converter := instructions.NewInstructionConverter(metrics.Get(), userAnnotationPrefix, &compress.NoOpCompressor{}, true)
		store := lookoutdb.NewLookoutDb(db, metrics.Get(), 3, 10)

		job := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, baseTime, &JobOptions{JobId: jobId}).
			Build().
			Job()

		_ = NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, baseTime, &JobOptions{JobId: "01f3j0g1md4qx7z5qb148qnaaa"}).
			Build().
			Job()

		_ = NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, baseTime, &JobOptions{JobId: "01f3j0g1md4qx7z5qb148qnbbb"}).
			Build().
			Job()

		repo := NewSqlGetJobsRepository(db)

		t.Run("exact", func(t *testing.T) {
			result, err := repo.GetJobs(
				context.TODO(),
				[]*model.Filter{{
					Field: "jobId",
					Match: model.MatchExact,
					Value: jobId,
				}},
				&model.Order{},
				0,
				10,
			)
			assert.NoError(t, err)
			assert.Len(t, result.Jobs, 1)
			assert.Equal(t, 1, result.Count)
			assert.Equal(t, job, result.Jobs[0])
		})

		return nil
	})
	assert.NoError(t, err)
}

func TestGetJobsByQueue(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		converter := instructions.NewInstructionConverter(metrics.Get(), userAnnotationPrefix, &compress.NoOpCompressor{}, true)
		store := lookoutdb.NewLookoutDb(db, metrics.Get(), 3, 10)

		job := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, baseTime, basicJobOpts).
			Build().
			Job()

		job2 := NewJobSimulator(converter, store).
			Submit("queue-2", jobSet, owner, baseTime, basicJobOpts).
			Build().
			Job()

		job3 := NewJobSimulator(converter, store).
			Submit("queue-3", jobSet, owner, baseTime, basicJobOpts).
			Build().
			Job()

		job4 := NewJobSimulator(converter, store).
			Submit("other-queue", jobSet, owner, baseTime, basicJobOpts).
			Build().
			Job()

		_ = NewJobSimulator(converter, store).
			Submit("something-else", jobSet, owner, baseTime, basicJobOpts).
			Build().
			Job()

		repo := NewSqlGetJobsRepository(db)

		t.Run("exact", func(t *testing.T) {
			result, err := repo.GetJobs(
				context.TODO(),
				[]*model.Filter{{
					Field: "queue",
					Match: model.MatchExact,
					Value: queue,
				}},
				&model.Order{},
				0,
				10,
			)
			assert.NoError(t, err)
			assert.Len(t, result.Jobs, 1)
			assert.Equal(t, 1, result.Count)
			assert.Equal(t, job, result.Jobs[0])
		})

		t.Run("startsWith", func(t *testing.T) {
			result, err := repo.GetJobs(
				context.TODO(),
				[]*model.Filter{{
					Field: "queue",
					Match: model.MatchStartsWith,
					Value: "queue-",
				}},
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			assert.NoError(t, err)
			assert.Len(t, result.Jobs, 3)
			assert.Equal(t, 3, result.Count)
			assert.Equal(t, job, result.Jobs[0])
			assert.Equal(t, job2, result.Jobs[1])
			assert.Equal(t, job3, result.Jobs[2])
		})

		t.Run("contains", func(t *testing.T) {
			result, err := repo.GetJobs(
				context.TODO(),
				[]*model.Filter{{
					Field: "queue",
					Match: model.MatchContains,
					Value: "queue",
				}},
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			assert.NoError(t, err)
			assert.Len(t, result.Jobs, 4)
			assert.Equal(t, 4, result.Count)
			assert.Equal(t, job, result.Jobs[0])
			assert.Equal(t, job2, result.Jobs[1])
			assert.Equal(t, job3, result.Jobs[2])
			assert.Equal(t, job4, result.Jobs[3])
		})

		return nil
	})
	assert.NoError(t, err)
}

func TestGetJobsByJobSet(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		converter := instructions.NewInstructionConverter(metrics.Get(), userAnnotationPrefix, &compress.NoOpCompressor{}, true)
		store := lookoutdb.NewLookoutDb(db, metrics.Get(), 3, 10)

		job := NewJobSimulator(converter, store).
			Submit(queue, "job\\set\\1", owner, baseTime, basicJobOpts).
			Build().
			Job()

		job2 := NewJobSimulator(converter, store).
			Submit(queue, "job\\set\\2", owner, baseTime, basicJobOpts).
			Build().
			Job()

		job3 := NewJobSimulator(converter, store).
			Submit(queue, "job\\set\\3", owner, baseTime, basicJobOpts).
			Build().
			Job()

		job4 := NewJobSimulator(converter, store).
			Submit(queue, "other-job\\set", owner, baseTime, basicJobOpts).
			Build().
			Job()

		_ = NewJobSimulator(converter, store).
			Submit(queue, "something-else", owner, baseTime, basicJobOpts).
			Build().
			Job()

		repo := NewSqlGetJobsRepository(db)

		t.Run("exact", func(t *testing.T) {
			result, err := repo.GetJobs(
				context.TODO(),
				[]*model.Filter{{
					Field: "jobSet",
					Match: model.MatchExact,
					Value: "job\\set\\1",
				}},
				&model.Order{},
				0,
				10,
			)
			assert.NoError(t, err)
			assert.Len(t, result.Jobs, 1)
			assert.Equal(t, 1, result.Count)
			assert.Equal(t, job, result.Jobs[0])
		})

		t.Run("startsWith", func(t *testing.T) {
			result, err := repo.GetJobs(
				context.TODO(),
				[]*model.Filter{{
					Field: "jobSet",
					Match: model.MatchStartsWith,
					Value: "job\\set\\",
				}},
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			assert.NoError(t, err)
			assert.Len(t, result.Jobs, 3)
			assert.Equal(t, 3, result.Count)
			assert.Equal(t, job, result.Jobs[0])
			assert.Equal(t, job2, result.Jobs[1])
			assert.Equal(t, job3, result.Jobs[2])
		})

		t.Run("contains", func(t *testing.T) {
			result, err := repo.GetJobs(
				context.TODO(),
				[]*model.Filter{{
					Field: "jobSet",
					Match: model.MatchContains,
					Value: "job\\set",
				}},
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			assert.NoError(t, err)
			assert.Len(t, result.Jobs, 4)
			assert.Equal(t, 4, result.Count)
			assert.Equal(t, job, result.Jobs[0])
			assert.Equal(t, job2, result.Jobs[1])
			assert.Equal(t, job3, result.Jobs[2])
			assert.Equal(t, job4, result.Jobs[3])
		})

		return nil
	})
	assert.NoError(t, err)
}

func TestGetJobsByOwner(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		converter := instructions.NewInstructionConverter(metrics.Get(), userAnnotationPrefix, &compress.NoOpCompressor{}, true)
		store := lookoutdb.NewLookoutDb(db, metrics.Get(), 3, 10)

		job := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, baseTime, basicJobOpts).
			Build().
			Job()

		job2 := NewJobSimulator(converter, store).
			Submit(queue, jobSet, "user-2", baseTime, basicJobOpts).
			Build().
			Job()

		job3 := NewJobSimulator(converter, store).
			Submit(queue, jobSet, "user-3", baseTime, basicJobOpts).
			Build().
			Job()

		job4 := NewJobSimulator(converter, store).
			Submit(queue, jobSet, "other-user", baseTime, basicJobOpts).
			Build().
			Job()

		_ = NewJobSimulator(converter, store).
			Submit(queue, jobSet, "something-else", baseTime, basicJobOpts).
			Build().
			Job()

		repo := NewSqlGetJobsRepository(db)

		t.Run("exact", func(t *testing.T) {
			result, err := repo.GetJobs(
				context.TODO(),
				[]*model.Filter{{
					Field: "owner",
					Match: model.MatchExact,
					Value: owner,
				}},
				&model.Order{},
				0,
				10,
			)
			assert.NoError(t, err)
			assert.Len(t, result.Jobs, 1)
			assert.Equal(t, 1, result.Count)
			assert.Equal(t, job, result.Jobs[0])
		})

		t.Run("startsWith", func(t *testing.T) {
			result, err := repo.GetJobs(
				context.TODO(),
				[]*model.Filter{{
					Field: "owner",
					Match: model.MatchStartsWith,
					Value: "user-",
				}},
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			assert.NoError(t, err)
			assert.Len(t, result.Jobs, 3)
			assert.Equal(t, result.Count, 3)
			assert.Equal(t, job, result.Jobs[0])
			assert.Equal(t, job2, result.Jobs[1])
			assert.Equal(t, job3, result.Jobs[2])
		})

		t.Run("contains", func(t *testing.T) {
			result, err := repo.GetJobs(
				context.TODO(),
				[]*model.Filter{{
					Field: "owner",
					Match: model.MatchContains,
					Value: "user",
				}},
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			assert.NoError(t, err)
			assert.Len(t, result.Jobs, 4)
			assert.Equal(t, result.Count, 4)
			assert.Equal(t, job, result.Jobs[0])
			assert.Equal(t, job2, result.Jobs[1])
			assert.Equal(t, job3, result.Jobs[2])
			assert.Equal(t, job4, result.Jobs[3])
		})

		return nil
	})
	assert.NoError(t, err)
}

func TestGetJobsByState(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		converter := instructions.NewInstructionConverter(metrics.Get(), userAnnotationPrefix, &compress.NoOpCompressor{}, true)
		store := lookoutdb.NewLookoutDb(db, metrics.Get(), 3, 10)

		queued := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, baseTime, basicJobOpts).
			Build().
			Job()

		pending := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, baseTime, basicJobOpts).
			Pending(uuid.NewString(), cluster, baseTime).
			Build().
			Job()

		runId2 := uuid.NewString()
		running := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, baseTime, basicJobOpts).
			Pending(runId2, cluster, baseTime).
			Running(runId2, node, baseTime).
			Build().
			Job()

		runId3 := uuid.NewString()
		_ = NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, baseTime, basicJobOpts).
			Pending(runId3, cluster, baseTime).
			Running(runId3, node, baseTime).
			Succeeded(baseTime).
			Build().
			Job()

		repo := NewSqlGetJobsRepository(db)

		t.Run("exact", func(t *testing.T) {
			result, err := repo.GetJobs(
				context.TODO(),
				[]*model.Filter{{
					Field: "state",
					Match: model.MatchExact,
					Value: string(lookout.JobRunning),
				}},
				&model.Order{},
				0,
				10,
			)
			assert.NoError(t, err)
			assert.Len(t, result.Jobs, 1)
			assert.Equal(t, 1, result.Count)
			assert.Equal(t, running, result.Jobs[0])
		})

		t.Run("anyOf", func(t *testing.T) {
			result, err := repo.GetJobs(
				context.TODO(),
				[]*model.Filter{{
					Field: "state",
					Match: model.MatchAnyOf,
					Value: []string{
						string(lookout.JobQueued),
						string(lookout.JobPending),
						string(lookout.JobRunning),
					},
				}},
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			assert.NoError(t, err)
			assert.Len(t, result.Jobs, 3)
			assert.Equal(t, 3, result.Count)
			assert.Equal(t, queued, result.Jobs[0])
			assert.Equal(t, pending, result.Jobs[1])
			assert.Equal(t, running, result.Jobs[2])
		})

		return nil
	})
	assert.NoError(t, err)
}

func TestGetJobsByAnnotation(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		converter := instructions.NewInstructionConverter(metrics.Get(), userAnnotationPrefix, &compress.NoOpCompressor{}, true)
		store := lookoutdb.NewLookoutDb(db, metrics.Get(), 3, 10)

		job := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, baseTime, &JobOptions{
				Annotations: map[string]string{
					"annotation-key-1": "annotation-value-1",
					"annotation-key-2": "annotation-value-3",
				},
			}).
			Build().
			Job()

		_ = NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, baseTime, &JobOptions{
				Annotations: map[string]string{
					"annotation-key-1": "annotation-value-2",
				},
			}).
			Build().
			Job()

		_ = NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, baseTime, &JobOptions{
				Annotations: map[string]string{
					"annotation-key-1": "annotation-value-3",
				},
			}).
			Build().
			Job()

		_ = NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, baseTime, &JobOptions{
				Annotations: map[string]string{
					"annotation-key-2": "annotation-value-1",
				},
			}).
			Build().
			Job()

		job2 := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, baseTime, &JobOptions{
				Annotations: map[string]string{
					"annotation-key-1": "annotation-value-6",
					"annotation-key-2": "annotation-value-4",
				},
			}).
			Build().
			Job()

		repo := NewSqlGetJobsRepository(db)

		t.Run("exact", func(t *testing.T) {
			result, err := repo.GetJobs(
				context.TODO(),
				[]*model.Filter{{
					Field:        "annotation-key-1",
					Match:        model.MatchExact,
					Value:        "annotation-value-1",
					IsAnnotation: true,
				}},
				&model.Order{},
				0,
				10,
			)
			assert.NoError(t, err)
			assert.Len(t, result.Jobs, 1)
			assert.Equal(t, 1, result.Count)
			assert.Equal(t, job, result.Jobs[0])
		})

		t.Run("exact, multiple annotations", func(t *testing.T) {
			result, err := repo.GetJobs(
				context.TODO(),
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
				&model.Order{},
				0,
				10,
			)
			assert.NoError(t, err)
			assert.Len(t, result.Jobs, 1)
			assert.Equal(t, 1, result.Count)
			assert.Equal(t, job, result.Jobs[0])
		})

		t.Run("startsWith, multiple annotations", func(t *testing.T) {
			result, err := repo.GetJobs(
				context.TODO(),
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
				&model.Order{},
				0,
				10,
			)
			assert.NoError(t, err)
			assert.Len(t, result.Jobs, 2)
			assert.Equal(t, 2, result.Count)
			assert.Equal(t, job, result.Jobs[0])
			assert.Equal(t, job2, result.Jobs[1])
		})

		t.Run("contains, multiple annotations", func(t *testing.T) {
			result, err := repo.GetJobs(
				context.TODO(),
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
				&model.Order{},
				0,
				10,
			)
			assert.NoError(t, err)
			assert.Len(t, result.Jobs, 2)
			assert.Equal(t, 2, result.Count)
			assert.Equal(t, job, result.Jobs[0])
			assert.Equal(t, job2, result.Jobs[1])
		})

		return nil
	})
	assert.NoError(t, err)
}

func TestGetJobsByCpu(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		converter := instructions.NewInstructionConverter(metrics.Get(), userAnnotationPrefix, &compress.NoOpCompressor{}, true)
		store := lookoutdb.NewLookoutDb(db, metrics.Get(), 3, 10)

		job1 := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, baseTime, &JobOptions{
				Cpu: resource.MustParse("1"),
			}).
			Build().
			Job()

		job2 := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, baseTime, &JobOptions{
				Cpu: resource.MustParse("3"),
			}).
			Build().
			Job()

		job3 := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, baseTime, &JobOptions{
				Cpu: resource.MustParse("5"),
			}).
			Build().
			Job()

		job4 := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, baseTime, &JobOptions{
				Cpu: resource.MustParse("10"),
			}).
			Build().
			Job()

		repo := NewSqlGetJobsRepository(db)

		t.Run("exact", func(t *testing.T) {
			result, err := repo.GetJobs(
				context.TODO(),
				[]*model.Filter{{
					Field: "cpu",
					Match: model.MatchExact,
					Value: 3000,
				}},
				&model.Order{},
				0,
				10,
			)
			assert.NoError(t, err)
			assert.Len(t, result.Jobs, 1)
			assert.Equal(t, 1, result.Count)
			assert.Equal(t, job2, result.Jobs[0])
		})

		t.Run("greaterThan", func(t *testing.T) {
			result, err := repo.GetJobs(
				context.TODO(),
				[]*model.Filter{{
					Field: "cpu",
					Match: model.MatchGreaterThan,
					Value: 3000,
				}},
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			assert.NoError(t, err)
			assert.Len(t, result.Jobs, 2)
			assert.Equal(t, 2, result.Count)
			assert.Equal(t, job3, result.Jobs[0])
			assert.Equal(t, job4, result.Jobs[1])
		})

		t.Run("lessThan", func(t *testing.T) {
			result, err := repo.GetJobs(
				context.TODO(),
				[]*model.Filter{{
					Field: "cpu",
					Match: model.MatchLessThan,
					Value: 5000,
				}},
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			assert.NoError(t, err)
			assert.Len(t, result.Jobs, 2)
			assert.Equal(t, 2, result.Count)
			assert.Equal(t, job1, result.Jobs[0])
			assert.Equal(t, job2, result.Jobs[1])
		})

		t.Run("greaterThanOrEqualTo", func(t *testing.T) {
			result, err := repo.GetJobs(
				context.TODO(),
				[]*model.Filter{{
					Field: "cpu",
					Match: model.MatchGreaterThanOrEqualTo,
					Value: 3000,
				}},
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			assert.NoError(t, err)
			assert.Len(t, result.Jobs, 3)
			assert.Equal(t, 3, result.Count)
			assert.Equal(t, job2, result.Jobs[0])
			assert.Equal(t, job3, result.Jobs[1])
			assert.Equal(t, job4, result.Jobs[2])
		})

		t.Run("lessThanOrEqualTo", func(t *testing.T) {
			result, err := repo.GetJobs(
				context.TODO(),
				[]*model.Filter{{
					Field: "cpu",
					Match: model.MatchLessThanOrEqualTo,
					Value: 5000,
				}},
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			assert.NoError(t, err)
			assert.Len(t, result.Jobs, 3)
			assert.Equal(t, 3, result.Count)
			assert.Equal(t, job1, result.Jobs[0])
			assert.Equal(t, job2, result.Jobs[1])
			assert.Equal(t, job3, result.Jobs[2])
		})

		return nil
	})
	assert.NoError(t, err)
}

func TestGetJobsByMemory(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		converter := instructions.NewInstructionConverter(metrics.Get(), userAnnotationPrefix, &compress.NoOpCompressor{}, true)
		store := lookoutdb.NewLookoutDb(db, metrics.Get(), 3, 10)

		job1 := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, baseTime, &JobOptions{
				Memory: resource.MustParse("1000"),
			}).
			Build().
			Job()

		job2 := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, baseTime, &JobOptions{
				Memory: resource.MustParse("3000"),
			}).
			Build().
			Job()

		job3 := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, baseTime, &JobOptions{
				Memory: resource.MustParse("5000"),
			}).
			Build().
			Job()

		job4 := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, baseTime, &JobOptions{
				Memory: resource.MustParse("10000"),
			}).
			Build().
			Job()

		repo := NewSqlGetJobsRepository(db)

		t.Run("exact", func(t *testing.T) {
			result, err := repo.GetJobs(
				context.TODO(),
				[]*model.Filter{{
					Field: "memory",
					Match: model.MatchExact,
					Value: 3000,
				}},
				&model.Order{},
				0,
				10,
			)
			assert.NoError(t, err)
			assert.Len(t, result.Jobs, 1)
			assert.Equal(t, 1, result.Count)
			assert.Equal(t, job2, result.Jobs[0])
		})

		t.Run("greaterThan", func(t *testing.T) {
			result, err := repo.GetJobs(
				context.TODO(),
				[]*model.Filter{{
					Field: "memory",
					Match: model.MatchGreaterThan,
					Value: 3000,
				}},
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			assert.NoError(t, err)
			assert.Len(t, result.Jobs, 2)
			assert.Equal(t, 2, result.Count)
			assert.Equal(t, job3, result.Jobs[0])
			assert.Equal(t, job4, result.Jobs[1])
		})

		t.Run("lessThan", func(t *testing.T) {
			result, err := repo.GetJobs(
				context.TODO(),
				[]*model.Filter{{
					Field: "memory",
					Match: model.MatchLessThan,
					Value: 5000,
				}},
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			assert.NoError(t, err)
			assert.Len(t, result.Jobs, 2)
			assert.Equal(t, 2, result.Count)
			assert.Equal(t, job1, result.Jobs[0])
			assert.Equal(t, job2, result.Jobs[1])
		})

		t.Run("greaterThanOrEqualTo", func(t *testing.T) {
			result, err := repo.GetJobs(
				context.TODO(),
				[]*model.Filter{{
					Field: "memory",
					Match: model.MatchGreaterThanOrEqualTo,
					Value: 3000,
				}},
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			assert.NoError(t, err)
			assert.Len(t, result.Jobs, 3)
			assert.Equal(t, 3, result.Count)
			assert.Equal(t, job2, result.Jobs[0])
			assert.Equal(t, job3, result.Jobs[1])
			assert.Equal(t, job4, result.Jobs[2])
		})

		t.Run("lessThanOrEqualTo", func(t *testing.T) {
			result, err := repo.GetJobs(
				context.TODO(),
				[]*model.Filter{{
					Field: "memory",
					Match: model.MatchLessThanOrEqualTo,
					Value: 5000,
				}},
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			assert.NoError(t, err)
			assert.Len(t, result.Jobs, 3)
			assert.Equal(t, 3, result.Count)
			assert.Equal(t, job1, result.Jobs[0])
			assert.Equal(t, job2, result.Jobs[1])
			assert.Equal(t, job3, result.Jobs[2])
		})

		return nil
	})
	assert.NoError(t, err)
}

func TestGetJobsByEphemeralStorage(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		converter := instructions.NewInstructionConverter(metrics.Get(), userAnnotationPrefix, &compress.NoOpCompressor{}, true)
		store := lookoutdb.NewLookoutDb(db, metrics.Get(), 3, 10)

		job1 := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, baseTime, &JobOptions{
				EphemeralStorage: resource.MustParse("1000"),
			}).
			Build().
			Job()

		job2 := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, baseTime, &JobOptions{
				EphemeralStorage: resource.MustParse("3000"),
			}).
			Build().
			Job()

		job3 := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, baseTime, &JobOptions{
				EphemeralStorage: resource.MustParse("5000"),
			}).
			Build().
			Job()

		job4 := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, baseTime, &JobOptions{
				EphemeralStorage: resource.MustParse("10000"),
			}).
			Build().
			Job()

		repo := NewSqlGetJobsRepository(db)

		t.Run("exact", func(t *testing.T) {
			result, err := repo.GetJobs(
				context.TODO(),
				[]*model.Filter{{
					Field: "ephemeralStorage",
					Match: model.MatchExact,
					Value: 3000,
				}},
				&model.Order{},
				0,
				10,
			)
			assert.NoError(t, err)
			assert.Len(t, result.Jobs, 1)
			assert.Equal(t, 1, result.Count)
			assert.Equal(t, job2, result.Jobs[0])
		})

		t.Run("greaterThan", func(t *testing.T) {
			result, err := repo.GetJobs(
				context.TODO(),
				[]*model.Filter{{
					Field: "ephemeralStorage",
					Match: model.MatchGreaterThan,
					Value: 3000,
				}},
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			assert.NoError(t, err)
			assert.Len(t, result.Jobs, 2)
			assert.Equal(t, 2, result.Count)
			assert.Equal(t, job3, result.Jobs[0])
			assert.Equal(t, job4, result.Jobs[1])
		})

		t.Run("lessThan", func(t *testing.T) {
			result, err := repo.GetJobs(
				context.TODO(),
				[]*model.Filter{{
					Field: "ephemeralStorage",
					Match: model.MatchLessThan,
					Value: 5000,
				}},
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			assert.NoError(t, err)
			assert.Len(t, result.Jobs, 2)
			assert.Equal(t, 2, result.Count)
			assert.Equal(t, job1, result.Jobs[0])
			assert.Equal(t, job2, result.Jobs[1])
		})

		t.Run("greaterThanOrEqualTo", func(t *testing.T) {
			result, err := repo.GetJobs(
				context.TODO(),
				[]*model.Filter{{
					Field: "ephemeralStorage",
					Match: model.MatchGreaterThanOrEqualTo,
					Value: 3000,
				}},
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			assert.NoError(t, err)
			assert.Len(t, result.Jobs, 3)
			assert.Equal(t, 3, result.Count)
			assert.Equal(t, job2, result.Jobs[0])
			assert.Equal(t, job3, result.Jobs[1])
			assert.Equal(t, job4, result.Jobs[2])
		})

		t.Run("lessThanOrEqualTo", func(t *testing.T) {
			result, err := repo.GetJobs(
				context.TODO(),
				[]*model.Filter{{
					Field: "ephemeralStorage",
					Match: model.MatchLessThanOrEqualTo,
					Value: 5000,
				}},
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			assert.NoError(t, err)
			assert.Len(t, result.Jobs, 3)
			assert.Equal(t, 3, result.Count)
			assert.Equal(t, job1, result.Jobs[0])
			assert.Equal(t, job2, result.Jobs[1])
			assert.Equal(t, job3, result.Jobs[2])
		})

		return nil
	})
	assert.NoError(t, err)
}

func TestGetJobsByGpu(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		converter := instructions.NewInstructionConverter(metrics.Get(), userAnnotationPrefix, &compress.NoOpCompressor{}, true)
		store := lookoutdb.NewLookoutDb(db, metrics.Get(), 3, 10)

		job1 := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, baseTime, &JobOptions{
				Gpu: resource.MustParse("1"),
			}).
			Build().
			Job()

		job2 := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, baseTime, &JobOptions{
				Gpu: resource.MustParse("3"),
			}).
			Build().
			Job()

		job3 := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, baseTime, &JobOptions{
				Gpu: resource.MustParse("5"),
			}).
			Build().
			Job()

		job4 := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, baseTime, &JobOptions{
				Gpu: resource.MustParse("8"),
			}).
			Build().
			Job()

		repo := NewSqlGetJobsRepository(db)

		t.Run("exact", func(t *testing.T) {
			result, err := repo.GetJobs(
				context.TODO(),
				[]*model.Filter{{
					Field: "gpu",
					Match: model.MatchExact,
					Value: 3,
				}},
				&model.Order{},
				0,
				10,
			)
			assert.NoError(t, err)
			assert.Len(t, result.Jobs, 1)
			assert.Equal(t, 1, result.Count)
			assert.Equal(t, job2, result.Jobs[0])
		})

		t.Run("greaterThan", func(t *testing.T) {
			result, err := repo.GetJobs(
				context.TODO(),
				[]*model.Filter{{
					Field: "gpu",
					Match: model.MatchGreaterThan,
					Value: 3,
				}},
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			assert.NoError(t, err)
			assert.Len(t, result.Jobs, 2)
			assert.Equal(t, 2, result.Count)
			assert.Equal(t, job3, result.Jobs[0])
			assert.Equal(t, job4, result.Jobs[1])
		})

		t.Run("lessThan", func(t *testing.T) {
			result, err := repo.GetJobs(
				context.TODO(),
				[]*model.Filter{{
					Field: "gpu",
					Match: model.MatchLessThan,
					Value: 5,
				}},
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			assert.NoError(t, err)
			assert.Len(t, result.Jobs, 2)
			assert.Equal(t, 2, result.Count)
			assert.Equal(t, job1, result.Jobs[0])
			assert.Equal(t, job2, result.Jobs[1])
		})

		t.Run("greaterThanOrEqualTo", func(t *testing.T) {
			result, err := repo.GetJobs(
				context.TODO(),
				[]*model.Filter{{
					Field: "gpu",
					Match: model.MatchGreaterThanOrEqualTo,
					Value: 3,
				}},
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			assert.NoError(t, err)
			assert.Len(t, result.Jobs, 3)
			assert.Equal(t, 3, result.Count)
			assert.Equal(t, job2, result.Jobs[0])
			assert.Equal(t, job3, result.Jobs[1])
			assert.Equal(t, job4, result.Jobs[2])
		})

		t.Run("lessThanOrEqualTo", func(t *testing.T) {
			result, err := repo.GetJobs(
				context.TODO(),
				[]*model.Filter{{
					Field: "gpu",
					Match: model.MatchLessThanOrEqualTo,
					Value: 5,
				}},
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			assert.NoError(t, err)
			assert.Len(t, result.Jobs, 3)
			assert.Equal(t, 3, result.Count)
			assert.Equal(t, job1, result.Jobs[0])
			assert.Equal(t, job2, result.Jobs[1])
			assert.Equal(t, job3, result.Jobs[2])
		})

		return nil
	})
	assert.NoError(t, err)
}

func TestGetJobsByPriority(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		converter := instructions.NewInstructionConverter(metrics.Get(), userAnnotationPrefix, &compress.NoOpCompressor{}, true)
		store := lookoutdb.NewLookoutDb(db, metrics.Get(), 3, 10)

		job1 := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, baseTime, &JobOptions{
				Priority: 10,
			}).
			Build().
			Job()

		job2 := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, baseTime, &JobOptions{
				Priority: 20,
			}).
			Build().
			Job()

		job3 := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, baseTime, &JobOptions{
				Priority: 30,
			}).
			Build().
			Job()

		job4 := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, baseTime, &JobOptions{
				Priority: 40,
			}).
			Build().
			Job()

		repo := NewSqlGetJobsRepository(db)

		t.Run("exact", func(t *testing.T) {
			result, err := repo.GetJobs(
				context.TODO(),
				[]*model.Filter{{
					Field: "priority",
					Match: model.MatchExact,
					Value: 20,
				}},
				&model.Order{},
				0,
				10,
			)
			assert.NoError(t, err)
			assert.Len(t, result.Jobs, 1)
			assert.Equal(t, 1, result.Count)
			assert.Equal(t, job2, result.Jobs[0])
		})

		t.Run("greaterThan", func(t *testing.T) {
			result, err := repo.GetJobs(
				context.TODO(),
				[]*model.Filter{{
					Field: "priority",
					Match: model.MatchGreaterThan,
					Value: 20,
				}},
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			assert.NoError(t, err)
			assert.Len(t, result.Jobs, 2)
			assert.Equal(t, 2, result.Count)
			assert.Equal(t, job3, result.Jobs[0])
			assert.Equal(t, job4, result.Jobs[1])
		})

		t.Run("lessThan", func(t *testing.T) {
			result, err := repo.GetJobs(
				context.TODO(),
				[]*model.Filter{{
					Field: "priority",
					Match: model.MatchLessThan,
					Value: 30,
				}},
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			assert.NoError(t, err)
			assert.Len(t, result.Jobs, 2)
			assert.Equal(t, 2, result.Count)
			assert.Equal(t, job1, result.Jobs[0])
			assert.Equal(t, job2, result.Jobs[1])
		})

		t.Run("greaterThanOrEqualTo", func(t *testing.T) {
			result, err := repo.GetJobs(
				context.TODO(),
				[]*model.Filter{{
					Field: "priority",
					Match: model.MatchGreaterThanOrEqualTo,
					Value: 20,
				}},
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			assert.NoError(t, err)
			assert.Len(t, result.Jobs, 3)
			assert.Equal(t, 3, result.Count)
			assert.Equal(t, job2, result.Jobs[0])
			assert.Equal(t, job3, result.Jobs[1])
			assert.Equal(t, job4, result.Jobs[2])
		})

		t.Run("lessThanOrEqualTo", func(t *testing.T) {
			result, err := repo.GetJobs(
				context.TODO(),
				[]*model.Filter{{
					Field: "priority",
					Match: model.MatchLessThanOrEqualTo,
					Value: 30,
				}},
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			assert.NoError(t, err)
			assert.Len(t, result.Jobs, 3)
			assert.Equal(t, 3, result.Count)
			assert.Equal(t, job1, result.Jobs[0])
			assert.Equal(t, job2, result.Jobs[1])
			assert.Equal(t, job3, result.Jobs[2])
		})

		return nil
	})
	assert.NoError(t, err)
}

func TestGetJobsByPriorityClass(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		converter := instructions.NewInstructionConverter(metrics.Get(), userAnnotationPrefix, &compress.NoOpCompressor{}, true)
		store := lookoutdb.NewLookoutDb(db, metrics.Get(), 3, 10)

		job := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, baseTime, &JobOptions{
				PriorityClass: "priority-class-1",
			}).
			Build().
			Job()

		job2 := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, baseTime, &JobOptions{
				PriorityClass: "priority-class-2",
			}).
			Build().
			Job()

		job3 := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, baseTime, &JobOptions{
				PriorityClass: "priority-class-3",
			}).
			Build().
			Job()

		job4 := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, baseTime, &JobOptions{
				PriorityClass: "other-priority-class",
			}).
			Build().
			Job()

		_ = NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, baseTime, &JobOptions{
				PriorityClass: "something-else",
			}).
			Build().
			Job()

		repo := NewSqlGetJobsRepository(db)

		t.Run("exact", func(t *testing.T) {
			result, err := repo.GetJobs(
				context.TODO(),
				[]*model.Filter{{
					Field: "priorityClass",
					Match: model.MatchExact,
					Value: "priority-class-1",
				}},
				&model.Order{},
				0,
				10,
			)
			assert.NoError(t, err)
			assert.Len(t, result.Jobs, 1)
			assert.Equal(t, 1, result.Count)
			assert.Equal(t, job, result.Jobs[0])
		})

		t.Run("startsWith", func(t *testing.T) {
			result, err := repo.GetJobs(
				context.TODO(),
				[]*model.Filter{{
					Field: "priorityClass",
					Match: model.MatchStartsWith,
					Value: "priority-class-",
				}},
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			assert.NoError(t, err)
			assert.Len(t, result.Jobs, 3)
			assert.Equal(t, result.Count, 3)
			assert.Equal(t, job, result.Jobs[0])
			assert.Equal(t, job2, result.Jobs[1])
			assert.Equal(t, job3, result.Jobs[2])
		})

		t.Run("contains", func(t *testing.T) {
			result, err := repo.GetJobs(
				context.TODO(),
				[]*model.Filter{{
					Field: "priorityClass",
					Match: model.MatchContains,
					Value: "priority-class",
				}},
				&model.Order{
					Field:     "jobId",
					Direction: model.DirectionAsc,
				},
				0,
				10,
			)
			assert.NoError(t, err)
			assert.Len(t, result.Jobs, 4)
			assert.Equal(t, result.Count, 4)
			assert.Equal(t, job, result.Jobs[0])
			assert.Equal(t, job2, result.Jobs[1])
			assert.Equal(t, job3, result.Jobs[2])
			assert.Equal(t, job4, result.Jobs[3])
		})

		return nil
	})
	assert.NoError(t, err)
}

func TestGetJobsSkip(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		converter := instructions.NewInstructionConverter(metrics.Get(), userAnnotationPrefix, &compress.NoOpCompressor{}, true)
		store := lookoutdb.NewLookoutDb(db, metrics.Get(), 3, 10)

		nJobs := 15
		jobs := make([]*model.Job, nJobs)
		for i := 0; i < nJobs; i++ {
			jobId := util.NewULID()
			jobs[i] = NewJobSimulator(converter, store).
				Submit(queue, jobSet, owner, baseTime, &JobOptions{JobId: jobId}).
				Build().
				Job()
		}

		repo := NewSqlGetJobsRepository(db)

		t.Run("skip 3", func(t *testing.T) {
			skip := 3
			take := 5
			result, err := repo.GetJobs(
				context.TODO(),
				[]*model.Filter{},
				&model.Order{
					Field:     "jobId",
					Direction: "ASC",
				},
				skip,
				take,
			)
			assert.NoError(t, err)
			assert.Len(t, result.Jobs, take)
			assert.Equal(t, nJobs, result.Count)
			assert.Equal(t, jobs[skip:skip+take], result.Jobs)
		})

		t.Run("skip 7", func(t *testing.T) {
			skip := 7
			take := 5
			result, err := repo.GetJobs(
				context.TODO(),
				[]*model.Filter{},
				&model.Order{
					Field:     "jobId",
					Direction: "ASC",
				},
				skip,
				take,
			)
			assert.NoError(t, err)
			assert.Len(t, result.Jobs, take)
			assert.Equal(t, nJobs, result.Count)
			assert.Equal(t, jobs[skip:skip+take], result.Jobs)
		})

		t.Run("skip 13", func(t *testing.T) {
			skip := 13
			take := 5
			result, err := repo.GetJobs(
				context.TODO(),
				[]*model.Filter{},
				&model.Order{
					Field:     "jobId",
					Direction: "ASC",
				},
				skip,
				take,
			)
			assert.NoError(t, err)
			assert.Len(t, result.Jobs, 2)
			assert.Equal(t, nJobs, result.Count)
			assert.Equal(t, jobs[skip:], result.Jobs)
		})

		return nil
	})
	assert.NoError(t, err)
}

func TestGetJobsComplex(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		converter := instructions.NewInstructionConverter(metrics.Get(), userAnnotationPrefix, &compress.NoOpCompressor{}, true)
		store := lookoutdb.NewLookoutDb(db, metrics.Get(), 3, 10)

		nJobs := 15
		jobs := make([]*model.Job, nJobs)
		for i := 0; i < nJobs; i++ {
			jobId := util.NewULID()
			jobs[i] = NewJobSimulator(converter, store).
				Submit(queue, jobSet, owner, baseTime, &JobOptions{
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
				Submit("other-queue", jobSet, owner, baseTime, &JobOptions{
					JobId: util.NewULID(),
					Annotations: map[string]string{
						"a": "value-1",
						"b": "value-2",
					},
				}).
				Build().
				Job()
		}

		repo := NewSqlGetJobsRepository(db)

		skip := 8
		take := 5
		result, err := repo.GetJobs(
			context.TODO(),
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
			&model.Order{
				Field:     "jobId",
				Direction: "ASC",
			},
			skip,
			take,
		)
		assert.NoError(t, err)
		assert.Len(t, result.Jobs, take)
		assert.Equal(t, nJobs, result.Count)
		assert.Equal(t, jobs[skip:skip+take], result.Jobs)

		return nil
	})
	assert.NoError(t, err)
}
