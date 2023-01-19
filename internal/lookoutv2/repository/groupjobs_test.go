package repository

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/common/database/lookout"
	"github.com/armadaproject/armada/internal/lookoutingesterv2/instructions"
	"github.com/armadaproject/armada/internal/lookoutingesterv2/lookoutdb"
	"github.com/armadaproject/armada/internal/lookoutingesterv2/metrics"
	"github.com/armadaproject/armada/internal/lookoutv2/model"
)

func TestGroupByQueue(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		converter := instructions.NewInstructionConverter(metrics.Get(), userAnnotationPrefix, &compress.NoOpCompressor{})
		store := lookoutdb.NewLookoutDb(db, metrics.Get(), 3, 10)

		manyJobs(10, "queue-1", jobSet, lookout.JobQueued, make(map[string]string), converter, store)
		manyJobs(5, "queue-2", jobSet, lookout.JobQueued, make(map[string]string), converter, store)
		manyJobs(3, "queue-3", jobSet, lookout.JobQueued, make(map[string]string), converter, store)

		repo := NewSqlGroupJobsRepository(db)
		result, err := repo.GroupBy(
			context.TODO(),
			[]*model.Filter{},
			&model.Order{
				Field:     "count",
				Direction: "DESC",
			},
			"queue",
			[]string{},
			0,
			10,
		)
		assert.NoError(t, err)
		assert.Len(t, result.Groups, 3)
		assert.Equal(t, 3, result.Count)
		assert.Equal(t, result.Groups, []*model.JobGroup{
			{
				Name:       "queue-1",
				Count:      10,
				Aggregates: map[string]string{},
			},
			{
				Name:       "queue-2",
				Count:      5,
				Aggregates: map[string]string{},
			},
			{
				Name:       "queue-3",
				Count:      3,
				Aggregates: map[string]string{},
			},
		})
		return nil
	})
	assert.NoError(t, err)
}

func TestGroupByJobSet(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		converter := instructions.NewInstructionConverter(metrics.Get(), userAnnotationPrefix, &compress.NoOpCompressor{})
		store := lookoutdb.NewLookoutDb(db, metrics.Get(), 3, 10)

		manyJobs(10, queue, "job-set-1", lookout.JobQueued, make(map[string]string), converter, store)
		manyJobs(5, queue, "job-set-2", lookout.JobQueued, make(map[string]string), converter, store)
		manyJobs(3, queue, "job-set-3", lookout.JobQueued, make(map[string]string), converter, store)

		repo := NewSqlGroupJobsRepository(db)
		result, err := repo.GroupBy(
			context.TODO(),
			[]*model.Filter{},
			&model.Order{
				Field:     "count",
				Direction: "DESC",
			},
			"jobSet",
			[]string{},
			0,
			10,
		)
		assert.NoError(t, err)
		assert.Len(t, result.Groups, 3)
		assert.Equal(t, 3, result.Count)
		assert.Equal(t, result.Groups, []*model.JobGroup{
			{
				Name:       "job-set-1",
				Count:      10,
				Aggregates: map[string]string{},
			},
			{
				Name:       "job-set-2",
				Count:      5,
				Aggregates: map[string]string{},
			},
			{
				Name:       "job-set-3",
				Count:      3,
				Aggregates: map[string]string{},
			},
		})
		return nil
	})
	assert.NoError(t, err)
}

func TestGroupByState(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		converter := instructions.NewInstructionConverter(metrics.Get(), userAnnotationPrefix, &compress.NoOpCompressor{})
		store := lookoutdb.NewLookoutDb(db, metrics.Get(), 3, 10)

		manyJobs(10, queue, jobSet, lookout.JobQueued, make(map[string]string), converter, store)
		manyJobs(5, queue, jobSet, lookout.JobPending, make(map[string]string), converter, store)
		manyJobs(3, queue, jobSet, lookout.JobRunning, make(map[string]string), converter, store)
		manyJobs(2, queue, jobSet, lookout.JobFailed, make(map[string]string), converter, store)

		repo := NewSqlGroupJobsRepository(db)
		result, err := repo.GroupBy(
			context.TODO(),
			[]*model.Filter{},
			&model.Order{
				Field:     "count",
				Direction: "DESC",
			},
			"state",
			[]string{},
			0,
			10,
		)
		assert.NoError(t, err)
		assert.Len(t, result.Groups, 4)
		assert.Equal(t, 4, result.Count)
		assert.Equal(t, result.Groups, []*model.JobGroup{
			{
				Name:       string(lookout.JobQueued),
				Count:      10,
				Aggregates: map[string]string{},
			},
			{
				Name:       string(lookout.JobPending),
				Count:      5,
				Aggregates: map[string]string{},
			},
			{
				Name:       string(lookout.JobRunning),
				Count:      3,
				Aggregates: map[string]string{},
			},
			{
				Name:       string(lookout.JobFailed),
				Count:      2,
				Aggregates: map[string]string{},
			},
		})
		return nil
	})
	assert.NoError(t, err)
}

func TestGroupByWithFilters(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		converter := instructions.NewInstructionConverter(metrics.Get(), userAnnotationPrefix, &compress.NoOpCompressor{})
		store := lookoutdb.NewLookoutDb(db, metrics.Get(), 3, 10)

		testAnnotations := map[string]string{
			"key-1": "val-1",
			"key-2": "val-2",
		}

		manyJobs(10, queue, jobSet, lookout.JobQueued, testAnnotations, converter, store)
		manyJobs(5, queue, jobSet, lookout.JobPending, testAnnotations, converter, store)
		manyJobs(3, queue, jobSet, lookout.JobRunning, testAnnotations, converter, store)
		manyJobs(2, queue, jobSet, lookout.JobFailed, testAnnotations, converter, store)

		manyJobs(11, queue, jobSet, lookout.JobQueued, make(map[string]string), converter, store)
		manyJobs(6, queue, jobSet, lookout.JobPending, make(map[string]string), converter, store)
		manyJobs(4, queue, jobSet, lookout.JobRunning, make(map[string]string), converter, store)
		manyJobs(3, queue, jobSet, lookout.JobFailed, make(map[string]string), converter, store)

		manyJobs(12, "queue-2", jobSet, lookout.JobQueued, make(map[string]string), converter, store)
		manyJobs(7, queue, "queue-2", lookout.JobPending, make(map[string]string), converter, store)
		manyJobs(5, queue, "queue-2", lookout.JobRunning, make(map[string]string), converter, store)
		manyJobs(4, queue, "queue-2", lookout.JobFailed, make(map[string]string), converter, store)

		manyJobs(13, queue, "job-set-2", lookout.JobQueued, make(map[string]string), converter, store)
		manyJobs(8, queue, "job-set-2", lookout.JobPending, make(map[string]string), converter, store)
		manyJobs(6, queue, "job-set-2", lookout.JobRunning, make(map[string]string), converter, store)
		manyJobs(5, queue, "job-set-2", lookout.JobFailed, make(map[string]string), converter, store)

		repo := NewSqlGroupJobsRepository(db)
		result, err := repo.GroupBy(
			context.TODO(),
			[]*model.Filter{
				{
					Field: "queue",
					Match: model.MatchExact,
					Value: queue,
				},
				{
					Field: "jobSet",
					Match: model.MatchExact,
					Value: jobSet,
				},
				{
					Field:        "key-1",
					Match:        model.MatchExact,
					Value:        "val-1",
					IsAnnotation: true,
				},
				{
					Field:        "key-2",
					Match:        model.MatchStartsWith,
					Value:        "val-2",
					IsAnnotation: true,
				},
			},
			&model.Order{
				Field:     "count",
				Direction: "DESC",
			},
			"state",
			[]string{},
			0,
			10,
		)
		assert.NoError(t, err)
		assert.Len(t, result.Groups, 4)
		assert.Equal(t, 4, result.Count)
		assert.Equal(t, result.Groups, []*model.JobGroup{
			{
				Name:       string(lookout.JobQueued),
				Count:      10,
				Aggregates: map[string]string{},
			},
			{
				Name:       string(lookout.JobPending),
				Count:      5,
				Aggregates: map[string]string{},
			},
			{
				Name:       string(lookout.JobRunning),
				Count:      3,
				Aggregates: map[string]string{},
			},
			{
				Name:       string(lookout.JobFailed),
				Count:      2,
				Aggregates: map[string]string{},
			},
		})
		return nil
	})
	assert.NoError(t, err)
}

func TestGroupJobsSkip(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		converter := instructions.NewInstructionConverter(metrics.Get(), userAnnotationPrefix, &compress.NoOpCompressor{})
		store := lookoutdb.NewLookoutDb(db, metrics.Get(), 3, 10)

		nGroups := 15
		for i := 0; i < nGroups; i++ {
			manyJobs(i+1, fmt.Sprintf("queue-%d", i+1), jobSet, lookout.JobQueued, make(map[string]string), converter, store)
		}

		queueGroup := func(i int) *model.JobGroup {
			return &model.JobGroup{
				Name:       fmt.Sprintf("queue-%d", i),
				Count:      int64(i),
				Aggregates: map[string]string{},
			}
		}

		repo := NewSqlGroupJobsRepository(db)

		t.Run("skip 3", func(t *testing.T) {
			skip := 3
			take := 5
			result, err := repo.GroupBy(
				context.TODO(),
				[]*model.Filter{},
				&model.Order{
					Field:     "count",
					Direction: "ASC",
				},
				"queue",
				[]string{},
				skip,
				take,
			)
			assert.NoError(t, err)
			assert.Len(t, result.Groups, take)
			assert.Equal(t, nGroups, result.Count)
			assert.Equal(t, []*model.JobGroup{
				queueGroup(4),
				queueGroup(5),
				queueGroup(6),
				queueGroup(7),
				queueGroup(8),
			}, result.Groups)
		})

		t.Run("skip 7", func(t *testing.T) {
			skip := 7
			take := 5
			result, err := repo.GroupBy(
				context.TODO(),
				[]*model.Filter{},
				&model.Order{
					Field:     "count",
					Direction: "ASC",
				},
				"queue",
				[]string{},
				skip,
				take,
			)
			assert.NoError(t, err)
			assert.Len(t, result.Groups, take)
			assert.Equal(t, nGroups, result.Count)
			assert.Equal(t, []*model.JobGroup{
				queueGroup(8),
				queueGroup(9),
				queueGroup(10),
				queueGroup(11),
				queueGroup(12),
			}, result.Groups)
		})

		t.Run("skip 13", func(t *testing.T) {
			skip := 13
			take := 5
			result, err := repo.GroupBy(
				context.TODO(),
				[]*model.Filter{},
				&model.Order{
					Field:     "count",
					Direction: "ASC",
				},
				"queue",
				[]string{},
				skip,
				take,
			)
			assert.NoError(t, err)
			assert.Len(t, result.Groups, 2)
			assert.Equal(t, nGroups, result.Count)
			assert.Equal(t, []*model.JobGroup{
				queueGroup(14),
				queueGroup(15),
			}, result.Groups)
		})

		return nil
	})
	assert.NoError(t, err)
}

type createJobsFn func(queue, jobSet string, annotations map[string]string, converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb)

func manyJobs(n int, queue, jobSet string, state lookout.JobState, annotations map[string]string, converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb) {
	fn := getCreateJobsFn(state)
	for i := 0; i < n; i++ {
		fn(queue, jobSet, annotations, converter, store)
	}
}

func getCreateJobsFn(state lookout.JobState) createJobsFn {
	switch state {
	case lookout.JobQueued:
		return makeQueued
	case lookout.JobPending:
		return makePending
	case lookout.JobRunning:
		return makeRunning
	case lookout.JobFailed:
		return makeFailed
	default:
		return makeQueued
	}
}

func makeQueued(queue, jobSet string, annotations map[string]string, converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb) {
	NewJobSimulator(converter, store).
		Submit(queue, jobSet, owner, baseTime, &JobOptions{
			Annotations: annotations,
		}).
		Build()
}

func makePending(queue, jobSet string, annotations map[string]string, converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb) {
	NewJobSimulator(converter, store).
		Submit(queue, jobSet, owner, baseTime, &JobOptions{
			Annotations: annotations,
		}).
		Pending(uuid.NewString(), cluster, baseTime).
		Build()
}

func makeRunning(queue, jobSet string, annotations map[string]string, converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb) {
	runId := uuid.NewString()
	NewJobSimulator(converter, store).
		Submit(queue, jobSet, owner, baseTime, &JobOptions{
			Annotations: annotations,
		}).
		Pending(runId, cluster, baseTime).
		Running(runId, cluster, baseTime).
		Build()
}

func makeFailed(queue, jobSet string, annotations map[string]string, converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb) {
	runId := uuid.NewString()
	NewJobSimulator(converter, store).
		Submit(queue, jobSet, owner, baseTime, &JobOptions{
			Annotations: annotations,
		}).
		Pending(runId, cluster, baseTime).
		Running(runId, cluster, baseTime).
		RunFailed(runId, node, 1, "error", baseTime).
		Failed(node, 1, "error", baseTime).
		Build()
}
