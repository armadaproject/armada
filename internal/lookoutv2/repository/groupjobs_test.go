package repository

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/armadaproject/armada/internal/common/pointer"
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

		manyJobs(10, &createJobsOpts{
			queue:  "queue-1",
			jobSet: jobSet,
		}, converter, store)
		manyJobs(5, &createJobsOpts{
			queue:  "queue-2",
			jobSet: jobSet,
		}, converter, store)
		manyJobs(3, &createJobsOpts{
			queue:  "queue-3",
			jobSet: jobSet,
		}, converter, store)

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

		manyJobs(10, &createJobsOpts{
			queue:  queue,
			jobSet: "job-set-1",
		}, converter, store)
		manyJobs(5, &createJobsOpts{
			queue:  queue,
			jobSet: "job-set-2",
		}, converter, store)
		manyJobs(3, &createJobsOpts{
			queue:  queue,
			jobSet: "job-set-3",
		}, converter, store)

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

		manyJobs(10, &createJobsOpts{
			queue:  queue,
			jobSet: jobSet,
			state:  lookout.JobQueued,
		}, converter, store)
		manyJobs(5, &createJobsOpts{
			queue:  queue,
			jobSet: jobSet,
			state:  lookout.JobPending,
		}, converter, store)
		manyJobs(3, &createJobsOpts{
			queue:  queue,
			jobSet: jobSet,
			state:  lookout.JobRunning,
		}, converter, store)
		manyJobs(2, &createJobsOpts{
			queue:  queue,
			jobSet: jobSet,
			state:  lookout.JobFailed,
		}, converter, store)

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

		manyJobs(10, &createJobsOpts{
			queue:       queue,
			jobSet:      jobSet,
			state:       lookout.JobQueued,
			annotations: testAnnotations,
		}, converter, store)
		manyJobs(5, &createJobsOpts{
			queue:       queue,
			jobSet:      jobSet,
			state:       lookout.JobPending,
			annotations: testAnnotations,
		}, converter, store)
		manyJobs(3, &createJobsOpts{
			queue:       queue,
			jobSet:      jobSet,
			state:       lookout.JobRunning,
			annotations: testAnnotations,
		}, converter, store)
		manyJobs(2, &createJobsOpts{
			queue:       queue,
			jobSet:      jobSet,
			state:       lookout.JobFailed,
			annotations: testAnnotations,
		}, converter, store)

		manyJobs(11, &createJobsOpts{
			queue:  queue,
			jobSet: jobSet,
			state:  lookout.JobQueued,
		}, converter, store)
		manyJobs(6, &createJobsOpts{
			queue:  queue,
			jobSet: jobSet,
			state:  lookout.JobPending,
		}, converter, store)
		manyJobs(4, &createJobsOpts{
			queue:  queue,
			jobSet: jobSet,
			state:  lookout.JobRunning,
		}, converter, store)
		manyJobs(3, &createJobsOpts{
			queue:  queue,
			jobSet: jobSet,
			state:  lookout.JobFailed,
		}, converter, store)

		manyJobs(12, &createJobsOpts{
			queue:  "queue-2",
			jobSet: jobSet,
			state:  lookout.JobQueued,
		}, converter, store)
		manyJobs(7, &createJobsOpts{
			queue:  "queue-2",
			jobSet: jobSet,
			state:  lookout.JobPending,
		}, converter, store)
		manyJobs(5, &createJobsOpts{
			queue:  "queue-2",
			jobSet: jobSet,
			state:  lookout.JobRunning,
		}, converter, store)
		manyJobs(4, &createJobsOpts{
			queue:  "queue-2",
			jobSet: jobSet,
			state:  lookout.JobFailed,
		}, converter, store)

		manyJobs(12, &createJobsOpts{
			queue:  "queue-2",
			jobSet: jobSet,
			state:  lookout.JobQueued,
		}, converter, store)
		manyJobs(7, &createJobsOpts{
			queue:  "queue-2",
			jobSet: jobSet,
			state:  lookout.JobPending,
		}, converter, store)
		manyJobs(5, &createJobsOpts{
			queue:  "queue-2",
			jobSet: jobSet,
			state:  lookout.JobRunning,
		}, converter, store)
		manyJobs(4, &createJobsOpts{
			queue:  "queue-2",
			jobSet: jobSet,
			state:  lookout.JobFailed,
		}, converter, store)

		manyJobs(13, &createJobsOpts{
			queue:  queue,
			jobSet: "job-set-2",
			state:  lookout.JobQueued,
		}, converter, store)
		manyJobs(8, &createJobsOpts{
			queue:  queue,
			jobSet: "job-set-2",
			state:  lookout.JobPending,
		}, converter, store)
		manyJobs(6, &createJobsOpts{
			queue:  queue,
			jobSet: "job-set-2",
			state:  lookout.JobRunning,
		}, converter, store)
		manyJobs(5, &createJobsOpts{
			queue:  queue,
			jobSet: "job-set-2",
			state:  lookout.JobFailed,
		}, converter, store)

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

func TestGroupJobsWithMaxSubmittedTime(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		converter := instructions.NewInstructionConverter(metrics.Get(), userAnnotationPrefix, &compress.NoOpCompressor{})
		store := lookoutdb.NewLookoutDb(db, metrics.Get(), 3, 10)

		manyJobs(5, &createJobsOpts{
			queue:         queue,
			jobSet:        "job-set-1",
			submittedTime: pointer.Time(baseTime.Add(-2 * time.Minute)),
		}, converter, store)
		manyJobs(5, &createJobsOpts{
			queue:         queue,
			jobSet:        "job-set-1",
			submittedTime: pointer.Time(baseTime.Add(-1 * time.Minute)),
		}, converter, store)
		manyJobs(5, &createJobsOpts{
			queue:         queue,
			jobSet:        "job-set-1",
			submittedTime: pointer.Time(baseTime),
		}, converter, store)

		manyJobs(4, &createJobsOpts{
			queue:         queue,
			jobSet:        "job-set-2",
			submittedTime: pointer.Time(baseTime.Add(-6 * time.Minute)),
		}, converter, store)
		manyJobs(4, &createJobsOpts{
			queue:         queue,
			jobSet:        "job-set-2",
			submittedTime: pointer.Time(baseTime.Add(-5 * time.Minute)),
		}, converter, store)
		manyJobs(4, &createJobsOpts{
			queue:         queue,
			jobSet:        "job-set-2",
			submittedTime: pointer.Time(baseTime.Add(-4 * time.Minute)),
		}, converter, store)

		manyJobs(6, &createJobsOpts{
			queue:         queue,
			jobSet:        "job-set-3",
			submittedTime: pointer.Time(baseTime.Add(-9 * time.Minute)),
		}, converter, store)
		manyJobs(6, &createJobsOpts{
			queue:         queue,
			jobSet:        "job-set-3",
			submittedTime: pointer.Time(baseTime.Add(-8 * time.Minute)),
		}, converter, store)
		manyJobs(6, &createJobsOpts{
			queue:         queue,
			jobSet:        "job-set-3",
			submittedTime: pointer.Time(baseTime.Add(-7 * time.Minute)),
		}, converter, store)

		repo := NewSqlGroupJobsRepository(db)
		result, err := repo.GroupBy(
			context.TODO(),
			[]*model.Filter{},
			&model.Order{
				Field:     "submitted",
				Direction: "DESC",
			},
			"jobSet",
			[]string{"submitted"},
			0,
			10,
		)
		assert.NoError(t, err)
		assert.Len(t, result.Groups, 3)
		assert.Equal(t, 3, result.Count)
		assert.Equal(t, []*model.JobGroup{
			{
				Name:  "job-set-1",
				Count: 15,
				Aggregates: map[string]string{
					"submitted": baseTime.Format(time.RFC3339),
				},
			},
			{
				Name:  "job-set-2",
				Count: 12,
				Aggregates: map[string]string{
					"submitted": baseTime.Add(-4 * time.Minute).Format(time.RFC3339),
				},
			},
			{
				Name:  "job-set-3",
				Count: 18,
				Aggregates: map[string]string{
					"submitted": baseTime.Add(-7 * time.Minute).Format(time.RFC3339),
				},
			},
		}, result.Groups)
		return nil
	})
	assert.NoError(t, err)
}

func TestGroupJobsWithAvgLastTransitionTime(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		converter := instructions.NewInstructionConverter(metrics.Get(), userAnnotationPrefix, &compress.NoOpCompressor{})
		store := lookoutdb.NewLookoutDb(db, metrics.Get(), 3, 10)

		manyJobs(5, &createJobsOpts{
			queue:         "queue-1",
			jobSet:        "job-set-1",
			submittedTime: pointer.Time(baseTime.Add(-2 * time.Minute)),
		}, converter, store)
		manyJobs(5, &createJobsOpts{
			queue:         "queue-1",
			jobSet:        "job-set-2",
			submittedTime: pointer.Time(baseTime.Add(-1 * time.Minute)),
		}, converter, store)
		manyJobs(5, &createJobsOpts{
			queue:         "queue-1",
			jobSet:        "job-set-3",
			submittedTime: pointer.Time(baseTime),
		}, converter, store)

		manyJobs(4, &createJobsOpts{
			queue:         "queue-2",
			jobSet:        "job-set-1",
			submittedTime: pointer.Time(baseTime.Add(-6 * time.Minute)),
		}, converter, store)
		manyJobs(4, &createJobsOpts{
			queue:         "queue-2",
			jobSet:        "job-set-2",
			submittedTime: pointer.Time(baseTime.Add(-5 * time.Minute)),
		}, converter, store)
		manyJobs(4, &createJobsOpts{
			queue:         "queue-2",
			jobSet:        "job-set-3",
			submittedTime: pointer.Time(baseTime.Add(-4 * time.Minute)),
		}, converter, store)

		manyJobs(6, &createJobsOpts{
			queue:         "queue-3",
			jobSet:        "job-set-1",
			submittedTime: pointer.Time(baseTime.Add(-9 * time.Minute)),
		}, converter, store)
		manyJobs(6, &createJobsOpts{
			queue:         "queue-3",
			jobSet:        "job-set-2",
			submittedTime: pointer.Time(baseTime.Add(-8 * time.Minute)),
		}, converter, store)
		manyJobs(6, &createJobsOpts{
			queue:         "queue-3",
			jobSet:        "job-set-3",
			submittedTime: pointer.Time(baseTime.Add(-7 * time.Minute)),
		}, converter, store)

		repo := NewSqlGroupJobsRepository(db)
		result, err := repo.GroupBy(
			context.TODO(),
			[]*model.Filter{},
			&model.Order{
				Field:     "lastTransitionTime",
				Direction: "ASC",
			},
			"queue",
			[]string{"lastTransitionTime"},
			0,
			10,
		)
		assert.NoError(t, err)
		assert.Len(t, result.Groups, 3)
		assert.Equal(t, 3, result.Count)
		assert.Equal(t, []*model.JobGroup{
			{
				Name:  "queue-3",
				Count: 18,
				Aggregates: map[string]string{
					"lastTransitionTime": baseTime.Add(-8 * time.Minute).Format(time.RFC3339),
				},
			},
			{
				Name:  "queue-2",
				Count: 12,
				Aggregates: map[string]string{
					"lastTransitionTime": baseTime.Add(-5 * time.Minute).Format(time.RFC3339),
				},
			},
			{
				Name:  "queue-1",
				Count: 15,
				Aggregates: map[string]string{
					"lastTransitionTime": baseTime.Add(-1 * time.Minute).Format(time.RFC3339),
				},
			},
		}, result.Groups)
		return nil
	})
	assert.NoError(t, err)
}

func TestGroupJobsComplex(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		converter := instructions.NewInstructionConverter(metrics.Get(), userAnnotationPrefix, &compress.NoOpCompressor{})
		store := lookoutdb.NewLookoutDb(db, metrics.Get(), 3, 10)

		testAnnotations := map[string]string{
			"key-1": "val-1",
			"key-2": "val-23",
		}

		manyJobs(5, &createJobsOpts{
			queue:         queue,
			jobSet:        "job-set-1",
			state:         lookout.JobQueued,
			annotations:   testAnnotations,
			submittedTime: pointer.Time(baseTime),
		}, converter, store)
		manyJobs(5, &createJobsOpts{
			queue:              queue,
			jobSet:             "job-set-1",
			state:              lookout.JobPending,
			annotations:        testAnnotations,
			submittedTime:      pointer.Time(baseTime.Add(1 * time.Minute)),
			lastTransitionTime: pointer.Time(baseTime.Add(10 * time.Minute)),
		}, converter, store)
		manyJobs(5, &createJobsOpts{
			queue:              queue,
			jobSet:             "job-set-1",
			state:              lookout.JobRunning,
			annotations:        testAnnotations,
			submittedTime:      pointer.Time(baseTime.Add(3 * time.Minute)),
			lastTransitionTime: pointer.Time(baseTime.Add(5 * time.Minute)),
		}, converter, store)
		manyJobs(2, &createJobsOpts{
			queue:              queue,
			jobSet:             "job-set-2",
			state:              lookout.JobPending,
			annotations:        testAnnotations,
			submittedTime:      pointer.Time(baseTime.Add(20 * time.Minute)),
			lastTransitionTime: pointer.Time(baseTime.Add(50 * time.Minute)),
		}, converter, store)

		manyJobs(11, &createJobsOpts{
			queue:  queue,
			jobSet: jobSet,
			state:  lookout.JobQueued,
		}, converter, store)

		manyJobs(7, &createJobsOpts{
			queue:  "queue-2",
			jobSet: jobSet,
			state:  lookout.JobPending,
		}, converter, store)

		manyJobs(5, &createJobsOpts{
			queue:  "queue-2",
			jobSet: jobSet,
			state:  lookout.JobRunning,
		}, converter, store)

		manyJobs(5, &createJobsOpts{
			queue:  queue,
			jobSet: "job-set-2",
			state:  lookout.JobFailed,
		}, converter, store)

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
					Field: "state",
					Match: model.MatchAnyOf,
					Value: []string{
						string(lookout.JobQueued),
						string(lookout.JobPending),
						string(lookout.JobRunning),
					},
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
				Field:     "lastTransitionTime",
				Direction: "DESC",
			},
			"jobSet",
			[]string{
				"submitted",
				"lastTransitionTime",
			},
			0,
			10,
		)
		assert.NoError(t, err)
		assert.Len(t, result.Groups, 2)
		assert.Equal(t, 2, result.Count)
		assert.Equal(t, result.Groups, []*model.JobGroup{
			{
				Name:  "job-set-2",
				Count: 2,
				Aggregates: map[string]string{
					"submitted":          baseTime.Add(20 * time.Minute).Format(time.RFC3339),
					"lastTransitionTime": baseTime.Add(50 * time.Minute).Format(time.RFC3339),
				},
			},
			{
				Name:  "job-set-1",
				Count: 15,
				Aggregates: map[string]string{
					"submitted":          baseTime.Add(3 * time.Minute).Format(time.RFC3339),
					"lastTransitionTime": baseTime.Add(5 * time.Minute).Format(time.RFC3339),
				},
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
			manyJobs(i+1, &createJobsOpts{
				queue:  fmt.Sprintf("queue-%d", i+1),
				jobSet: jobSet,
				state:  lookout.JobQueued,
			}, converter, store)
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

type createJobsOpts struct {
	queue              string
	jobSet             string
	state              lookout.JobState
	annotations        map[string]string
	submittedTime      *time.Time
	lastTransitionTime *time.Time
}

type createJobsFn func(opts *createJobsOpts, converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb)

func manyJobs(n int, opts *createJobsOpts, converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb) {
	fn := getCreateJobsFn(opts.state)
	for i := 0; i < n; i++ {
		fn(opts, converter, store)
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

func makeQueued(opts *createJobsOpts, converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb) {
	tSubmit := baseTime
	if opts.submittedTime != nil {
		tSubmit = *opts.submittedTime
	}
	if opts.lastTransitionTime != nil {
		tSubmit = *opts.lastTransitionTime
	}
	NewJobSimulator(converter, store).
		Submit(opts.queue, opts.jobSet, owner, tSubmit, &JobOptions{
			Annotations: opts.annotations,
		}).
		Build()
}

func makePending(opts *createJobsOpts, converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb) {
	tSubmit := baseTime
	if opts.submittedTime != nil {
		tSubmit = *opts.submittedTime
	}
	lastTransitionTime := baseTime
	if opts.lastTransitionTime != nil {
		lastTransitionTime = *opts.lastTransitionTime
	}
	NewJobSimulator(converter, store).
		Submit(opts.queue, opts.jobSet, owner, tSubmit, &JobOptions{
			Annotations: opts.annotations,
		}).
		Pending(uuid.NewString(), cluster, lastTransitionTime).
		Build()
}

func makeRunning(opts *createJobsOpts, converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb) {
	tSubmit := baseTime
	if opts.submittedTime != nil {
		tSubmit = *opts.submittedTime
	}
	lastTransitionTime := baseTime
	if opts.lastTransitionTime != nil {
		lastTransitionTime = *opts.lastTransitionTime
	}
	runId := uuid.NewString()
	NewJobSimulator(converter, store).
		Submit(opts.queue, opts.jobSet, owner, tSubmit, &JobOptions{
			Annotations: opts.annotations,
		}).
		Pending(runId, cluster, lastTransitionTime.Add(-1*time.Minute)).
		Running(runId, cluster, lastTransitionTime).
		Build()
}

func makeFailed(opts *createJobsOpts, converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb) {
	tSubmit := baseTime
	if opts.submittedTime != nil {
		tSubmit = *opts.submittedTime
	}
	lastTransitionTime := baseTime
	if opts.lastTransitionTime != nil {
		lastTransitionTime = *opts.lastTransitionTime
	}
	runId := uuid.NewString()
	NewJobSimulator(converter, store).
		Submit(opts.queue, opts.jobSet, owner, tSubmit, &JobOptions{
			Annotations: opts.annotations,
		}).
		Pending(runId, cluster, lastTransitionTime.Add(-2*time.Minute)).
		Running(runId, cluster, lastTransitionTime.Add(-1*time.Minute)).
		RunFailed(runId, node, 1, "error", lastTransitionTime).
		Failed(node, 1, "error", lastTransitionTime).
		Build()
}
