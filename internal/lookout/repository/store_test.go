package repository

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/doug-martin/goqu/v9"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/common/database"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/lookout/testutil"
	"github.com/armadaproject/armada/pkg/api"
)

const userAnnotationPrefix = "test_prefix/"

func Test_RecordRunEvents(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)

		jobId := util.NewULID()

		err := jobStore.RecordJobPending(&api.JobPendingEvent{
			JobId:        jobId,
			Queue:        queue,
			Created:      time.Now(),
			KubernetesId: k8sId1,
		})
		assert.NoError(t, err)

		err = jobStore.RecordJobRunning(&api.JobRunningEvent{
			JobId:        jobId,
			Queue:        queue,
			Created:      time.Now(),
			KubernetesId: k8sId1,
		})
		assert.NoError(t, err)

		err = jobStore.RecordJobFailed(&api.JobFailedEvent{
			JobId:        jobId,
			Queue:        queue,
			Created:      time.Now(),
			KubernetesId: k8sId1,
		})
		assert.NoError(t, err)

		assert.Equal(t, 1, selectInt(t, db,
			"SELECT count(*) FROM job_run WHERE created IS NOT NULL AND started IS NOT NULL AND finished IS NOT NULL"))
	})
}

func Test_RunContainers(t *testing.T) {
	t.Run("no exit codes", func(t *testing.T) {
		withDatabase(t, func(db *goqu.Database) {
			jobStore := NewSQLJobStore(db, userAnnotationPrefix)

			err := jobStore.RecordJobFailed(&api.JobFailedEvent{
				JobId:        "job-1",
				Queue:        queue,
				Created:      time.Now(),
				KubernetesId: "a1",
			})
			assert.NoError(t, err)

			assert.Equal(t, 0, selectInt(t, db,
				"SELECT COUNT(*) FROM job_run_container"))
		})
	})

	t.Run("multiple containers", func(t *testing.T) {
		withDatabase(t, func(db *goqu.Database) {
			jobStore := NewSQLJobStore(db, userAnnotationPrefix)

			err := jobStore.RecordJobFailed(&api.JobFailedEvent{
				JobId:        "job-1",
				Queue:        queue,
				Created:      time.Now(),
				ExitCodes:    map[string]int32{"container-1": -1},
				KubernetesId: "a1",
			})
			assert.NoError(t, err)

			err = jobStore.RecordJobFailed(&api.JobFailedEvent{
				JobId:        "job-1",
				Queue:        queue,
				Created:      time.Now(),
				ExitCodes:    map[string]int32{"container-1": 2, "container-2": 3},
				KubernetesId: "a1",
			})
			assert.NoError(t, err)

			assert.Equal(t, 3, selectInt(t, db,
				"SELECT exit_code FROM job_run_container WHERE run_id = 'a1' AND container_name = 'container-2'"))
			assert.Equal(t, 2, selectInt(t, db,
				"SELECT exit_code FROM job_run_container WHERE run_id = 'a1' AND container_name = 'container-1'"))
		})
	})
}

func Test_RecordNullNodeIfEmptyString(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)

		err := jobStore.RecordJobRunning(&api.JobRunningEvent{
			JobId:        "job-1",
			Queue:        queue,
			Created:      time.Now(),
			KubernetesId: "a1",
		})
		assert.NoError(t, err)

		err = jobStore.RecordJobFailed(&api.JobFailedEvent{
			JobId:        "job-1",
			Queue:        queue,
			Created:      time.Now(),
			KubernetesId: "a2",
		})
		assert.NoError(t, err)

		err = jobStore.RecordJobSucceeded(&api.JobSucceededEvent{
			JobId:        "job-1",
			Queue:        queue,
			Created:      time.Now(),
			KubernetesId: "a3",
		})
		assert.NoError(t, err)

		err = jobStore.RecordJobUnableToSchedule(&api.JobUnableToScheduleEvent{
			JobId:        "job-1",
			Queue:        queue,
			Created:      time.Now(),
			KubernetesId: "a4",
		})
		assert.NoError(t, err)

		assert.False(t, selectNullString(t, db,
			"SELECT node FROM job_run WHERE run_id = 'a1'").Valid)
		assert.False(t, selectNullString(t, db,
			"SELECT node FROM job_run WHERE run_id = 'a2'").Valid)
		assert.False(t, selectNullString(t, db,
			"SELECT node FROM job_run WHERE run_id = 'a3'").Valid)
		assert.False(t, selectNullString(t, db,
			"SELECT node FROM job_run WHERE run_id = 'a4'").Valid)
	})
}

func Test_RecordLongError(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)

		err := jobStore.RecordJobFailed(&api.JobFailedEvent{
			JobId:        "job-1",
			Queue:        queue,
			Created:      time.Now(),
			Reason:       strings.Repeat("long error test ", 1000),
			KubernetesId: util.NewULID(),
		})
		assert.NoError(t, err)
	})
}

func Test_RecordAnnotations(t *testing.T) {
	t.Run("no annotations", func(t *testing.T) {
		withDatabase(t, func(db *goqu.Database) {
			jobStore := NewSQLJobStore(db, userAnnotationPrefix)

			err := jobStore.RecordJob(&api.Job{
				Id:      util.NewULID(),
				Queue:   queue,
				Created: someTime,
			}, someTime)
			assert.NoError(t, err)
			assert.Equal(t, 0, selectInt(t, db,
				"SELECT COUNT(*) FROM user_annotation_lookup"))
		})
	})

	t.Run("some annotations with correct prefix", func(t *testing.T) {
		withDatabase(t, func(db *goqu.Database) {
			jobStore := NewSQLJobStore(db, "prefix/")

			err := jobStore.RecordJob(&api.Job{
				Id:      util.NewULID(),
				Queue:   queue,
				Created: someTime,
				Annotations: map[string]string{
					"prefix/first": "some",
					"second":       "value",
					"prefix/third": "other",
					"prefix/":      "nothing",
				},
			}, someTime.Add(time.Minute))
			assert.NoError(t, err)
			assert.Equal(t, 2, selectInt(t, db,
				"SELECT COUNT(*) FROM user_annotation_lookup"))
		})
	})
}

func Test_EmptyRunId(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)

		err := jobStore.RecordJobFailed(&api.JobFailedEvent{
			JobId:        "job-1",
			Queue:        queue,
			Created:      time.Now(),
			KubernetesId: "",
		})
		assert.NoError(t, err)

		err = jobStore.RecordJobFailed(&api.JobFailedEvent{
			JobId:        "job-2",
			Queue:        queue,
			Created:      time.Now(),
			KubernetesId: "",
		})
		assert.NoError(t, err)

		assert.Equal(t, JobStateToIntMap[JobFailed], selectInt(t, db,
			"SELECT state FROM job WHERE job.job_id = 'job-1'"))
		assert.Equal(t, JobStateToIntMap[JobFailed], selectInt(t, db,
			"SELECT state FROM job WHERE job.job_id = 'job-2'"))
	})
}

func Test_UnableToSchedule(t *testing.T) {
	t.Run("single job", func(t *testing.T) {
		withDatabase(t, func(db *goqu.Database) {
			jobStore := NewSQLJobStore(db, userAnnotationPrefix)

			err := jobStore.RecordJobUnableToSchedule(&api.JobUnableToScheduleEvent{
				JobId:        util.NewULID(),
				Queue:        queue,
				Created:      time.Now(),
				KubernetesId: util.NewULID(),
			})
			assert.NoError(t, err)

			assert.Equal(t, 1, selectInt(t, db,
				"SELECT count(*) FROM job_run WHERE job_run.unable_to_schedule = TRUE"))
			assert.Equal(t, 1, selectInt(t, db,
				"SELECT count(*) FROM job_run WHERE job_run.finished IS NOT NULL"))
		})
	})

	t.Run("null character in error message", func(t *testing.T) {
		withDatabase(t, func(db *goqu.Database) {
			jobStore := NewSQLJobStore(db, userAnnotationPrefix)
			jobId := util.NewULID()

			expectedError := "Some error  please"
			err := jobStore.RecordJobUnableToSchedule(&api.JobUnableToScheduleEvent{
				JobId:        jobId,
				Queue:        "queue",
				Created:      someTime,
				KubernetesId: k8sId1,
				PodNumber:    0,
				Reason:       "Some error \000 please",
			})
			assert.NoError(t, err)

			nullString := selectNullString(t, db, "SELECT error FROM job_run")
			assert.True(t, nullString.Valid)
			assert.Equal(t, expectedError, nullString.String)
		})
	})
}

func Test_Queued(t *testing.T) {
	t.Run("queued", func(t *testing.T) {
		withDatabase(t, func(db *goqu.Database) {
			jobStore := NewSQLJobStore(db, userAnnotationPrefix)

			err := jobStore.RecordJob(&api.Job{
				Id:      util.NewULID(),
				Queue:   queue,
				Created: someTime,
			}, someTime)
			assert.NoError(t, err)

			assert.Equal(t, JobStateToIntMap[JobQueued], selectInt(t, db,
				"SELECT state FROM job"))
		})
	})

	t.Run("queued after pending", func(t *testing.T) {
		withDatabase(t, func(db *goqu.Database) {
			jobStore := NewSQLJobStore(db, userAnnotationPrefix)
			jobId := util.NewULID()

			err := jobStore.RecordJobPending(&api.JobPendingEvent{
				JobId:        jobId,
				Queue:        queue,
				Created:      someTime.Add(1 * time.Minute),
				KubernetesId: k8sId1,
			})
			assert.NoError(t, err)

			err = jobStore.RecordJob(&api.Job{
				Id:      jobId,
				Queue:   "queue",
				Created: someTime,
			}, someTime)
			assert.NoError(t, err)

			assert.Equal(t, JobStateToIntMap[JobPending], selectInt(t, db,
				"SELECT state FROM job"))
		})
	})

	t.Run("null character in pod spec", func(t *testing.T) {
		withDatabase(t, func(db *goqu.Database) {
			jobStore := NewSQLJobStore(db, userAnnotationPrefix)
			jobId := util.NewULID()

			err := jobStore.RecordJob(&api.Job{
				Id:      jobId,
				Queue:   "queue",
				Created: someTime,
				PodSpecs: []*v1.PodSpec{
					{
						Containers: []v1.Container{
							{
								Name:    "test-container",
								Image:   "test-image",
								Command: []string{"/bin/bash"},
								Args:    []string{"echo", "hello", "\000"},
							},
						},
					},
				},
			}, someTime)
			assert.NoError(t, err)
		})
	})
}

func Test_Pending(t *testing.T) {
	t.Run("single node job, pending", func(t *testing.T) {
		withDatabase(t, func(db *goqu.Database) {
			jobStore := NewSQLJobStore(db, userAnnotationPrefix)
			jobId := util.NewULID()

			err := jobStore.RecordJobPending(&api.JobPendingEvent{
				JobId:        jobId,
				Queue:        queue,
				Created:      someTime,
				KubernetesId: k8sId1,
			})
			assert.NoError(t, err)

			assert.Equal(t, JobStateToIntMap[JobPending], selectInt(t, db,
				"SELECT state FROM job"))
		})
	})

	t.Run("single node job, pending after running, same pod", func(t *testing.T) {
		withDatabase(t, func(db *goqu.Database) {
			jobStore := NewSQLJobStore(db, userAnnotationPrefix)
			jobId := util.NewULID()

			err := jobStore.RecordJobRunning(&api.JobRunningEvent{
				JobId:        jobId,
				Queue:        queue,
				Created:      someTime.Add(5 * time.Minute),
				KubernetesId: k8sId1,
			})
			assert.NoError(t, err)

			err = jobStore.RecordJobPending(&api.JobPendingEvent{
				JobId:        jobId,
				Queue:        queue,
				Created:      someTime,
				KubernetesId: k8sId1,
			})
			assert.NoError(t, err)

			assert.Equal(t, JobStateToIntMap[JobRunning], selectInt(t, db,
				"SELECT state FROM job"))
		})
	})

	t.Run("single node job, pending after running, different pod", func(t *testing.T) {
		withDatabase(t, func(db *goqu.Database) {
			jobStore := NewSQLJobStore(db, userAnnotationPrefix)
			jobId := util.NewULID()

			err := jobStore.RecordJobRunning(&api.JobRunningEvent{
				JobId:        jobId,
				Queue:        queue,
				Created:      someTime,
				KubernetesId: k8sId1,
			})
			assert.NoError(t, err)

			err = jobStore.RecordJobPending(&api.JobPendingEvent{
				JobId:        jobId,
				Queue:        queue,
				Created:      someTime.Add(5 * time.Minute),
				KubernetesId: k8sId2,
			})
			assert.NoError(t, err)

			assert.Equal(t, JobStateToIntMap[JobPending], selectInt(t, db,
				"SELECT state FROM job"))
		})
	})

	t.Run("multi node job, pending after running", func(t *testing.T) {
		withDatabase(t, func(db *goqu.Database) {
			jobStore := NewSQLJobStore(db, userAnnotationPrefix)
			jobId := util.NewULID()

			err := jobStore.RecordJobRunning(&api.JobRunningEvent{
				JobId:        jobId,
				Queue:        queue,
				Created:      someTime.Add(3 * time.Minute),
				KubernetesId: k8sId2,
				PodNumber:    1,
			})
			assert.NoError(t, err)

			err = jobStore.RecordJobPending(&api.JobPendingEvent{
				JobId:        jobId,
				Queue:        queue,
				Created:      someTime,
				KubernetesId: k8sId1,
				PodNumber:    0,
			})
			assert.NoError(t, err)

			assert.Equal(t, JobStateToIntMap[JobPending], selectInt(t, db,
				"SELECT state FROM job"))
		})
	})
}

func Test_Running(t *testing.T) {
	t.Run("single node job, running", func(t *testing.T) {
		withDatabase(t, func(db *goqu.Database) {
			jobStore := NewSQLJobStore(db, userAnnotationPrefix)

			err := jobStore.RecordJobRunning(&api.JobRunningEvent{
				JobId:        util.NewULID(),
				Queue:        queue,
				Created:      someTime.Add(3 * time.Minute),
				KubernetesId: k8sId2,
			})
			assert.NoError(t, err)

			assert.Equal(t, JobStateToIntMap[JobRunning], selectInt(t, db,
				"SELECT state FROM job"))
		})
	})

	t.Run("single node job, running after pending in separate pod", func(t *testing.T) {
		withDatabase(t, func(db *goqu.Database) {
			jobStore := NewSQLJobStore(db, userAnnotationPrefix)
			jobId := util.NewULID()

			err := jobStore.RecordJobPending(&api.JobPendingEvent{
				JobId:        jobId,
				Queue:        queue,
				Created:      someTime.Add(3 * time.Minute),
				KubernetesId: k8sId2,
			})
			assert.NoError(t, err)

			err = jobStore.RecordJobRunning(&api.JobRunningEvent{
				JobId:        jobId,
				Queue:        queue,
				Created:      someTime,
				KubernetesId: k8sId1,
			})
			assert.NoError(t, err)

			assert.Equal(t, JobStateToIntMap[JobPending], selectInt(t, db,
				"SELECT state FROM job"))
		})
	})

	t.Run("single node job, running after success", func(t *testing.T) {
		withDatabase(t, func(db *goqu.Database) {
			jobStore := NewSQLJobStore(db, userAnnotationPrefix)
			jobId := util.NewULID()

			err := jobStore.RecordJobSucceeded(&api.JobSucceededEvent{
				JobId:        jobId,
				Queue:        queue,
				Created:      someTime.Add(3 * time.Minute),
				KubernetesId: k8sId1,
			})
			assert.NoError(t, err)

			err = jobStore.RecordJobRunning(&api.JobRunningEvent{
				JobId:        jobId,
				Queue:        queue,
				Created:      someTime,
				KubernetesId: k8sId1,
			})
			assert.NoError(t, err)

			assert.Equal(t, JobStateToIntMap[JobSucceeded], selectInt(t, db,
				"SELECT state FROM job"))
		})
	})

	t.Run("multi node job, running after success", func(t *testing.T) {
		withDatabase(t, func(db *goqu.Database) {
			jobStore := NewSQLJobStore(db, userAnnotationPrefix)
			jobId := util.NewULID()

			err := jobStore.RecordJobSucceeded(&api.JobSucceededEvent{
				JobId:        jobId,
				Queue:        queue,
				Created:      someTime.Add(3 * time.Minute),
				KubernetesId: k8sId1,
				PodNumber:    0,
			})
			assert.NoError(t, err)

			err = jobStore.RecordJobRunning(&api.JobRunningEvent{
				JobId:        jobId,
				Queue:        queue,
				Created:      someTime,
				KubernetesId: k8sId2,
				PodNumber:    1,
			})
			assert.NoError(t, err)

			assert.Equal(t, JobStateToIntMap[JobRunning], selectInt(t, db,
				"SELECT state FROM job"))
		})
	})
}

func Test_Succeeded(t *testing.T) {
	t.Run("multi node job, not all successes", func(t *testing.T) {
		withDatabase(t, func(db *goqu.Database) {
			jobStore := NewSQLJobStore(db, userAnnotationPrefix)
			jobId := util.NewULID()

			err := jobStore.RecordJobSucceeded(&api.JobSucceededEvent{
				JobId:        jobId,
				Queue:        queue,
				Created:      someTime,
				KubernetesId: k8sId1,
				PodNumber:    0,
			})
			assert.NoError(t, err)

			err = jobStore.RecordJobSucceeded(&api.JobSucceededEvent{
				JobId:        jobId,
				Queue:        queue,
				Created:      someTime,
				KubernetesId: k8sId2,
				PodNumber:    1,
			})
			assert.NoError(t, err)

			err = jobStore.RecordJobPending(&api.JobPendingEvent{
				JobId:        jobId,
				Queue:        queue,
				Created:      someTime,
				KubernetesId: k8sId3,
				PodNumber:    2,
			})
			assert.NoError(t, err)

			assert.Equal(t, JobStateToIntMap[JobPending], selectInt(t, db,
				"SELECT state FROM job"))
		})
	})

	t.Run("multi node job, all successes", func(t *testing.T) {
		withDatabase(t, func(db *goqu.Database) {
			jobStore := NewSQLJobStore(db, userAnnotationPrefix)
			jobId := util.NewULID()

			err := jobStore.RecordJobSucceeded(&api.JobSucceededEvent{
				JobId:        jobId,
				Queue:        queue,
				Created:      someTime,
				KubernetesId: k8sId1,
				PodNumber:    0,
			})
			assert.NoError(t, err)

			err = jobStore.RecordJobSucceeded(&api.JobSucceededEvent{
				JobId:        jobId,
				Queue:        queue,
				Created:      someTime,
				KubernetesId: k8sId2,
				PodNumber:    1,
			})
			assert.NoError(t, err)

			err = jobStore.RecordJobSucceeded(&api.JobSucceededEvent{
				JobId:        jobId,
				Queue:        queue,
				Created:      someTime,
				KubernetesId: k8sId3,
				PodNumber:    2,
			})
			assert.NoError(t, err)

			assert.Equal(t, JobStateToIntMap[JobSucceeded], selectInt(t, db,
				"SELECT state FROM job"))
		})
	})
}

func Test_Failed(t *testing.T) {
	t.Run("multi-node job", func(t *testing.T) {
		withDatabase(t, func(db *goqu.Database) {
			jobStore := NewSQLJobStore(db, userAnnotationPrefix)
			jobId := util.NewULID()

			err := jobStore.RecordJobFailed(&api.JobFailedEvent{
				JobId:        jobId,
				Queue:        "queue",
				Created:      someTime,
				KubernetesId: k8sId1,
				PodNumber:    0,
			})
			assert.NoError(t, err)

			err = jobStore.RecordJobSucceeded(&api.JobSucceededEvent{
				JobId:        jobId,
				Queue:        "queue",
				Created:      someTime,
				KubernetesId: k8sId2,
				PodNumber:    1,
			})
			assert.NoError(t, err)

			err = jobStore.RecordJobSucceeded(&api.JobSucceededEvent{
				JobId:        jobId,
				Queue:        "queue",
				Created:      someTime,
				KubernetesId: k8sId3,
				PodNumber:    2,
			})
			assert.NoError(t, err)

			assert.Equal(t, JobStateToIntMap[JobFailed], selectInt(t, db,
				"SELECT state FROM job"))
		})
	})

	t.Run("null character in error message", func(t *testing.T) {
		withDatabase(t, func(db *goqu.Database) {
			jobStore := NewSQLJobStore(db, userAnnotationPrefix)
			jobId := util.NewULID()

			expectedError := "Some error  please"
			err := jobStore.RecordJobFailed(&api.JobFailedEvent{
				JobId:        jobId,
				Queue:        "queue",
				Created:      someTime,
				KubernetesId: k8sId1,
				PodNumber:    0,
				Reason:       "Some error \000 please",
			})
			assert.NoError(t, err)

			nullString := selectNullString(t, db, "SELECT error FROM job_run")
			assert.True(t, nullString.Valid)
			assert.Equal(t, expectedError, nullString.String)
		})
	})
}

func Test_Cancelled(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)
		jobId := util.NewULID()

		err := jobStore.RecordJobPending(&api.JobPendingEvent{
			JobId:        jobId,
			Queue:        "queue",
			Created:      someTime,
			KubernetesId: k8sId1,
			PodNumber:    0,
		})
		assert.NoError(t, err)

		err = jobStore.RecordJobRunning(&api.JobRunningEvent{
			JobId:        jobId,
			Queue:        "queue",
			Created:      someTime,
			KubernetesId: k8sId2,
			PodNumber:    1,
		})
		assert.NoError(t, err)

		err = jobStore.MarkCancelled(&api.JobCancelledEvent{
			JobId:   jobId,
			Queue:   "queue",
			Created: someTime,
		})
		assert.NoError(t, err)

		assert.Equal(t, JobStateToIntMap[JobCancelled], selectInt(t, db,
			"SELECT state FROM job"))
	})
}

func Test_Duplicate(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)
		jobId := util.NewULID()

		err := jobStore.RecordJob(&api.Job{
			Id:      jobId,
			Queue:   "queue",
			Created: someTime,
		}, someTime)
		assert.NoError(t, err)

		err = jobStore.RecordJobDuplicate(&api.JobDuplicateFoundEvent{
			JobId:         jobId,
			Queue:         "queue",
			Created:       someTime,
			OriginalJobId: util.NewULID(),
		})
		assert.NoError(t, err)

		assert.Equal(t, JobStateToIntMap[JobDuplicate], selectInt(t, db,
			"SELECT state FROM job"))
	})
}

func Test_DuplicateOutOfOrder(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)
		jobId := util.NewULID()

		err := jobStore.RecordJobDuplicate(&api.JobDuplicateFoundEvent{
			JobId:         jobId,
			Queue:         "queue",
			Created:       someTime,
			OriginalJobId: util.NewULID(),
		})
		assert.NoError(t, err)

		err = jobStore.RecordJob(&api.Job{
			Id:      jobId,
			Queue:   "queue",
			Created: someTime,
		}, someTime)
		assert.NoError(t, err)

		assert.Equal(t, JobStateToIntMap[JobDuplicate], selectInt(t, db,
			"SELECT state FROM job"))
	})
}

func Test_JobTerminatedEvent(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)
		jobId := util.NewULID()

		err := jobStore.RecordJobRunning(&api.JobRunningEvent{
			JobId:        jobId,
			JobSetId:     "job-set",
			Queue:        "queue",
			Created:      someTime,
			KubernetesId: k8sId2,
			ClusterId:    "cluster1",
			PodNumber:    0,
		})
		assert.NoError(t, err)

		err = jobStore.RecordJobTerminated(&api.JobTerminatedEvent{
			JobId:        jobId,
			JobSetId:     "job-set",
			Queue:        "queue",
			Created:      someTime,
			ClusterId:    "cluster1",
			KubernetesId: k8sId2,
			PodNumber:    0,
			Reason:       "Termination reason",
		})
		assert.NoError(t, err)

		assert.True(t, ToUTC(someTime).Equal(selectTime(t, db, "SELECT finished from job_run")))
		assert.Equal(t, false, selectBoolean(t, db, "SELECT succeeded from job_run"))
	})
}

func Test_JobReprioritizedEvent(t *testing.T) {
	t.Run("after job created", func(t *testing.T) {
		withDatabase(t, func(db *goqu.Database) {
			jobStore := NewSQLJobStore(db, userAnnotationPrefix)
			jobId := util.NewULID()

			err := jobStore.RecordJob(&api.Job{
				Id:      jobId,
				Queue:   "queue",
				Created: someTime,
			}, someTime)
			assert.NoError(t, err)

			err = jobStore.RecordJobReprioritized(&api.JobReprioritizedEvent{
				JobId:       jobId,
				JobSetId:    "job-set",
				Queue:       "queue",
				Created:     someTime,
				NewPriority: 123,
			})
			assert.NoError(t, err)

			assert.Equal(t, float64(123), getPriority(t, db, jobId))

			assert.Equal(t, float64(123), selectDouble(t, db,
				"SELECT priority FROM job"))

			var job api.Job
			jobProto := database.ParseNullStringDefault(selectNullString(t, db, "SELECT orig_job_spec FROM job"))
			err = job.Unmarshal([]byte(jobProto))
			assert.NoError(t, err)
			assert.Equal(t, float64(123), job.Priority)
		})
	})

	t.Run("multiple events without job submission", func(t *testing.T) {
		withDatabase(t, func(db *goqu.Database) {
			jobStore := NewSQLJobStore(db, userAnnotationPrefix)
			jobId := util.NewULID()

			err := jobStore.RecordJobReprioritized(&api.JobReprioritizedEvent{
				JobId:       jobId,
				JobSetId:    "job-set",
				Queue:       "queue",
				Created:     someTime,
				NewPriority: 123,
			})
			assert.NoError(t, err)

			err = jobStore.RecordJobReprioritized(&api.JobReprioritizedEvent{
				JobId:       jobId,
				JobSetId:    "job-set",
				Queue:       "queue",
				Created:     someTime,
				NewPriority: 256,
			})
			assert.NoError(t, err)

			assert.Equal(t, float64(256), getPriority(t, db, jobId))

			assert.Equal(t, float64(256), selectDouble(t, db,
				"SELECT priority FROM job"))
			assert.True(t, selectNullString(t, db,
				"SELECT orig_job_spec FROM job").Valid)
		})
	})
}

func Test_JobUpdatedEvent(t *testing.T) {
	t.Run("job exists", func(t *testing.T) {
		newPriority := 123.0

		oldAnnotations := map[string]string{userAnnotationPrefix + "a": "b", userAnnotationPrefix + "1": "2"}
		newAnnotations := map[string]string{userAnnotationPrefix + "c": "d", userAnnotationPrefix + "1": "3"}

		withDatabase(t, func(db *goqu.Database) {
			jobStore := NewSQLJobStore(db, userAnnotationPrefix)
			jobId := util.NewULID()
			otherJobId := util.NewULID()

			err := jobStore.RecordJob(&api.Job{
				Id:          jobId,
				Queue:       "queue",
				Created:     someTime,
				Annotations: oldAnnotations,
			},
				someTime)
			assert.NoError(t, err)

			err = jobStore.RecordJob(&api.Job{
				Id:      otherJobId,
				Queue:   "queue",
				Created: someTime,
			},
				someTime.Add(time.Minute))
			assert.NoError(t, err)

			err = jobStore.RecordJob(
				&api.Job{
					Id:          jobId,
					Queue:       "queue",
					Created:     someTime,
					Annotations: newAnnotations,
					Priority:    newPriority,
				},
				someTime.Add(time.Minute))
			assert.NoError(t, err)

			assert.Equal(t, newPriority, getPriority(t, db, jobId))
			assert.Equal(t, 0.0, getPriority(t, db, otherJobId))
			assert.Equal(t, JobStateToIntMap[JobQueued], selectInt(t, db,
				fmt.Sprintf("SELECT state FROM job WHERE job_id = '%s'", jobId)))

			job := getJob(t, db, jobId)
			assert.Equal(t, newPriority, job.Priority)
			assert.Equal(t, newAnnotations, job.Annotations)

			assert.False(t, hasUserAnnotation(t, db, jobId, "a", "b"))
			assert.True(t, hasUserAnnotation(t, db, jobId, "c", "d"))
			assert.False(t, hasUserAnnotation(t, db, jobId, "1", "2"))
			assert.True(t, hasUserAnnotation(t, db, jobId, "1", "3"))

			otherJob := getJob(t, db, otherJobId)
			assert.Equal(t, 0.0, otherJob.Priority)
			assert.Nil(t, otherJob.Annotations)
		})
	})

	t.Run("existing job has later timestamp than event", func(t *testing.T) {
		oldPriority := 123.0
		oldAnnotations := map[string]string{userAnnotationPrefix + "a": "b", userAnnotationPrefix + "1": "2"}

		newPriority := 456.0
		newAnnotations := map[string]string{userAnnotationPrefix + "c": "d", userAnnotationPrefix + "1": "3"}

		withDatabase(t, func(db *goqu.Database) {
			jobStore := NewSQLJobStore(db, userAnnotationPrefix)
			jobId := util.NewULID()

			err := jobStore.RecordJob(
				&api.Job{
					Id:          jobId,
					Queue:       "queue",
					Created:     someTime,
					Annotations: oldAnnotations,
					Priority:    oldPriority,
				},
				someTime)
			assert.Nil(t, err)

			err = jobStore.RecordJob(
				&api.Job{
					Id:          jobId,
					Queue:       "queue",
					Created:     someTime,
					Annotations: newAnnotations,
					Priority:    newPriority,
				},
				someTime.Add(-time.Minute))
			assert.Nil(t, err)

			reloadedJob := getJob(t, db, jobId)
			assert.Equal(t, oldPriority, reloadedJob.Priority)
			assert.Equal(t, oldAnnotations, reloadedJob.Annotations)

			assert.Equal(t, oldPriority, getPriority(t, db, jobId))

			assert.True(t, hasUserAnnotation(t, db, jobId, "a", "b"))
			assert.False(t, hasUserAnnotation(t, db, jobId, "c", "d"))
			assert.True(t, hasUserAnnotation(t, db, jobId, "1", "2"))
			assert.False(t, hasUserAnnotation(t, db, jobId, "1", "3"))
		})
	})
}

func hasUserAnnotation(t *testing.T, db *goqu.Database, jobId string, key string, val string) bool {
	return selectInt(t, db, fmt.Sprintf("SELECT COUNT(1) FROM user_annotation_lookup WHERE job_id = '%s' AND key='%s' AND value='%s'", jobId, key, val)) > 0
}

func getPriority(t *testing.T, db *goqu.Database, jobId string) float64 {
	return selectDouble(t, db, fmt.Sprintf("SELECT priority FROM job WHERE job_id = '%s'", jobId))
}

func getJob(t *testing.T, db *goqu.Database, jobId string) *api.Job {
	var job api.Job
	jobProto := database.ParseNullStringDefault(selectNullString(t, db, fmt.Sprintf("SELECT orig_job_spec FROM job WHERE job_id = '%s'", jobId)))
	err := job.Unmarshal([]byte(jobProto))
	assert.Nil(t, err)
	return &job
}

func selectInt(t *testing.T, db *goqu.Database, query string) int {
	r, err := db.Query(query)
	assert.NoError(t, err)
	r.Next()
	var value int
	err = r.Scan(&value)
	assert.NoError(t, err)
	return value
}

func selectDouble(t *testing.T, db *goqu.Database, query string) float64 {
	r, err := db.Query(query)
	assert.NoError(t, err)
	r.Next()
	var value float64
	err = r.Scan(&value)
	assert.NoError(t, err)
	return value
}

func selectNullString(t *testing.T, db *goqu.Database, query string) sql.NullString {
	r, err := db.Query(query)
	assert.NoError(t, err)
	r.Next()
	var str sql.NullString
	err = r.Scan(&str)
	assert.NoError(t, err)
	return str
}

func selectTime(t *testing.T, db *goqu.Database, query string) time.Time {
	r, err := db.Query(query)
	assert.NoError(t, err)
	r.Next()
	var value time.Time
	err = r.Scan(&value)
	assert.NoError(t, err)
	return value
}

func selectBoolean(t *testing.T, db *goqu.Database, query string) bool {
	r, err := db.Query(query)
	assert.NoError(t, err)
	r.Next()
	var value bool
	err = r.Scan(&value)
	assert.NoError(t, err)
	return value
}

func withDatabase(t *testing.T, action func(*goqu.Database)) {
	err := testutil.WithDatabase(func(testDb *sql.DB) error {
		action(goqu.New("postgres", testDb))
		return nil
	})
	assert.NoError(t, err)
}
