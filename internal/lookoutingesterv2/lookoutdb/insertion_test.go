package lookoutdb

import (
	"fmt"
	"regexp"
	"sort"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"k8s.io/utils/pointer"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/database/lookout"
	"github.com/armadaproject/armada/internal/common/ingest/testfixtures"
	"github.com/armadaproject/armada/internal/common/pulsarutils"
	"github.com/armadaproject/armada/internal/lookoutingesterv2/metrics"
	"github.com/armadaproject/armada/internal/lookoutingesterv2/model"
)

const (
	jobIdString      = "01f3j0g1md4qx7z5qb148qnh4r"
	runIdString      = "123e4567-e89b-12d3-a456-426614174000"
	jobSetName       = "testJobset"
	cpu              = 12500
	memory           = 2000 * 1024 * 1024 * 1024
	ephemeralStorage = 3000 * 1024 * 1024 * 1024
	gpu              = 8
	executorId       = "testCluster"
	nodeName         = "testNode"
	queue            = "test-queue"
	userId           = "testUser"
	priority         = 3
	updatePriority   = 4
	priorityClass    = "default"
	jobProto         = "hello world"
)

var m = metrics.Get()

var annotations = map[string]string{
	"a": "0",
	"b": "1",
}

var (
	baseTime, _     = time.Parse("2006-01-02T15:04:05.000Z", "2022-03-01T15:04:05.000Z")
	updateTime, _   = time.Parse("2006-01-02T15:04:05.000Z", "2022-03-01T15:04:06.000Z")
	startTime, _    = time.Parse("2006-01-02T15:04:05.000Z", "2022-03-01T15:04:07.000Z")
	finishedTime, _ = time.Parse("2006-01-02T15:04:05.000Z", "2022-03-01T15:04:08.000Z")
	fatalErrors     = []*regexp.Regexp{regexp.MustCompile("SQLSTATE 22001")}
)

// An invalid job id that exceeds the varchar count
var invalidId = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

type JobRow struct {
	JobId                     string
	Queue                     string
	Owner                     string
	JobSet                    string
	Cpu                       int64
	Memory                    int64
	EphemeralStorage          int64
	Gpu                       int64
	Priority                  uint32
	Submitted                 time.Time
	Cancelled                 *time.Time
	State                     int32
	LastTransitionTime        time.Time
	LastTransitionTimeSeconds int64
	JobProto                  []byte
	Duplicate                 bool
	PriorityClass             string
	LatestRunId               *string
	CancelReason              *string
	Annotations               map[string]string
}

type JobRunRow struct {
	RunId       string
	JobId       string
	Cluster     string
	Node        *string
	Pending     time.Time
	Started     *time.Time
	Finished    *time.Time
	JobRunState int32
	Error       []byte
	Debug       []byte
	ExitCode    *int32
}

type JobErrorRow struct {
	JobId string
	Error []byte
}

func defaultInstructionSet() *model.InstructionSet {
	return &model.InstructionSet{
		JobsToCreate: []*model.CreateJobInstruction{makeCreateJobInstruction(jobIdString)},
		JobsToUpdate: []*model.UpdateJobInstruction{{
			JobId:                     jobIdString,
			Priority:                  pointer.Int64(updatePriority),
			State:                     pointer.Int32(lookout.JobFailedOrdinal),
			LastTransitionTime:        &updateTime,
			LastTransitionTimeSeconds: pointer.Int64(updateTime.Unix()),
		}},
		JobRunsToCreate: []*model.CreateJobRunInstruction{{
			RunId:       runIdString,
			JobId:       jobIdString,
			Cluster:     executorId,
			Leased:      &updateTime,
			Pending:     &updateTime,
			JobRunState: lookout.JobRunPendingOrdinal,
		}},
		JobRunsToUpdate: []*model.UpdateJobRunInstruction{{
			RunId:       runIdString,
			Node:        pointer.String(nodeName),
			Started:     &startTime,
			Finished:    &finishedTime,
			Debug:       []byte(testfixtures.DebugMsg),
			JobRunState: pointer.Int32(lookout.JobRunSucceededOrdinal),
			ExitCode:    pointer.Int32(0),
		}},
		JobErrorsToCreate: []*model.CreateJobErrorInstruction{{
			JobId: jobIdString,
			Error: []byte(testfixtures.ErrMsg),
		}},
		MessageIds: []pulsar.MessageID{pulsarutils.NewMessageId(3)},
	}
}

var expectedJobAfterSubmit = JobRow{
	JobId:                     jobIdString,
	Queue:                     queue,
	Owner:                     userId,
	JobSet:                    jobSetName,
	Cpu:                       cpu,
	Memory:                    memory,
	EphemeralStorage:          ephemeralStorage,
	Gpu:                       gpu,
	Priority:                  priority,
	Submitted:                 baseTime,
	State:                     lookout.JobQueuedOrdinal,
	LastTransitionTime:        baseTime,
	LastTransitionTimeSeconds: baseTime.Unix(),
	JobProto:                  []byte(jobProto),
	Duplicate:                 false,
	PriorityClass:             priorityClass,
	Annotations:               annotations,
}

var expectedJobAfterUpdate = JobRow{
	JobId:                     jobIdString,
	Queue:                     queue,
	Owner:                     userId,
	JobSet:                    jobSetName,
	Cpu:                       cpu,
	Memory:                    memory,
	EphemeralStorage:          ephemeralStorage,
	Gpu:                       gpu,
	Priority:                  updatePriority,
	Submitted:                 baseTime,
	State:                     lookout.JobFailedOrdinal,
	LastTransitionTime:        updateTime,
	LastTransitionTimeSeconds: updateTime.Unix(),
	JobProto:                  []byte(jobProto),
	Duplicate:                 false,
	PriorityClass:             priorityClass,
	Annotations:               annotations,
}

var expectedJobRun = JobRunRow{
	RunId:       runIdString,
	JobId:       jobIdString,
	Cluster:     executorId,
	Pending:     updateTime,
	JobRunState: lookout.JobRunPendingOrdinal,
}

var expectedJobError = JobErrorRow{
	JobId: jobIdString,
	Error: []byte(testfixtures.ErrMsg),
}

var expectedJobRunAfterUpdate = JobRunRow{
	RunId:       runIdString,
	JobId:       jobIdString,
	Cluster:     executorId,
	Node:        pointer.String(nodeName),
	Pending:     updateTime,
	Started:     &startTime,
	Finished:    &finishedTime,
	JobRunState: lookout.JobRunSucceededOrdinal,
	ExitCode:    pointer.Int32(0),
	Debug:       []byte(testfixtures.DebugMsg),
}

func TestCreateJobsBatch(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		ldb := NewLookoutDb(db, fatalErrors, m, 10)
		// Insert
		err := ldb.CreateJobsBatch(armadacontext.Background(), defaultInstructionSet().JobsToCreate)
		assert.Nil(t, err)
		job := getJob(t, db, jobIdString)
		assert.Equal(t, expectedJobAfterSubmit, job)

		// Insert again and test that it's idempotent
		err = ldb.CreateJobsBatch(armadacontext.Background(), defaultInstructionSet().JobsToCreate)
		assert.Nil(t, err)
		job = getJob(t, db, jobIdString)
		assert.Equal(t, expectedJobAfterSubmit, job)

		// If a row is bad then we should return an error and no updates should happen
		_, err = db.Exec(armadacontext.Background(), "DELETE FROM job")
		assert.NoError(t, err)
		invalidJob := &model.CreateJobInstruction{
			JobId: invalidId,
		}
		err = ldb.CreateJobsBatch(armadacontext.Background(), append(defaultInstructionSet().JobsToCreate, invalidJob))
		assert.Error(t, err)
		assertNoRows(t, db, "job")
		return nil
	})
	assert.NoError(t, err)
}

func TestUpdateJobsBatch(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		ldb := NewLookoutDb(db, fatalErrors, m, 10)
		// Insert
		err := ldb.CreateJobsBatch(armadacontext.Background(), defaultInstructionSet().JobsToCreate)
		assert.Nil(t, err)

		// Update
		err = ldb.UpdateJobsBatch(armadacontext.Background(), defaultInstructionSet().JobsToUpdate)
		assert.Nil(t, err)
		job := getJob(t, db, jobIdString)
		assert.Equal(t, expectedJobAfterUpdate, job)

		err = ldb.UpdateJobsBatch(armadacontext.Background(), defaultInstructionSet().JobsToUpdate)
		assert.Nil(t, err)
		job = getJob(t, db, jobIdString)
		assert.Equal(t, expectedJobAfterUpdate, job)

		// If an update is bad then we should return an error and no updates should happen
		_, err = db.Exec(armadacontext.Background(), "DELETE FROM job")
		assert.NoError(t, err)
		err = ldb.CreateJobsBatch(armadacontext.Background(), defaultInstructionSet().JobsToCreate)
		assert.Nil(t, err)
		invalidUpdate := &model.UpdateJobInstruction{
			JobId: invalidId,
		}
		err = ldb.UpdateJobsBatch(armadacontext.Background(), append(defaultInstructionSet().JobsToUpdate, invalidUpdate))
		assert.Error(t, err)
		job = getJob(t, db, jobIdString)
		assert.Equal(t, expectedJobAfterSubmit, job)
		return nil
	})
	assert.NoError(t, err)
}

func TestUpdateJobsScalar(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		ldb := NewLookoutDb(db, fatalErrors, m, 10)
		// Insert
		err := ldb.CreateJobsBatch(armadacontext.Background(), defaultInstructionSet().JobsToCreate)
		assert.Nil(t, err)

		// Update
		ldb.UpdateJobsScalar(armadacontext.Background(), defaultInstructionSet().JobsToUpdate)
		job := getJob(t, db, jobIdString)
		assert.Equal(t, expectedJobAfterUpdate, job)

		// Insert again and test that it's idempotent
		ldb.UpdateJobsScalar(armadacontext.Background(), defaultInstructionSet().JobsToUpdate)
		job = getJob(t, db, jobIdString)
		assert.Equal(t, expectedJobAfterUpdate, job)

		// If a update is bad then we should return an error and no updates should happen
		_, err = db.Exec(armadacontext.Background(), "DELETE FROM job")
		assert.NoError(t, err)
		err = ldb.CreateJobsBatch(armadacontext.Background(), defaultInstructionSet().JobsToCreate)
		assert.Nil(t, err)
		invalidUpdate := &model.UpdateJobInstruction{
			JobId: invalidId,
		}
		ldb.UpdateJobsScalar(armadacontext.Background(), append(defaultInstructionSet().JobsToUpdate, invalidUpdate))
		job = getJob(t, db, jobIdString)
		assert.Equal(t, expectedJobAfterUpdate, job)
		return nil
	})
	assert.NoError(t, err)
}

func TestUpdateJobsWithTerminal(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		initial := []*model.CreateJobInstruction{
			{
				JobId:                     jobIdString,
				Queue:                     queue,
				Owner:                     userId,
				JobSet:                    jobSetName,
				Cpu:                       cpu,
				Memory:                    memory,
				EphemeralStorage:          ephemeralStorage,
				Gpu:                       gpu,
				Priority:                  priority,
				Submitted:                 baseTime,
				State:                     lookout.JobQueuedOrdinal,
				LastTransitionTime:        baseTime,
				LastTransitionTimeSeconds: baseTime.Unix(),
				JobProto:                  []byte(jobProto),
				PriorityClass:             pointer.String(priorityClass),
				Annotations:               map[string]string{},
			},
			{
				JobId:                     "job2",
				Queue:                     queue,
				Owner:                     userId,
				JobSet:                    jobSetName,
				Cpu:                       cpu,
				Memory:                    memory,
				EphemeralStorage:          ephemeralStorage,
				Gpu:                       gpu,
				Priority:                  priority,
				Submitted:                 baseTime,
				State:                     lookout.JobQueuedOrdinal,
				LastTransitionTime:        baseTime,
				LastTransitionTimeSeconds: baseTime.Unix(),
				JobProto:                  []byte(jobProto),
				PriorityClass:             pointer.String(priorityClass),
				Annotations:               map[string]string{},
			},
			{
				JobId:                     "job3",
				Queue:                     queue,
				Owner:                     userId,
				JobSet:                    jobSetName,
				Cpu:                       cpu,
				Memory:                    memory,
				EphemeralStorage:          ephemeralStorage,
				Gpu:                       gpu,
				Priority:                  priority,
				Submitted:                 baseTime,
				State:                     lookout.JobQueuedOrdinal,
				LastTransitionTime:        baseTime,
				LastTransitionTimeSeconds: baseTime.Unix(),
				JobProto:                  []byte(jobProto),
				PriorityClass:             pointer.String(priorityClass),
				Annotations:               map[string]string{},
			},
		}

		update1 := []*model.UpdateJobInstruction{
			{
				JobId:                     jobIdString,
				State:                     pointer.Int32(lookout.JobCancelledOrdinal),
				Cancelled:                 &baseTime,
				CancelReason:              pointer.String("some reason"),
				LastTransitionTime:        &baseTime,
				LastTransitionTimeSeconds: pointer.Int64(baseTime.Unix()),
			},
			{
				JobId:                     "job2",
				State:                     pointer.Int32(lookout.JobSucceededOrdinal),
				Cancelled:                 &baseTime,
				LastTransitionTime:        &baseTime,
				LastTransitionTimeSeconds: pointer.Int64(baseTime.Unix()),
			},
			{
				JobId:                     "job3",
				State:                     pointer.Int32(lookout.JobFailedOrdinal),
				Cancelled:                 &baseTime,
				LastTransitionTime:        &baseTime,
				LastTransitionTimeSeconds: pointer.Int64(baseTime.Unix()),
			},
		}

		update2 := []*model.UpdateJobInstruction{{
			JobId:                     jobIdString,
			State:                     pointer.Int32(lookout.JobRunningOrdinal),
			LastTransitionTime:        &baseTime,
			LastTransitionTimeSeconds: pointer.Int64(baseTime.Unix()),
			LatestRunId:               pointer.String(runIdString),
		}, {
			JobId:                     "job2",
			State:                     pointer.Int32(lookout.JobRunningOrdinal),
			LastTransitionTime:        &baseTime,
			LastTransitionTimeSeconds: pointer.Int64(baseTime.Unix()),
			LatestRunId:               pointer.String(runIdString),
		}, {
			JobId:                     "job3",
			State:                     pointer.Int32(lookout.JobRunningOrdinal),
			LastTransitionTime:        &baseTime,
			LastTransitionTimeSeconds: pointer.Int64(baseTime.Unix()),
			LatestRunId:               pointer.String(runIdString),
		}}

		ldb := NewLookoutDb(db, fatalErrors, m, 10)

		// Insert
		ldb.CreateJobs(armadacontext.Background(), initial)

		// Mark the jobs terminal
		ldb.UpdateJobs(armadacontext.Background(), update1)

		// Update the jobs - these should be discarded
		ldb.UpdateJobs(armadacontext.Background(), update2)

		// Assert the states are still terminal
		job := getJob(t, db, jobIdString)
		assert.Equal(t, lookout.JobCancelledOrdinal, int(job.State))
		assert.Equal(t, "some reason", *job.CancelReason)

		job2 := getJob(t, db, "job2")
		assert.Equal(t, lookout.JobSucceededOrdinal, int(job2.State))

		job3 := getJob(t, db, "job3")
		assert.Equal(t, lookout.JobFailedOrdinal, int(job3.State))

		return nil
	})
	assert.NoError(t, err)
}

func TestCreateJobsScalar(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		ldb := NewLookoutDb(db, fatalErrors, m, 10)
		// Simple create
		ldb.CreateJobsScalar(armadacontext.Background(), defaultInstructionSet().JobsToCreate)
		job := getJob(t, db, jobIdString)
		assert.Equal(t, expectedJobAfterSubmit, job)

		// Insert again and check for idempotency
		ldb.CreateJobsScalar(armadacontext.Background(), defaultInstructionSet().JobsToCreate)
		job = getJob(t, db, jobIdString)
		assert.Equal(t, expectedJobAfterSubmit, job)

		// If a row is bad then we should update only the good rows
		_, err := ldb.db.Exec(armadacontext.Background(), "DELETE FROM job")
		assert.NoError(t, err)
		invalidJob := &model.CreateJobInstruction{
			JobId: invalidId,
		}
		ldb.CreateJobsScalar(armadacontext.Background(), append(defaultInstructionSet().JobsToCreate, invalidJob))
		job = getJob(t, db, jobIdString)
		assert.Equal(t, expectedJobAfterSubmit, job)
		return nil
	})
	assert.NoError(t, err)
}

func TestCreateJobRunsBatch(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		ldb := NewLookoutDb(db, fatalErrors, m, 10)
		// Need to make sure we have a job, so we can satisfy PK
		err := ldb.CreateJobsBatch(armadacontext.Background(), defaultInstructionSet().JobsToCreate)
		assert.Nil(t, err)

		// Insert
		err = ldb.CreateJobRunsBatch(armadacontext.Background(), defaultInstructionSet().JobRunsToCreate)
		assert.Nil(t, err)
		job := getJobRun(t, db, runIdString)
		assert.Equal(t, expectedJobRun, job)

		// Insert again and test that it's idempotent
		err = ldb.CreateJobRunsBatch(armadacontext.Background(), defaultInstructionSet().JobRunsToCreate)
		assert.Nil(t, err)
		job = getJobRun(t, db, runIdString)
		assert.Equal(t, expectedJobRun, job)

		// If a row is bad then we should return an error and no updates should happen
		_, err = ldb.db.Exec(armadacontext.Background(), "DELETE FROM job_run")
		assert.NoError(t, err)
		invalidRun := &model.CreateJobRunInstruction{
			RunId: invalidId,
		}
		err = ldb.CreateJobRunsBatch(armadacontext.Background(), append(defaultInstructionSet().JobRunsToCreate, invalidRun))
		assert.Error(t, err)
		assertNoRows(t, db, "job_run")
		return nil
	})
	assert.NoError(t, err)
}

func TestCreateJobRunsScalar(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		ldb := NewLookoutDb(db, fatalErrors, m, 10)
		// Need to make sure we have a job, so we can satisfy PK
		err := ldb.CreateJobsBatch(armadacontext.Background(), defaultInstructionSet().JobsToCreate)
		assert.Nil(t, err)

		// Insert
		ldb.CreateJobRunsScalar(armadacontext.Background(), defaultInstructionSet().JobRunsToCreate)
		job := getJobRun(t, db, runIdString)
		assert.Equal(t, expectedJobRun, job)

		// Insert again and test that it's idempotent
		ldb.CreateJobRunsScalar(armadacontext.Background(), defaultInstructionSet().JobRunsToCreate)
		job = getJobRun(t, db, runIdString)
		assert.Equal(t, expectedJobRun, job)

		// If a row is bad then we create rows that can be created
		_, err = db.Exec(armadacontext.Background(), "DELETE FROM job_run")
		assert.NoError(t, err)
		invalidRun := &model.CreateJobRunInstruction{
			RunId: invalidId,
		}
		ldb.CreateJobRunsScalar(armadacontext.Background(), append(defaultInstructionSet().JobRunsToCreate, invalidRun))
		job = getJobRun(t, db, runIdString)
		assert.Equal(t, expectedJobRun, job)
		return nil
	})
	assert.NoError(t, err)
}

func TestUpdateJobRunsBatch(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		ldb := NewLookoutDb(db, fatalErrors, m, 10)
		// Need to make sure we have a job and run
		err := ldb.CreateJobsBatch(armadacontext.Background(), defaultInstructionSet().JobsToCreate)
		assert.Nil(t, err)

		err = ldb.CreateJobRunsBatch(armadacontext.Background(), defaultInstructionSet().JobRunsToCreate)
		assert.Nil(t, err)

		// Update
		err = ldb.UpdateJobRunsBatch(armadacontext.Background(), defaultInstructionSet().JobRunsToUpdate)
		assert.Nil(t, err)
		run := getJobRun(t, db, runIdString)
		assert.Equal(t, expectedJobRunAfterUpdate, run)

		// Update again and test that it's idempotent
		err = ldb.UpdateJobRunsBatch(armadacontext.Background(), defaultInstructionSet().JobRunsToUpdate)
		assert.Nil(t, err)
		run = getJobRun(t, db, runIdString)
		assert.Equal(t, expectedJobRunAfterUpdate, run)

		// If a row is bad then we should return an error and no updates should happen
		_, err = db.Exec(armadacontext.Background(), "DELETE FROM job_run;")
		assert.Nil(t, err)
		invalidRun := &model.UpdateJobRunInstruction{
			RunId: invalidId,
		}
		err = ldb.CreateJobRunsBatch(armadacontext.Background(), defaultInstructionSet().JobRunsToCreate)
		assert.Nil(t, err)
		err = ldb.UpdateJobRunsBatch(armadacontext.Background(), append(defaultInstructionSet().JobRunsToUpdate, invalidRun))
		assert.Error(t, err)
		run = getJobRun(t, db, runIdString)
		assert.Equal(t, expectedJobRun, run)
		return nil
	})
	assert.NoError(t, err)
}

func TestUpdateJobRunsScalar(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		ldb := NewLookoutDb(db, fatalErrors, m, 10)
		// Need to make sure we have a job and run
		err := ldb.CreateJobsBatch(armadacontext.Background(), defaultInstructionSet().JobsToCreate)
		assert.Nil(t, err)

		err = ldb.CreateJobRunsBatch(armadacontext.Background(), defaultInstructionSet().JobRunsToCreate)
		assert.Nil(t, err)

		// Update
		ldb.UpdateJobRunsScalar(armadacontext.Background(), defaultInstructionSet().JobRunsToUpdate)
		assert.Nil(t, err)
		run := getJobRun(t, db, runIdString)
		assert.Equal(t, expectedJobRunAfterUpdate, run)

		// Update again and test that it's idempotent
		ldb.UpdateJobRunsScalar(armadacontext.Background(), defaultInstructionSet().JobRunsToUpdate)
		assert.Nil(t, err)
		run = getJobRun(t, db, runIdString)
		assert.Equal(t, expectedJobRunAfterUpdate, run)

		// If a row is bad then we should update the rows we can
		_, err = ldb.db.Exec(armadacontext.Background(), "DELETE FROM job_run;")
		assert.Nil(t, err)
		invalidRun := &model.UpdateJobRunInstruction{
			RunId: invalidId,
		}
		err = ldb.CreateJobRunsBatch(armadacontext.Background(), defaultInstructionSet().JobRunsToCreate)
		assert.Nil(t, err)
		ldb.UpdateJobRunsScalar(armadacontext.Background(), append(defaultInstructionSet().JobRunsToUpdate, invalidRun))
		run = getJobRun(t, ldb.db, runIdString)
		assert.Equal(t, expectedJobRunAfterUpdate, run)
		return nil
	})
	assert.NoError(t, err)
}

func TestCreateJobErrorsBatch(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		ldb := NewLookoutDb(db, fatalErrors, m, 10)

		// Insert
		err := ldb.CreateJobErrorsBatch(armadacontext.Background(), defaultInstructionSet().JobErrorsToCreate)
		assert.Nil(t, err)
		jobError := getJobError(t, db, jobIdString)
		assert.Equal(t, expectedJobError, jobError)

		// Insert again and test that it's idempotent
		err = ldb.CreateJobErrorsBatch(armadacontext.Background(), defaultInstructionSet().JobErrorsToCreate)
		assert.Nil(t, err)
		jobError = getJobError(t, db, jobIdString)
		assert.Equal(t, expectedJobError, jobError)

		// If a row is bad then we should return an error and no updates should happen
		_, err = ldb.db.Exec(armadacontext.Background(), "DELETE FROM job_error")
		assert.NoError(t, err)
		invalidError := &model.CreateJobErrorInstruction{
			JobId: invalidId,
		}
		err = ldb.CreateJobErrorsBatch(armadacontext.Background(), append(defaultInstructionSet().JobErrorsToCreate, invalidError))
		assert.Error(t, err)
		assertNoRows(t, db, "job_error")
		return nil
	})
	assert.NoError(t, err)
}

func TestCreateJobErrorsScalar(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		ldb := NewLookoutDb(db, fatalErrors, m, 10)

		// Insert
		ldb.CreateJobErrorsScalar(armadacontext.Background(), defaultInstructionSet().JobErrorsToCreate)
		jobError := getJobError(t, db, jobIdString)
		assert.Equal(t, expectedJobError, jobError)

		// Insert again and test that it's idempotent
		ldb.CreateJobErrorsScalar(armadacontext.Background(), defaultInstructionSet().JobErrorsToCreate)
		jobError = getJobError(t, db, jobIdString)
		assert.Equal(t, expectedJobError, jobError)
		return nil
	})
	assert.NoError(t, err)
}

func TestStoreWithEmptyInstructionSet(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		ldb := NewLookoutDb(db, fatalErrors, m, 10)
		err := ldb.Store(armadacontext.Background(), &model.InstructionSet{
			MessageIds: []pulsar.MessageID{pulsarutils.NewMessageId(1)},
		})
		assert.NoError(t, err)
		assertNoRows(t, ldb.db, "job")
		assertNoRows(t, ldb.db, "job_run")
		return nil
	})
	assert.NoError(t, err)
}

func TestStore(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		ldb := NewLookoutDb(db, fatalErrors, m, 10)
		// Do the update
		err := ldb.Store(armadacontext.Background(), defaultInstructionSet())
		assert.NoError(t, err)

		job := getJob(t, ldb.db, jobIdString)
		jobRun := getJobRun(t, ldb.db, runIdString)

		assert.Equal(t, expectedJobAfterUpdate, job)
		assert.Equal(t, expectedJobRunAfterUpdate, jobRun)
		return nil
	})
	assert.NoError(t, err)
}

func TestConflateJobUpdates(t *testing.T) {
	// Empty
	updates := conflateJobUpdates([]*model.UpdateJobInstruction{})
	assert.Equal(t, []*model.UpdateJobInstruction{}, updates)

	// Non-Empty
	updates = conflateJobUpdates([]*model.UpdateJobInstruction{
		{JobId: jobIdString, Priority: pointer.Int64(3)},
		{JobId: jobIdString, State: pointer.Int32(2)},
		{JobId: "someOtherJob", State: pointer.Int32(3)},
	})

	expected := []*model.UpdateJobInstruction{
		{JobId: jobIdString, Priority: pointer.Int64(3), State: pointer.Int32(2)},
		{JobId: "someOtherJob", State: pointer.Int32(3)},
	}
	sort.Slice(updates, func(i, j int) bool {
		return updates[i].JobId < updates[j].JobId
	})

	sort.Slice(expected, func(i, j int) bool {
		return expected[i].JobId < expected[j].JobId
	})
	assert.Equal(t, expected, updates)
}

func TestConflateJobUpdatesWithTerminal(t *testing.T) {
	// Updates after the cancelled shouldn't be processed
	updates := conflateJobUpdates([]*model.UpdateJobInstruction{
		{JobId: jobIdString, State: pointer.Int32(lookout.JobCancelledOrdinal)},
		{JobId: jobIdString, State: pointer.Int32(lookout.JobRunningOrdinal)},
		{JobId: "someSucceededJob", State: pointer.Int32(lookout.JobSucceededOrdinal)},
		{JobId: "someSucceededJob", State: pointer.Int32(lookout.JobRunningOrdinal)},
		{JobId: "someFailedJob", State: pointer.Int32(lookout.JobFailedOrdinal)},
		{JobId: "someFailedJob", State: pointer.Int32(lookout.JobRunningOrdinal)},
	})

	expected := []*model.UpdateJobInstruction{
		{JobId: jobIdString, State: pointer.Int32(lookout.JobCancelledOrdinal)},
		{JobId: "someSucceededJob", State: pointer.Int32(lookout.JobSucceededOrdinal)},
		{JobId: "someFailedJob", State: pointer.Int32(lookout.JobFailedOrdinal)},
	}

	sort.Slice(updates, func(i, j int) bool {
		return updates[i].JobId < updates[j].JobId
	})

	sort.Slice(expected, func(i, j int) bool {
		return expected[i].JobId < expected[j].JobId
	})
	assert.Equal(t, expected, updates)
}

func TestConflateJobUpdatesWithPreempted(t *testing.T) {
	// Updates after the cancelled shouldn't be processed
	updates := conflateJobUpdates([]*model.UpdateJobInstruction{
		{JobId: jobIdString, State: pointer.Int32(lookout.JobPreemptedOrdinal)},
		{JobId: jobIdString, State: pointer.Int32(lookout.JobFailedOrdinal)},
		{JobId: jobIdString, State: pointer.Int32(lookout.JobRunningOrdinal)},
		{JobId: jobIdString, State: pointer.Int32(lookout.JobRunningOrdinal)},
		{JobId: "job2", State: pointer.Int32(lookout.JobPreemptedOrdinal)},
		{JobId: "job2", State: pointer.Int32(lookout.JobRunningOrdinal)},
		{JobId: "job2", State: pointer.Int32(lookout.JobFailedOrdinal)},
		{JobId: "job3", State: pointer.Int32(lookout.JobFailedOrdinal)},
		{JobId: "job3", State: pointer.Int32(lookout.JobSucceededOrdinal)},
		{JobId: "job3", State: pointer.Int32(lookout.JobPreemptedOrdinal)},
	})

	expected := []*model.UpdateJobInstruction{
		{JobId: jobIdString, State: pointer.Int32(lookout.JobPreemptedOrdinal)},
		{JobId: "job2", State: pointer.Int32(lookout.JobPreemptedOrdinal)},
		{JobId: "job3", State: pointer.Int32(lookout.JobPreemptedOrdinal)},
	}

	sort.Slice(updates, func(i, j int) bool {
		return updates[i].JobId < updates[j].JobId
	})

	sort.Slice(expected, func(i, j int) bool {
		return expected[i].JobId < expected[j].JobId
	})
	assert.Equal(t, expected, updates)
}

func TestConflateJobUpdatesWithNullState(t *testing.T) {
	updates := conflateJobUpdates([]*model.UpdateJobInstruction{
		{JobId: jobIdString, State: pointer.Int32(lookout.JobFailedOrdinal)},
		{JobId: jobIdString, LatestRunId: pointer.String("test-id")},
	})

	// Ignored because terminal event received
	expected := []*model.UpdateJobInstruction{
		{JobId: jobIdString, State: pointer.Int32(lookout.JobFailedOrdinal)},
	}

	sort.Slice(updates, func(i, j int) bool {
		return updates[i].JobId < updates[j].JobId
	})

	sort.Slice(expected, func(i, j int) bool {
		return expected[i].JobId < expected[j].JobId
	})
	assert.Equal(t, expected, updates)
}

func TestConflateJobRunUpdates(t *testing.T) {
	// Empty
	updates := conflateJobRunUpdates([]*model.UpdateJobRunInstruction{})
	assert.Equal(t, []*model.UpdateJobRunInstruction{}, updates)

	// Non-Empty
	updates = conflateJobRunUpdates([]*model.UpdateJobRunInstruction{
		{RunId: runIdString, Started: &baseTime},
		{RunId: runIdString, Node: pointer.String(nodeName), Debug: []byte("some \000 debug \000")},
		{RunId: "someOtherJobRun", Started: &baseTime},
	})

	expected := []*model.UpdateJobRunInstruction{
		{RunId: runIdString, Started: &baseTime, Node: pointer.String(nodeName), Debug: []byte("some \000 debug \000")},
		{RunId: "someOtherJobRun", Started: &baseTime},
	}

	sort.Slice(updates, func(i, j int) bool {
		return updates[i].RunId < updates[j].RunId
	})

	sort.Slice(expected, func(i, j int) bool {
		return expected[i].RunId < expected[j].RunId
	})
	assert.Equal(t, expected, updates)

	// Latest job run state is used
	updates = conflateJobRunUpdates([]*model.UpdateJobRunInstruction{
		{RunId: runIdString, Started: &baseTime, JobRunState: pointer.Int32(lookout.JobRunRunningOrdinal)},
		{RunId: runIdString, Node: pointer.String(nodeName), JobRunState: pointer.Int32(lookout.JobRunSucceededOrdinal)},
	})

	expected = []*model.UpdateJobRunInstruction{
		{RunId: runIdString, Started: &baseTime, Node: pointer.String(nodeName), JobRunState: pointer.Int32(lookout.JobRunSucceededOrdinal)},
	}

	assert.Equal(t, expected, updates)
}

func TestStoreNullValue(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		jobProto := []byte("hello \000 world \000")
		errorMsg := []byte("some \000 error \000")
		debugMsg := []byte("some \000 debug \000")
		instructions := defaultInstructionSet()
		instructions.JobsToCreate[0].JobProto = jobProto
		instructions.JobRunsToUpdate[0].Error = errorMsg
		instructions.JobRunsToUpdate[0].Debug = debugMsg

		ldb := NewLookoutDb(db, fatalErrors, m, 10)
		// Do the update
		err := ldb.Store(armadacontext.Background(), instructions)
		assert.NoError(t, err)

		job := getJob(t, ldb.db, jobIdString)
		jobRun := getJobRun(t, ldb.db, runIdString)

		assert.Equal(t, jobProto, job.JobProto)
		assert.Equal(t, errorMsg, jobRun.Error)
		assert.Equal(t, debugMsg, jobRun.Debug)
		return nil
	})
	assert.NoError(t, err)
}

func TestStoreEventsForAlreadyTerminalJobs(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		ldb := NewLookoutDb(db, fatalErrors, m, 10)

		baseInstructions := &model.InstructionSet{
			JobsToCreate: []*model.CreateJobInstruction{
				makeCreateJobInstruction("job-1"),
				makeCreateJobInstruction("job-2"),
				makeCreateJobInstruction("job-3"),
			},
			JobsToUpdate: []*model.UpdateJobInstruction{
				makeUpdateJobInstruction("job-1", lookout.JobFailedOrdinal),
				makeUpdateJobInstruction("job-2", lookout.JobSucceededOrdinal),
				makeUpdateJobInstruction("job-3", lookout.JobCancelledOrdinal),
			},
			MessageIds: []pulsar.MessageID{pulsarutils.NewMessageId(3)},
		}

		// Create the jobs in the DB
		err := ldb.Store(armadacontext.Background(), baseInstructions)
		assert.NoError(t, err)

		mutateInstructions := &model.InstructionSet{
			JobsToUpdate: []*model.UpdateJobInstruction{
				makeUpdateJobInstruction("job-1", lookout.JobFailedOrdinal),
				makeUpdateJobInstruction("job-2", lookout.JobSucceededOrdinal),
				makeUpdateJobInstruction("job-3", lookout.JobCancelledOrdinal),

				makeUpdateJobInstruction("job-1", lookout.JobPreemptedOrdinal),
				makeUpdateJobInstruction("job-2", lookout.JobPreemptedOrdinal),
				makeUpdateJobInstruction("job-3", lookout.JobPreemptedOrdinal),

				makeUpdateJobInstruction("job-1", lookout.JobFailedOrdinal),
				makeUpdateJobInstruction("job-2", lookout.JobSucceededOrdinal),
				makeUpdateJobInstruction("job-3", lookout.JobCancelledOrdinal),
			},
		}

		// Update the jobs in the DB
		err = ldb.Store(armadacontext.Background(), mutateInstructions)
		assert.NoError(t, err)

		for _, jobId := range []string{"job-1", "job-2", "job-3"} {
			job := getJob(t, db, jobId)
			assert.Equal(t, lookout.JobPreemptedOrdinal, int(job.State))
		}

		return nil
	})
	assert.NoError(t, err)
}

func makeCreateJobInstruction(jobId string) *model.CreateJobInstruction {
	return &model.CreateJobInstruction{
		JobId:                     jobId,
		Queue:                     queue,
		Owner:                     userId,
		JobSet:                    jobSetName,
		Cpu:                       cpu,
		Memory:                    memory,
		EphemeralStorage:          ephemeralStorage,
		Gpu:                       gpu,
		Priority:                  priority,
		Submitted:                 baseTime,
		State:                     lookout.JobQueuedOrdinal,
		LastTransitionTime:        baseTime,
		LastTransitionTimeSeconds: baseTime.Unix(),
		JobProto:                  []byte(jobProto),
		PriorityClass:             pointer.String(priorityClass),
		Annotations:               annotations,
	}
}

func makeUpdateJobInstruction(jobId string, state int32) *model.UpdateJobInstruction {
	return &model.UpdateJobInstruction{
		JobId:                     jobId,
		Priority:                  pointer.Int64(updatePriority),
		State:                     pointer.Int32(state),
		LastTransitionTime:        &updateTime,
		LastTransitionTimeSeconds: pointer.Int64(updateTime.Unix()),
	}
}

func getJob(t *testing.T, db *pgxpool.Pool, jobId string) JobRow {
	job := JobRow{}
	r := db.QueryRow(
		armadacontext.Background(),
		`SELECT
    		job_id,
    		queue,
    		owner,
    		jobset,
    		cpu,
    		memory,
    		ephemeral_storage,
    		gpu,
    		priority,
    		submitted,
    		cancelled,
    		state,
    		last_transition_time,
    		last_transition_time_seconds,
    		job_spec,
    		duplicate,
			priority_class,
			latest_run_id,
			cancel_reason,
			annotations
		FROM job WHERE job_id = $1`,
		jobId)
	err := r.Scan(
		&job.JobId,
		&job.Queue,
		&job.Owner,
		&job.JobSet,
		&job.Cpu,
		&job.Memory,
		&job.EphemeralStorage,
		&job.Gpu,
		&job.Priority,
		&job.Submitted,
		&job.Cancelled,
		&job.State,
		&job.LastTransitionTime,
		&job.LastTransitionTimeSeconds,
		&job.JobProto,
		&job.Duplicate,
		&job.PriorityClass,
		&job.LatestRunId,
		&job.CancelReason,
		&job.Annotations,
	)
	assert.Nil(t, err)
	return job
}

func getJobRun(t *testing.T, db *pgxpool.Pool, runId string) JobRunRow {
	run := JobRunRow{}
	r := db.QueryRow(
		armadacontext.Background(),
		`SELECT
			run_id,
			job_id,
			cluster,
			node,
			pending,
			started,
			finished,
			job_run_state,
			error,
			exit_code,
			debug
		FROM job_run WHERE run_id = $1`,
		runId)
	err := r.Scan(
		&run.RunId,
		&run.JobId,
		&run.Cluster,
		&run.Node,
		&run.Pending,
		&run.Started,
		&run.Finished,
		&run.JobRunState,
		&run.Error,
		&run.ExitCode,
		&run.Debug,
	)
	assert.NoError(t, err)
	return run
}

func getJobError(t *testing.T, db *pgxpool.Pool, jobId string) JobErrorRow {
	errorRow := JobErrorRow{}
	r := db.QueryRow(
		armadacontext.Background(),
		"SELECT job_id, error FROM job_error WHERE job_id = $1",
		jobId)

	err := r.Scan(&errorRow.JobId, &errorRow.Error)

	assert.NoError(t, err)
	return errorRow
}

func assertNoRows(t *testing.T, db *pgxpool.Pool, table string) {
	t.Helper()
	var count int
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", table)
	r := db.QueryRow(armadacontext.Background(), query)
	err := r.Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 0, count)
}
