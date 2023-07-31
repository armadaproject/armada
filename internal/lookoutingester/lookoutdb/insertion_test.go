package lookoutdb

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/armadaproject/armada/internal/common/database/lookout"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"k8s.io/utils/pointer"

	"github.com/armadaproject/armada/internal/common/pulsarutils"
	"github.com/armadaproject/armada/internal/lookout/configuration"
	"github.com/armadaproject/armada/internal/lookout/repository"
	"github.com/armadaproject/armada/internal/lookout/testutil"
	"github.com/armadaproject/armada/internal/lookoutingester/metrics"
	"github.com/armadaproject/armada/internal/lookoutingester/model"
)

const (
	jobIdString    = "01f3j0g1md4qx7z5qb148qnh4r"
	runIdString    = "123e4567-e89b-12d3-a456-426614174000"
	jobSetName     = "testJobset"
	executorId     = "testCluster"
	nodeName       = "testNode"
	queue          = "test-queue"
	userId         = "testUser"
	priority       = 3
	updatePriority = 4
	updateState    = 5
	podNumber      = 6
	jobJson        = `{"foo": "bar"}`
	jobProto       = "hello world"
	containerName  = "testContainer"
)

var (
	baseTime, _      = time.Parse("2006-01-02T15:04:05.000Z", "2022-03-01T15:04:05.000Z")
	updateTime, _    = time.Parse("2006-01-02T15:04:05.000Z", "2022-03-01T15:04:06.000Z")
	startTime, _     = time.Parse("2006-01-02T15:04:05.000Z", "2022-03-01T15:04:07.000Z")
	finishedTime, _  = time.Parse("2006-01-02T15:04:05.000Z", "2022-03-01T15:04:08.000Z")
	preemptedTime, _ = time.Parse("2006-01-02T15:04:05.000Z", "2022-03-01T15:04:09.000Z")
)

// An invalid job id that exceeds th varchar count
var invalidId = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

type JobRow struct {
	JobId     string
	Queue     string
	Owner     string
	JobSet    string
	Priority  uint32
	Submitted time.Time
	JobProto  []byte
	State     int64
	Duplicate bool
	Updated   time.Time
	Cancelled *time.Time
}

type JobRunRow struct {
	RunId            string
	JobId            string
	Cluster          string
	Node             *string
	Created          time.Time
	Started          *time.Time
	Finished         *time.Time
	Preempted        *time.Time
	Succeeded        *bool
	Error            *string
	PodNumber        int
	UnableToSchedule *bool
}

type UserAnnotationRow struct {
	JobId string
	Key   string
	Value string
}

type JobRunContainerRow struct {
	RunId         string
	ContainerName string
	ExitCode      int
}

func defaultInstructionSet() *model.InstructionSet {
	return &model.InstructionSet{
		JobsToCreate: []*model.CreateJobInstruction{{
			JobId:     jobIdString,
			Queue:     queue,
			Owner:     userId,
			JobSet:    jobSetName,
			Priority:  priority,
			Submitted: baseTime,
			JobProto:  []byte(jobProto),
			State:     0,
			Updated:   baseTime,
		}},
		JobsToUpdate: []*model.UpdateJobInstruction{{
			JobId:    jobIdString,
			Priority: pointer.Int32(updatePriority),
			State:    pointer.Int32(updateState),
			Updated:  updateTime,
		}},
		JobRunsToCreate: []*model.CreateJobRunInstruction{{
			RunId:   runIdString,
			JobId:   jobIdString,
			Cluster: executorId,
			Created: updateTime,
		}},
		JobRunsToUpdate: []*model.UpdateJobRunInstruction{{
			RunId:            runIdString,
			Node:             pointer.String(nodeName),
			Started:          &startTime,
			Finished:         &finishedTime,
			Succeeded:        pointer.Bool(true),
			Preempted:        &preemptedTime,
			Error:            nil,
			PodNumber:        pointer.Int32(podNumber),
			UnableToSchedule: nil,
		}},
		UserAnnotationsToCreate: []*model.CreateUserAnnotationInstruction{{
			JobId: jobIdString,
			Key:   "someKey",
			Value: "someValue",
		}},
		JobRunContainersToCreate: []*model.CreateJobRunContainerInstruction{{
			RunId:         runIdString,
			ContainerName: containerName,
			ExitCode:      3,
		}},
		MessageIds: []pulsar.MessageID{pulsarutils.NewMessageId(3), pulsarutils.NewMessageId(4)},
	}
}

var expectedJobAfterSubmit = JobRow{
	JobId:     jobIdString,
	Queue:     queue,
	Owner:     userId,
	JobSet:    jobSetName,
	Priority:  priority,
	Submitted: baseTime,
	JobProto:  []byte(jobProto),
	State:     0,
	Updated:   baseTime,
	Duplicate: false,
}

var expectedJobAfterUpdate = JobRow{
	JobId:     jobIdString,
	Queue:     queue,
	Owner:     userId,
	JobSet:    jobSetName,
	Priority:  updatePriority,
	State:     updateState,
	Updated:   updateTime,
	Submitted: baseTime,
	JobProto:  []byte(jobProto),
	Duplicate: false,
}

var expectedJobRun = JobRunRow{
	RunId:     runIdString,
	JobId:     jobIdString,
	Cluster:   executorId,
	Created:   updateTime,
	PodNumber: 0,
}

var expectedJobRunAfterUpdate = JobRunRow{
	RunId:            runIdString,
	JobId:            jobIdString,
	Cluster:          executorId,
	Created:          updateTime,
	Node:             pointer.String(nodeName),
	Started:          &startTime,
	Finished:         &finishedTime,
	Succeeded:        pointer.Bool(true),
	Error:            nil,
	PodNumber:        podNumber,
	UnableToSchedule: nil,
	Preempted:        &preemptedTime,
}

var expectedUserAnnotation = UserAnnotationRow{
	JobId: jobIdString,
	Key:   "someKey",
	Value: "someValue",
}

var expectedJobRunContainer = JobRunContainerRow{
	RunId:         runIdString,
	ContainerName: containerName,
	ExitCode:      3,
}

func getTestLookoutDb(db *pgxpool.Pool) *LookoutDb {
	return &LookoutDb{
		db:      db,
		metrics: metrics.Get(),
		config:  &configuration.LookoutIngesterConfiguration{},
	}
}

func TestCreateJobsBatch(t *testing.T) {
	err := testutil.WithDatabasePgx(func(db *pgxpool.Pool) error {
		ldb := getTestLookoutDb(db)
		// Insert
		err := ldb.CreateJobsBatch(context.Background(), defaultInstructionSet().JobsToCreate)
		require.NoError(t, err)
		job := getJob(t, db, jobIdString)
		assert.Equal(t, expectedJobAfterSubmit, job)

		// Insert again and test that it's idempotent
		err = ldb.CreateJobsBatch(context.Background(), defaultInstructionSet().JobsToCreate)
		require.NoError(t, err)
		job = getJob(t, db, jobIdString)
		assert.Equal(t, expectedJobAfterSubmit, job)

		// If a row is bad then we should return an error and no updates should happen
		_, err = db.Exec(context.Background(), "DELETE FROM job")
		require.NoError(t, err)
		invalidJob := &model.CreateJobInstruction{
			JobId: invalidId,
		}
		err = ldb.CreateJobsBatch(context.Background(), append(defaultInstructionSet().JobsToCreate, invalidJob))
		assert.Error(t, err)
		assertNoRows(t, db, "job")
		return nil
	})
	assert.NoError(t, err)
}

func TestUpdateJobsBatch(t *testing.T) {
	err := testutil.WithDatabasePgx(func(db *pgxpool.Pool) error {
		ldb := getTestLookoutDb(db)
		// Insert
		err := ldb.CreateJobsBatch(context.Background(), defaultInstructionSet().JobsToCreate)
		require.NoError(t, err)

		// Update
		err = ldb.UpdateJobsBatch(context.Background(), defaultInstructionSet().JobsToUpdate)
		require.NoError(t, err)
		job := getJob(t, db, jobIdString)
		assert.Equal(t, expectedJobAfterUpdate, job)

		err = ldb.UpdateJobsBatch(context.Background(), defaultInstructionSet().JobsToUpdate)
		require.NoError(t, err)
		job = getJob(t, db, jobIdString)
		assert.Equal(t, expectedJobAfterUpdate, job)

		// If an update is bad then we should return an error and no updates should happen
		_, err = db.Exec(context.Background(), "DELETE FROM job")
		require.NoError(t, err)
		err = ldb.CreateJobsBatch(context.Background(), defaultInstructionSet().JobsToCreate)
		require.NoError(t, err)
		invalidUpdate := &model.UpdateJobInstruction{
			JobId: invalidId,
		}
		err = ldb.UpdateJobsBatch(context.Background(), append(defaultInstructionSet().JobsToUpdate, invalidUpdate))
		assert.Error(t, err)
		job = getJob(t, db, jobIdString)
		assert.Equal(t, expectedJobAfterSubmit, job)
		return nil
	})
	require.NoError(t, err)
}

func TestUpdateJobsScalar(t *testing.T) {
	err := testutil.WithDatabasePgx(func(db *pgxpool.Pool) error {
		ldb := getTestLookoutDb(db)
		// Insert
		err := ldb.CreateJobsBatch(context.Background(), defaultInstructionSet().JobsToCreate)
		require.NoError(t, err)

		// Update
		ldb.UpdateJobsScalar(context.Background(), defaultInstructionSet().JobsToUpdate)
		job := getJob(t, db, jobIdString)
		assert.Equal(t, expectedJobAfterUpdate, job)

		// Insert again and test that it's idempotent
		ldb.UpdateJobsScalar(context.Background(), defaultInstructionSet().JobsToUpdate)
		job = getJob(t, db, jobIdString)
		assert.Equal(t, expectedJobAfterUpdate, job)

		// If a update is bad then we should return an error and no updates should happen
		_, err = db.Exec(context.Background(), "DELETE FROM job")
		require.NoError(t, err)
		err = ldb.CreateJobsBatch(context.Background(), defaultInstructionSet().JobsToCreate)
		require.NoError(t, err)
		invalidUpdate := &model.UpdateJobInstruction{
			JobId: invalidId,
		}
		ldb.UpdateJobsScalar(context.Background(), append(defaultInstructionSet().JobsToUpdate, invalidUpdate))
		job = getJob(t, db, jobIdString)
		assert.Equal(t, expectedJobAfterUpdate, job)
		return nil
	})
	require.NoError(t, err)
}

func TestUpdateJobsWithTerminal(t *testing.T) {
	err := testutil.WithDatabasePgx(func(db *pgxpool.Pool) error {
		initial := []*model.CreateJobInstruction{
			{
				JobId:     jobIdString,
				Queue:     queue,
				Owner:     userId,
				JobSet:    jobSetName,
				Priority:  priority,
				Submitted: baseTime,
				State:     repository.JobQueuedOrdinal,
				JobProto:  []byte(jobProto),
			},
			{
				JobId:     "job2",
				Queue:     queue,
				Owner:     userId,
				JobSet:    jobSetName,
				Priority:  priority,
				Submitted: baseTime,
				State:     repository.JobQueuedOrdinal,
				JobProto:  []byte(jobProto),
			},
			{
				JobId:     "job3",
				Queue:     queue,
				Owner:     userId,
				JobSet:    jobSetName,
				Priority:  priority,
				Submitted: baseTime,
				State:     repository.JobQueuedOrdinal,
				JobProto:  []byte(jobProto),
			},
		}

		update1 := []*model.UpdateJobInstruction{
			{
				JobId:     jobIdString,
				State:     pointer.Int32(repository.JobCancelledOrdinal),
				Cancelled: &baseTime,
			},
			{
				JobId:     "job2",
				State:     pointer.Int32(repository.JobSucceededOrdinal),
				Cancelled: &baseTime,
			},
			{
				JobId:     "job3",
				State:     pointer.Int32(repository.JobFailedOrdinal),
				Cancelled: &baseTime,
			},
		}

		update2 := []*model.UpdateJobInstruction{{
			JobId: jobIdString,
			State: pointer.Int32(repository.JobRunningOrdinal),
		}, {
			JobId: "job2",
			State: pointer.Int32(repository.JobRunningOrdinal),
		}, {
			JobId: "job3",
			State: pointer.Int32(repository.JobRunningOrdinal),
		}}

		ldb := getTestLookoutDb(db)

		// Insert
		ldb.CreateJobs(context.Background(), initial)

		// Mark the jobs terminal
		ldb.UpdateJobs(context.Background(), update1)

		// Update the jobs - these should be discarded
		ldb.UpdateJobs(context.Background(), update2)

		// Assert the states are still terminal
		job := getJob(t, db, jobIdString)
		assert.Equal(t, lookout.JobCancelledOrdinal, int(job.State))

		job2 := getJob(t, db, "job2")
		assert.Equal(t, lookout.JobSucceededOrdinal, int(job2.State))

		job3 := getJob(t, db, "job3")
		assert.Equal(t, lookout.JobFailedOrdinal, int(job3.State))

		return nil
	})
	assert.NoError(t, err)
}

func TestCreateJobsScalar(t *testing.T) {
	err := testutil.WithDatabasePgx(func(db *pgxpool.Pool) error {
		ldb := getTestLookoutDb(db)
		// Simple create
		ldb.CreateJobsScalar(context.Background(), defaultInstructionSet().JobsToCreate)
		job := getJob(t, db, jobIdString)
		assert.Equal(t, expectedJobAfterSubmit, job)

		// Insert again and check for idempotency
		ldb.CreateJobsScalar(context.Background(), defaultInstructionSet().JobsToCreate)
		job = getJob(t, db, jobIdString)
		assert.Equal(t, expectedJobAfterSubmit, job)

		// If a row is bad then we should update only the good rows
		_, err := ldb.db.Exec(context.Background(), "DELETE FROM job")
		require.NoError(t, err)
		invalidJob := &model.CreateJobInstruction{
			JobId: invalidId,
		}
		ldb.CreateJobsScalar(context.Background(), append(defaultInstructionSet().JobsToCreate, invalidJob))
		job = getJob(t, db, jobIdString)
		assert.Equal(t, expectedJobAfterSubmit, job)
		return nil
	})
	require.NoError(t, err)
}

func TestCreateJobRunsBatch(t *testing.T) {
	err := testutil.WithDatabasePgx(func(db *pgxpool.Pool) error {
		ldb := getTestLookoutDb(db)
		// Need to make sure we have a job, so we can satisfy PK
		err := ldb.CreateJobsBatch(context.Background(), defaultInstructionSet().JobsToCreate)
		require.NoError(t, err)

		// Insert
		err = ldb.CreateJobRunsBatch(context.Background(), defaultInstructionSet().JobRunsToCreate)
		require.NoError(t, err)
		job := getJobRun(t, db, runIdString)
		assert.Equal(t, expectedJobRun, job)

		// Insert again and test that it's idempotent
		err = ldb.CreateJobRunsBatch(context.Background(), defaultInstructionSet().JobRunsToCreate)
		require.NoError(t, err)
		job = getJobRun(t, db, runIdString)
		assert.Equal(t, expectedJobRun, job)

		// If a row is bad then we should return an error and no updates should happen
		_, err = ldb.db.Exec(context.Background(), "DELETE FROM job_run")
		require.NoError(t, err)
		invalidRun := &model.CreateJobRunInstruction{
			RunId: invalidId,
		}
		err = ldb.CreateJobRunsBatch(context.Background(), append(defaultInstructionSet().JobRunsToCreate, invalidRun))
		assert.Error(t, err)
		assertNoRows(t, db, "job_run")
		return nil
	})
	require.NoError(t, err)
}

func TestCreateJobRunsScalar(t *testing.T) {
	err := testutil.WithDatabasePgx(func(db *pgxpool.Pool) error {
		ldb := getTestLookoutDb(db)
		// Need to make sure we have a job, so we can satisfy PK
		err := ldb.CreateJobsBatch(context.Background(), defaultInstructionSet().JobsToCreate)
		require.NoError(t, err)

		// Insert
		ldb.CreateJobRunsScalar(context.Background(), defaultInstructionSet().JobRunsToCreate)
		job := getJobRun(t, db, runIdString)
		assert.Equal(t, expectedJobRun, job)

		// Insert again and test that it's idempotent
		ldb.CreateJobRunsScalar(context.Background(), defaultInstructionSet().JobRunsToCreate)
		job = getJobRun(t, db, runIdString)
		assert.Equal(t, expectedJobRun, job)

		// If a row is bad then we create rows that can be created
		_, err = db.Exec(context.Background(), "DELETE FROM job_run")
		require.NoError(t, err)
		invalidRun := &model.CreateJobRunInstruction{
			RunId: invalidId,
		}
		ldb.CreateJobRunsScalar(context.Background(), append(defaultInstructionSet().JobRunsToCreate, invalidRun))
		job = getJobRun(t, db, runIdString)
		assert.Equal(t, expectedJobRun, job)
		return nil
	})
	require.NoError(t, err)
}

func TestUpdateJobRunsBatch(t *testing.T) {
	err := testutil.WithDatabasePgx(func(db *pgxpool.Pool) error {
		ldb := getTestLookoutDb(db)
		// Need to make sure we have a job and run
		err := ldb.CreateJobsBatch(context.Background(), defaultInstructionSet().JobsToCreate)
		require.NoError(t, err)

		err = ldb.CreateJobRunsBatch(context.Background(), defaultInstructionSet().JobRunsToCreate)
		require.NoError(t, err)

		// Update
		err = ldb.UpdateJobRunsBatch(context.Background(), defaultInstructionSet().JobRunsToUpdate)
		require.NoError(t, err)
		run := getJobRun(t, db, runIdString)
		assert.Equal(t, expectedJobRunAfterUpdate, run)

		// Update again and test that it's idempotent
		err = ldb.UpdateJobRunsBatch(context.Background(), defaultInstructionSet().JobRunsToUpdate)
		require.NoError(t, err)
		run = getJobRun(t, db, runIdString)
		assert.Equal(t, expectedJobRunAfterUpdate, run)

		// If a row is bad then we should return an error and no updates should happen
		_, err = db.Exec(context.Background(), "DELETE FROM job_run;")
		require.NoError(t, err)
		invalidRun := &model.UpdateJobRunInstruction{
			RunId: invalidId,
		}
		err = ldb.CreateJobRunsBatch(context.Background(), defaultInstructionSet().JobRunsToCreate)
		require.NoError(t, err)
		err = ldb.UpdateJobRunsBatch(context.Background(), append(defaultInstructionSet().JobRunsToUpdate, invalidRun))
		assert.Error(t, err)
		run = getJobRun(t, db, runIdString)
		assert.Equal(t, expectedJobRun, run)
		return nil
	})
	require.NoError(t, err)
}

func TestUpdateJobRunsScalar(t *testing.T) {
	err := testutil.WithDatabasePgx(func(db *pgxpool.Pool) error {
		ldb := getTestLookoutDb(db)
		// Need to make sure we have a job and run
		err := ldb.CreateJobsBatch(context.Background(), defaultInstructionSet().JobsToCreate)
		require.NoError(t, err)

		err = ldb.CreateJobRunsBatch(context.Background(), defaultInstructionSet().JobRunsToCreate)
		require.NoError(t, err)

		// Update
		ldb.UpdateJobRunsScalar(context.Background(), defaultInstructionSet().JobRunsToUpdate)
		require.NoError(t, err)
		run := getJobRun(t, db, runIdString)
		assert.Equal(t, expectedJobRunAfterUpdate, run)

		// Update again and test that it's idempotent
		ldb.UpdateJobRunsScalar(context.Background(), defaultInstructionSet().JobRunsToUpdate)
		require.NoError(t, err)
		run = getJobRun(t, db, runIdString)
		assert.Equal(t, expectedJobRunAfterUpdate, run)

		// If a row is bad then we should update the rows we can
		_, err = ldb.db.Exec(context.Background(), "DELETE FROM job_run;")
		require.NoError(t, err)
		invalidRun := &model.UpdateJobRunInstruction{
			RunId: invalidId,
		}
		err = ldb.CreateJobRunsBatch(context.Background(), defaultInstructionSet().JobRunsToCreate)
		assert.Nil(t, err)
		ldb.UpdateJobRunsScalar(context.Background(), append(defaultInstructionSet().JobRunsToUpdate, invalidRun))
		run = getJobRun(t, ldb.db, runIdString)
		assert.Equal(t, expectedJobRunAfterUpdate, run)
		return nil
	})
	require.NoError(t, err)
}

func TestCreateUserAnnotationsBatch(t *testing.T) {
	err := testutil.WithDatabasePgx(func(db *pgxpool.Pool) error {
		ldb := getTestLookoutDb(db)
		// Need to make sure we have a job
		err := ldb.CreateJobsBatch(context.Background(), defaultInstructionSet().JobsToCreate)
		require.NoError(t, err)

		// Insert
		err = ldb.CreateUserAnnotationsBatch(context.Background(), defaultInstructionSet().UserAnnotationsToCreate)
		require.NoError(t, err)
		annotation := getUserAnnotationLookup(t, db, jobIdString)
		assert.Equal(t, expectedUserAnnotation, annotation)

		// Insert again and test that it's idempotent
		err = ldb.CreateUserAnnotationsBatch(context.Background(), defaultInstructionSet().UserAnnotationsToCreate)
		require.NoError(t, err)
		annotation = getUserAnnotationLookup(t, db, jobIdString)
		assert.Equal(t, expectedUserAnnotation, annotation)

		// If a row is bad then we should return an error and no updates should happen
		_, err = ldb.db.Exec(context.Background(), "DELETE FROM user_annotation_lookup")
		require.NoError(t, err)
		invalidAnnotation := &model.CreateUserAnnotationInstruction{
			JobId: invalidId,
		}
		err = ldb.CreateUserAnnotationsBatch(context.Background(), append(defaultInstructionSet().UserAnnotationsToCreate, invalidAnnotation))
		assert.Error(t, err)
		assertNoRows(t, ldb.db, "user_annotation_lookup")
		return nil
	})
	require.NoError(t, err)
}

func TestEmptyUpdate(t *testing.T) {
	err := testutil.WithDatabasePgx(func(db *pgxpool.Pool) error {
		ldb := getTestLookoutDb(db)
		storeErr := ldb.Store(context.Background(), &model.InstructionSet{})
		require.NoError(t, storeErr)
		assertNoRows(t, ldb.db, "job")
		assertNoRows(t, ldb.db, "job_run")
		assertNoRows(t, ldb.db, "user_annotation_lookup")
		assertNoRows(t, ldb.db, "job_run_container")
		return nil
	})
	require.NoError(t, err)
}

func TestCreateUserAnnotationsScalar(t *testing.T) {
	err := testutil.WithDatabasePgx(func(db *pgxpool.Pool) error {
		ldb := getTestLookoutDb(db)
		// Need to make sure we have a job
		err := ldb.CreateJobsBatch(context.Background(), defaultInstructionSet().JobsToCreate)
		require.NoError(t, err)

		// Insert
		ldb.CreateUserAnnotationsScalar(context.Background(), defaultInstructionSet().UserAnnotationsToCreate)
		annotation := getUserAnnotationLookup(t, db, jobIdString)
		assert.Equal(t, expectedUserAnnotation, annotation)

		// Insert again and test that it's idempotent
		ldb.CreateUserAnnotationsScalar(context.Background(), defaultInstructionSet().UserAnnotationsToCreate)
		annotation = getUserAnnotationLookup(t, db, jobIdString)
		assert.Equal(t, expectedUserAnnotation, annotation)

		// If a row is bad then we should update the rows we can
		_, err = ldb.db.Exec(context.Background(), "DELETE FROM user_annotation_lookup")
		require.NoError(t, err)
		invalidAnnotation := &model.CreateUserAnnotationInstruction{
			JobId: invalidId,
		}
		ldb.CreateUserAnnotationsScalar(context.Background(), append(defaultInstructionSet().UserAnnotationsToCreate, invalidAnnotation))
		annotation = getUserAnnotationLookup(t, ldb.db, jobIdString)
		assert.Equal(t, expectedUserAnnotation, annotation)
		return nil
	})
	require.NoError(t, err)
}

func TestUpdate(t *testing.T) {
	err := testutil.WithDatabasePgx(func(db *pgxpool.Pool) error {
		ldb := getTestLookoutDb(db)
		// Do the update
		storeErr := ldb.Store(context.Background(), defaultInstructionSet())
		require.NoError(t, storeErr)
		job := getJob(t, ldb.db, jobIdString)
		jobRun := getJobRun(t, ldb.db, runIdString)
		annotation := getUserAnnotationLookup(t, ldb.db, jobIdString)
		container := getJobRunContainer(t, ldb.db, runIdString)

		assert.Equal(t, expectedJobAfterUpdate, job)
		assert.Equal(t, expectedJobRunAfterUpdate, jobRun)
		assert.Equal(t, expectedJobRunContainer, container)
		assert.Equal(t, expectedUserAnnotation, annotation)
		return nil
	})
	require.NoError(t, err)
}

func TestConflateJobUpdates(T *testing.T) {
	// Empty
	updates := conflateJobUpdates([]*model.UpdateJobInstruction{})
	assert.Equal(T, []*model.UpdateJobInstruction{}, updates)

	// Non-Empty
	updates = conflateJobUpdates([]*model.UpdateJobInstruction{
		{JobId: jobIdString, Priority: pointer.Int32(3)},
		{JobId: jobIdString, State: pointer.Int32(2)},
		{JobId: "someOtherJob", State: pointer.Int32(3)},
	})

	expected := []*model.UpdateJobInstruction{
		{JobId: jobIdString, Priority: pointer.Int32(3), State: pointer.Int32(2)},
		{JobId: "someOtherJob", State: pointer.Int32(3)},
	}
	sort.Slice(updates, func(i, j int) bool {
		return updates[i].JobId < updates[j].JobId
	})

	sort.Slice(expected, func(i, j int) bool {
		return expected[i].JobId < expected[j].JobId
	})
	assert.Equal(T, expected, updates)
}

func TestConflateJobUpdatesWithTerminal(T *testing.T) {
	// Updates after the cancelled shouldn't be processed
	updates := conflateJobUpdates([]*model.UpdateJobInstruction{
		{JobId: jobIdString, State: pointer.Int32(repository.JobCancelledOrdinal)},
		{JobId: jobIdString, State: pointer.Int32(repository.JobRunningOrdinal)},
		{JobId: "someSucceededJob", State: pointer.Int32(repository.JobSucceededOrdinal)},
		{JobId: "someSucceededJob", State: pointer.Int32(repository.JobRunningOrdinal)},
		{JobId: "someFailedJob", State: pointer.Int32(repository.JobFailedOrdinal)},
		{JobId: "someFailedJob", State: pointer.Int32(repository.JobRunningOrdinal)},
	})

	expected := []*model.UpdateJobInstruction{
		{JobId: jobIdString, State: pointer.Int32(repository.JobCancelledOrdinal)},
		{JobId: "someSucceededJob", State: pointer.Int32(repository.JobSucceededOrdinal)},
		{JobId: "someFailedJob", State: pointer.Int32(repository.JobFailedOrdinal)},
	}

	sort.Slice(updates, func(i, j int) bool {
		return updates[i].JobId < updates[j].JobId
	})

	sort.Slice(expected, func(i, j int) bool {
		return expected[i].JobId < expected[j].JobId
	})
	assert.Equal(T, expected, updates)
}

func TestConflateJobRunUpdates(T *testing.T) {
	// Empty
	updates := conflateJobRunUpdates([]*model.UpdateJobRunInstruction{})
	assert.Equal(T, []*model.UpdateJobRunInstruction{}, updates)

	// Non-Empty
	updates = conflateJobRunUpdates([]*model.UpdateJobRunInstruction{
		{RunId: runIdString, Started: &baseTime},
		{RunId: runIdString, Node: pointer.String(nodeName)},
		{RunId: "someOtherJobRun", Started: &baseTime},
	})

	expected := []*model.UpdateJobRunInstruction{
		{RunId: runIdString, Started: &baseTime, Node: pointer.String(nodeName)},
		{RunId: "someOtherJobRun", Started: &baseTime},
	}

	sort.Slice(updates, func(i, j int) bool {
		return updates[i].RunId < updates[j].RunId
	})

	sort.Slice(expected, func(i, j int) bool {
		return expected[i].RunId < expected[j].RunId
	})
	assert.Equal(T, expected, updates)
}

func getJob(t *testing.T, db *pgxpool.Pool, jobId string) JobRow {
	job := JobRow{}
	r := db.QueryRow(
		context.Background(),
		`SELECT job_id, queue, owner, jobset, priority, submitted, state, duplicate, job_updated,  orig_job_spec, cancelled FROM job WHERE job_id = $1`,
		jobId)
	err := r.Scan(
		&job.JobId,
		&job.Queue,
		&job.Owner,
		&job.JobSet,
		&job.Priority,
		&job.Submitted,
		&job.State,
		&job.Duplicate,
		&job.Updated,
		&job.JobProto,
		&job.Cancelled,
	)
	require.NoError(t, err)
	return job
}

func getJobRun(t *testing.T, db *pgxpool.Pool, runId string) JobRunRow {
	run := JobRunRow{}
	r := db.QueryRow(
		context.Background(),
		`SELECT run_id, job_id, cluster, node, created, started, finished, succeeded, error, pod_number, unable_to_schedule, preempted  FROM job_run WHERE run_id = $1`,
		runId)
	err := r.Scan(
		&run.RunId,
		&run.JobId,
		&run.Cluster,
		&run.Node,
		&run.Created,
		&run.Started,
		&run.Finished,
		&run.Succeeded,
		&run.Error,
		&run.PodNumber,
		&run.UnableToSchedule,
		&run.Preempted,
	)
	require.NoError(t, err)
	return run
}

func getJobRunContainer(t *testing.T, db *pgxpool.Pool, runId string) JobRunContainerRow {
	container := JobRunContainerRow{}
	r := db.QueryRow(
		context.Background(),
		`SELECT run_id, container_name, exit_code FROM job_run_container WHERE run_id = $1`,
		runId)
	err := r.Scan(&container.RunId, &container.ContainerName, &container.ExitCode)
	require.NoError(t, err)
	return container
}

func getUserAnnotationLookup(t *testing.T, db *pgxpool.Pool, jobId string) UserAnnotationRow {
	annotation := UserAnnotationRow{}
	r := db.QueryRow(
		context.Background(),
		`SELECT job_id, key, value  FROM user_annotation_lookup WHERE job_id = $1`,
		jobId)
	err := r.Scan(&annotation.JobId, &annotation.Key, &annotation.Value)
	require.NoError(t, err)
	return annotation
}

func assertNoRows(t *testing.T, db *pgxpool.Pool, table string) {
	var count int
	r := db.QueryRow(context.Background(), fmt.Sprintf("SELECT COUNT(*) FROM %s", table))
	err := r.Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}
