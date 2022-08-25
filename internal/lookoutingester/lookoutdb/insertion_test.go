package lookoutdb

import (
	ctx "context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/G-Research/armada/internal/lookout/repository"

	"github.com/G-Research/armada/internal/pulsarutils"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/assert"
	"k8s.io/utils/pointer"

	"github.com/G-Research/armada/internal/lookout/testutil"
	"github.com/G-Research/armada/internal/lookoutingester/model"
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
	baseTime, _     = time.Parse("2006-01-02T15:04:05.000Z", "2022-03-01T15:04:05.000Z")
	updateTime, _   = time.Parse("2006-01-02T15:04:05.000Z", "2022-03-01T15:04:06.000Z")
	startTime, _    = time.Parse("2006-01-02T15:04:05.000Z", "2022-03-01T15:04:07.000Z")
	finishedTime, _ = time.Parse("2006-01-02T15:04:05.000Z", "2022-03-01T15:04:08.000Z")
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
	JobJson   []byte
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
			JobJson:   []byte(jobJson),
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
		MessageIds: []*pulsarutils.ConsumerMessageId{{MessageId: pulsarutils.NewMessageId(3), Index: 0, ConsumerId: 1}},
	}
}

var expectedJobAfterSubmit = JobRow{
	JobId:     jobIdString,
	Queue:     queue,
	Owner:     userId,
	JobSet:    jobSetName,
	Priority:  priority,
	Submitted: baseTime,
	JobJson:   []byte(jobJson),
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
	JobJson:   []byte(jobJson),
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

func TestCreateJobsBatch(t *testing.T) {
	err := testutil.WithDatabasePgx(func(db *pgxpool.Pool) error {
		// Insert
		err := CreateJobsBatch(ctx.Background(), db, defaultInstructionSet().JobsToCreate)
		assert.Nil(t, err)
		job := getJob(t, db, jobIdString)
		assert.Equal(t, expectedJobAfterSubmit, job)

		// Insert again and test that it's idempotent
		err = CreateJobsBatch(ctx.Background(), db, defaultInstructionSet().JobsToCreate)
		assert.Nil(t, err)
		job = getJob(t, db, jobIdString)
		assert.Equal(t, expectedJobAfterSubmit, job)

		// If a row is bad then we should return an error and no updates should happen
		_, err = db.Exec(ctx.Background(), "DELETE FROM job")
		assert.NoError(t, err)
		invalidJob := &model.CreateJobInstruction{
			JobId: invalidId,
		}
		err = CreateJobsBatch(ctx.Background(), db, append(defaultInstructionSet().JobsToCreate, invalidJob))
		assert.Error(t, err)
		assertNoRows(t, db, "job")
		return nil
	})
	assert.NoError(t, err)
}

func TestUpdateJobsBatch(t *testing.T) {
	err := testutil.WithDatabasePgx(func(db *pgxpool.Pool) error {
		// Insert
		err := CreateJobsBatch(ctx.Background(), db, defaultInstructionSet().JobsToCreate)
		assert.Nil(t, err)

		// Update
		err = UpdateJobsBatch(ctx.Background(), db, defaultInstructionSet().JobsToUpdate)
		assert.Nil(t, err)
		job := getJob(t, db, jobIdString)
		assert.Equal(t, expectedJobAfterUpdate, job)

		err = UpdateJobsBatch(ctx.Background(), db, defaultInstructionSet().JobsToUpdate)
		assert.Nil(t, err)
		job = getJob(t, db, jobIdString)
		assert.Equal(t, expectedJobAfterUpdate, job)

		// If a update is bad then we should return an error and no updates should happen
		_, err = db.Exec(ctx.Background(), "DELETE FROM job")
		assert.NoError(t, err)
		err = CreateJobsBatch(ctx.Background(), db, defaultInstructionSet().JobsToCreate)
		assert.Nil(t, err)
		invalidUpdate := &model.UpdateJobInstruction{
			JobId: invalidId,
		}
		err = UpdateJobsBatch(ctx.Background(), db, append(defaultInstructionSet().JobsToUpdate, invalidUpdate))
		assert.Error(t, err)
		job = getJob(t, db, jobIdString)
		assert.Equal(t, expectedJobAfterSubmit, job)
		return nil
	})
	assert.NoError(t, err)
}

func TestUpdateJobsScalar(t *testing.T) {
	err := testutil.WithDatabasePgx(func(db *pgxpool.Pool) error {
		// Insert
		err := CreateJobsBatch(ctx.Background(), db, defaultInstructionSet().JobsToCreate)
		assert.Nil(t, err)

		// Update
		UpdateJobsScalar(ctx.Background(), db, defaultInstructionSet().JobsToUpdate)
		job := getJob(t, db, jobIdString)
		assert.Equal(t, expectedJobAfterUpdate, job)

		// Insert again and test that it's idempotent
		UpdateJobsScalar(ctx.Background(), db, defaultInstructionSet().JobsToUpdate)
		job = getJob(t, db, jobIdString)
		assert.Equal(t, expectedJobAfterUpdate, job)

		// If a update is bad then we should return an error and no updates should happen
		_, err = db.Exec(ctx.Background(), "DELETE FROM job")
		assert.NoError(t, err)
		err = CreateJobsBatch(ctx.Background(), db, defaultInstructionSet().JobsToCreate)
		assert.Nil(t, err)
		invalidUpdate := &model.UpdateJobInstruction{
			JobId: invalidId,
		}
		UpdateJobsScalar(ctx.Background(), db, append(defaultInstructionSet().JobsToUpdate, invalidUpdate))
		job = getJob(t, db, jobIdString)
		assert.Equal(t, expectedJobAfterUpdate, job)
		return nil
	})
	assert.NoError(t, err)
}

func TestUpdateJobsWithCancelled(t *testing.T) {
	err := testutil.WithDatabasePgx(func(db *pgxpool.Pool) error {
		initial := []*model.CreateJobInstruction{{
			JobId:     jobIdString,
			Queue:     queue,
			Owner:     userId,
			JobSet:    jobSetName,
			Priority:  priority,
			Submitted: baseTime,
			JobJson:   []byte(jobJson),
			JobProto:  []byte(jobProto),
			State:     0,
			Updated:   baseTime,
		}}

		update1 := []*model.UpdateJobInstruction{{
			JobId:   jobIdString,
			State:   pointer.Int32(repository.JobCancelledOrdinal),
			Updated: baseTime,
		}}

		update2 := []*model.UpdateJobInstruction{{
			JobId:   jobIdString,
			State:   pointer.Int32(repository.JobRunningOrdinal),
			Updated: baseTime,
		}}

		// Insert
		CreateJobs(ctx.Background(), db, initial)

		// Cancel the job
		UpdateJobs(ctx.Background(), db, update1)

		// Update the job- this should be discarded
		UpdateJobs(ctx.Background(), db, update2)

		// Assert the state is still cancelled
		job := getJob(t, db, jobIdString)
		assert.Equal(t, repository.JobCancelledOrdinal, int(job.State))

		return nil
	})
	assert.NoError(t, err)
}

func TestCreateJobsScalar(t *testing.T) {
	err := testutil.WithDatabasePgx(func(db *pgxpool.Pool) error {
		// Simple create
		CreateJobsScalar(ctx.Background(), db, defaultInstructionSet().JobsToCreate)
		job := getJob(t, db, jobIdString)
		assert.Equal(t, expectedJobAfterSubmit, job)

		// Insert again and check for idempotency
		CreateJobsScalar(ctx.Background(), db, defaultInstructionSet().JobsToCreate)
		job = getJob(t, db, jobIdString)
		assert.Equal(t, expectedJobAfterSubmit, job)

		// If a row is bad then we should update only the good rows
		_, err := db.Exec(ctx.Background(), "DELETE FROM job")
		assert.NoError(t, err)
		invalidJob := &model.CreateJobInstruction{
			JobId: invalidId,
		}
		CreateJobsScalar(ctx.Background(), db, append(defaultInstructionSet().JobsToCreate, invalidJob))
		job = getJob(t, db, jobIdString)
		assert.Equal(t, expectedJobAfterSubmit, job)
		return nil
	})
	assert.NoError(t, err)
}

func TestCreateJobRunsBatch(t *testing.T) {
	err := testutil.WithDatabasePgx(func(db *pgxpool.Pool) error {
		// Need to make sure we have a job so we can satisfy PK
		err := CreateJobsBatch(ctx.Background(), db, defaultInstructionSet().JobsToCreate)
		assert.Nil(t, err)

		// Insert
		err = CreateJobRunsBatch(ctx.Background(), db, defaultInstructionSet().JobRunsToCreate)
		assert.Nil(t, err)
		job := getJobRun(t, db, runIdString)
		assert.Equal(t, expectedJobRun, job)

		// Insert again and test that it's idempotent
		err = CreateJobRunsBatch(ctx.Background(), db, defaultInstructionSet().JobRunsToCreate)
		assert.Nil(t, err)
		job = getJobRun(t, db, runIdString)
		assert.Equal(t, expectedJobRun, job)

		// If a row is bad then we should return an error and no updates should happen
		_, err = db.Exec(ctx.Background(), "DELETE FROM job_run")
		assert.NoError(t, err)
		invalidRun := &model.CreateJobRunInstruction{
			RunId: invalidId,
		}
		err = CreateJobRunsBatch(ctx.Background(), db, append(defaultInstructionSet().JobRunsToCreate, invalidRun))
		assert.Error(t, err)
		assertNoRows(t, db, "job_run")
		return nil
	})
	assert.NoError(t, err)
}

func TestCreateJobRunsScalar(t *testing.T) {
	err := testutil.WithDatabasePgx(func(db *pgxpool.Pool) error {
		// Need to make sure we have a job so we can satisfy PK
		err := CreateJobsBatch(ctx.Background(), db, defaultInstructionSet().JobsToCreate)
		assert.Nil(t, err)

		// Insert
		CreateJobRunsScalar(ctx.Background(), db, defaultInstructionSet().JobRunsToCreate)
		job := getJobRun(t, db, runIdString)
		assert.Equal(t, expectedJobRun, job)

		// Insert again and test that it's idempotent
		CreateJobRunsScalar(ctx.Background(), db, defaultInstructionSet().JobRunsToCreate)
		job = getJobRun(t, db, runIdString)
		assert.Equal(t, expectedJobRun, job)

		// If a row is bad then we create rows that can be created
		_, err = db.Exec(ctx.Background(), "DELETE FROM job_run")
		assert.NoError(t, err)
		invalidRun := &model.CreateJobRunInstruction{
			RunId: invalidId,
		}
		CreateJobRunsScalar(ctx.Background(), db, append(defaultInstructionSet().JobRunsToCreate, invalidRun))
		job = getJobRun(t, db, runIdString)
		assert.Equal(t, expectedJobRun, job)
		return nil
	})
	assert.NoError(t, err)
}

func TestUpdateJobRunsBatch(t *testing.T) {
	err := testutil.WithDatabasePgx(func(db *pgxpool.Pool) error {
		// Need to make sure we have a job and run
		err := CreateJobsBatch(ctx.Background(), db, defaultInstructionSet().JobsToCreate)
		assert.Nil(t, err)

		err = CreateJobRunsBatch(ctx.Background(), db, defaultInstructionSet().JobRunsToCreate)
		assert.Nil(t, err)

		// Update
		err = UpdateJobRunsBatch(ctx.Background(), db, defaultInstructionSet().JobRunsToUpdate)
		assert.Nil(t, err)
		run := getJobRun(t, db, runIdString)
		assert.Equal(t, expectedJobRunAfterUpdate, run)

		// Update again and test that it's idempotent
		err = UpdateJobRunsBatch(ctx.Background(), db, defaultInstructionSet().JobRunsToUpdate)
		assert.Nil(t, err)
		run = getJobRun(t, db, runIdString)
		assert.Equal(t, expectedJobRunAfterUpdate, run)

		// If a row is bad then we should return an error and no updates should happen
		_, err = db.Exec(ctx.Background(), "DELETE FROM job_run;")
		assert.Nil(t, err)
		invalidRun := &model.UpdateJobRunInstruction{
			RunId: invalidId,
		}
		err = CreateJobRunsBatch(ctx.Background(), db, defaultInstructionSet().JobRunsToCreate)
		assert.Nil(t, err)
		err = UpdateJobRunsBatch(ctx.Background(), db, append(defaultInstructionSet().JobRunsToUpdate, invalidRun))
		assert.Error(t, err)
		run = getJobRun(t, db, runIdString)
		assert.Equal(t, expectedJobRun, run)
		return nil
	})
	assert.NoError(t, err)
}

func TestUpdateJobRunsScalar(t *testing.T) {
	err := testutil.WithDatabasePgx(func(db *pgxpool.Pool) error {
		// Need to make sure we have a job and run
		err := CreateJobsBatch(ctx.Background(), db, defaultInstructionSet().JobsToCreate)
		assert.Nil(t, err)

		err = CreateJobRunsBatch(ctx.Background(), db, defaultInstructionSet().JobRunsToCreate)
		assert.Nil(t, err)

		// Update
		UpdateJobRunsScalar(ctx.Background(), db, defaultInstructionSet().JobRunsToUpdate)
		assert.Nil(t, err)
		run := getJobRun(t, db, runIdString)
		assert.Equal(t, expectedJobRunAfterUpdate, run)

		// Update again and test that it's idempotent
		UpdateJobRunsScalar(ctx.Background(), db, defaultInstructionSet().JobRunsToUpdate)
		assert.Nil(t, err)
		run = getJobRun(t, db, runIdString)
		assert.Equal(t, expectedJobRunAfterUpdate, run)

		// If a row is bad then we should update the rows we can
		_, err = db.Exec(ctx.Background(), "DELETE FROM job_run;")
		assert.Nil(t, err)
		invalidRun := &model.UpdateJobRunInstruction{
			RunId: invalidId,
		}
		err = CreateJobRunsBatch(ctx.Background(), db, defaultInstructionSet().JobRunsToCreate)
		assert.Nil(t, err)
		UpdateJobRunsScalar(ctx.Background(), db, append(defaultInstructionSet().JobRunsToUpdate, invalidRun))
		run = getJobRun(t, db, runIdString)
		assert.Equal(t, expectedJobRunAfterUpdate, run)
		return nil
	})
	assert.NoError(t, err)
}

func TestCreateUserAnnotationsBatch(t *testing.T) {
	err := testutil.WithDatabasePgx(func(db *pgxpool.Pool) error {
		// Need to make sure we have a job
		err := CreateJobsBatch(ctx.Background(), db, defaultInstructionSet().JobsToCreate)
		assert.Nil(t, err)

		// Insert
		err = CreateUserAnnotationsBatch(ctx.Background(), db, defaultInstructionSet().UserAnnotationsToCreate)
		assert.Nil(t, err)
		annotation := getUserAnnotationLookup(t, db, jobIdString)
		assert.Equal(t, expectedUserAnnotation, annotation)

		// Insert again and test that it's idempotent
		err = CreateUserAnnotationsBatch(ctx.Background(), db, defaultInstructionSet().UserAnnotationsToCreate)
		assert.Nil(t, err)
		annotation = getUserAnnotationLookup(t, db, jobIdString)
		assert.Equal(t, expectedUserAnnotation, annotation)

		// If a row is bad then we should return an error and no updates should happen
		_, err = db.Exec(ctx.Background(), "DELETE FROM user_annotation_lookup")
		assert.NoError(t, err)
		invalidAnnotation := &model.CreateUserAnnotationInstruction{
			JobId: invalidId,
		}
		err = CreateUserAnnotationsBatch(ctx.Background(), db, append(defaultInstructionSet().UserAnnotationsToCreate, invalidAnnotation))
		assert.Error(t, err)
		assertNoRows(t, db, "user_annotation_lookup")
		return nil
	})
	assert.NoError(t, err)
}

func TestEmptyUpdate(t *testing.T) {
	err := testutil.WithDatabasePgx(func(db *pgxpool.Pool) error {
		Update(ctx.Background(), db, &model.InstructionSet{})
		assertNoRows(t, db, "job")
		assertNoRows(t, db, "job_run")
		assertNoRows(t, db, "user_annotation_lookup")
		assertNoRows(t, db, "job_run_container")
		return nil
	})
	assert.NoError(t, err)
}

func TestCreateUserAnnotationsScalar(t *testing.T) {
	err := testutil.WithDatabasePgx(func(db *pgxpool.Pool) error {
		// Need to make sure we have a job
		err := CreateJobsBatch(ctx.Background(), db, defaultInstructionSet().JobsToCreate)
		assert.Nil(t, err)

		// Insert
		CreateUserAnnotationsScalar(ctx.Background(), db, defaultInstructionSet().UserAnnotationsToCreate)
		annotation := getUserAnnotationLookup(t, db, jobIdString)
		assert.Equal(t, expectedUserAnnotation, annotation)

		// Insert again and test that it's idempotent
		CreateUserAnnotationsScalar(ctx.Background(), db, defaultInstructionSet().UserAnnotationsToCreate)
		annotation = getUserAnnotationLookup(t, db, jobIdString)
		assert.Equal(t, expectedUserAnnotation, annotation)

		// If a row is bad then we should update the rows we can
		_, err = db.Exec(ctx.Background(), "DELETE FROM user_annotation_lookup")
		assert.NoError(t, err)
		invalidAnnotation := &model.CreateUserAnnotationInstruction{
			JobId: invalidId,
		}
		CreateUserAnnotationsScalar(ctx.Background(), db, append(defaultInstructionSet().UserAnnotationsToCreate, invalidAnnotation))
		annotation = getUserAnnotationLookup(t, db, jobIdString)
		assert.Equal(t, expectedUserAnnotation, annotation)
		return nil
	})
	assert.NoError(t, err)
}

func TestUpdate(t *testing.T) {
	err := testutil.WithDatabasePgx(func(db *pgxpool.Pool) error {
		// Do the update
		Update(ctx.Background(), db, defaultInstructionSet())

		job := getJob(t, db, jobIdString)
		jobRun := getJobRun(t, db, runIdString)
		annotation := getUserAnnotationLookup(t, db, jobIdString)
		container := getJobRunContainer(t, db, runIdString)

		assert.Equal(t, expectedJobAfterUpdate, job)
		assert.Equal(t, expectedJobRunAfterUpdate, jobRun)
		assert.Equal(t, expectedJobRunContainer, container)
		assert.Equal(t, expectedUserAnnotation, annotation)
		return nil
	})
	assert.NoError(t, err)
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

func TestConflateJobUpdatesWithCancelled(T *testing.T) {
	// Updates after the cancelled shouldn't be processed
	updates := conflateJobUpdates([]*model.UpdateJobInstruction{
		{JobId: jobIdString, State: pointer.Int32(repository.JobCancelledOrdinal)},
		{JobId: jobIdString, State: pointer.Int32(repository.JobRunningOrdinal)},
	})

	expected := []*model.UpdateJobInstruction{
		{JobId: jobIdString, State: pointer.Int32(repository.JobCancelledOrdinal)},
	}
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
		ctx.Background(),
		`SELECT job_id, queue, owner, jobset, priority, submitted, state, duplicate, job_updated, job, orig_job_spec, cancelled FROM job WHERE job_id = $1`,
		jobId)
	err := r.Scan(&job.JobId, &job.Queue, &job.Owner, &job.JobSet, &job.Priority, &job.Submitted, &job.State, &job.Duplicate, &job.Updated, &job.JobJson, &job.JobProto, &job.Cancelled)
	assert.Nil(t, err)
	return job
}

func getJobRun(t *testing.T, db *pgxpool.Pool, runId string) JobRunRow {
	run := JobRunRow{}
	r := db.QueryRow(
		ctx.Background(),
		`SELECT run_id, job_id, cluster, node, created, started, finished, succeeded, error, pod_number, unable_to_schedule FROM job_run WHERE run_id = $1`,
		runId)
	err := r.Scan(&run.RunId, &run.JobId, &run.Cluster, &run.Node, &run.Created, &run.Started, &run.Finished, &run.Succeeded, &run.Error, &run.PodNumber, &run.UnableToSchedule)
	assert.Nil(t, err)
	return run
}

func getJobRunContainer(t *testing.T, db *pgxpool.Pool, runId string) JobRunContainerRow {
	container := JobRunContainerRow{}
	r := db.QueryRow(
		ctx.Background(),
		`SELECT run_id, container_name, exit_code FROM job_run_container WHERE run_id = $1`,
		runId)
	err := r.Scan(&container.RunId, &container.ContainerName, &container.ExitCode)
	assert.Nil(t, err)
	return container
}

func getUserAnnotationLookup(t *testing.T, db *pgxpool.Pool, jobId string) UserAnnotationRow {
	annotation := UserAnnotationRow{}
	r := db.QueryRow(
		ctx.Background(),
		`SELECT job_id, key, value  FROM user_annotation_lookup WHERE job_id = $1`,
		jobId)
	err := r.Scan(&annotation.JobId, &annotation.Key, &annotation.Value)
	assert.Nil(t, err)
	return annotation
}

func assertNoRows(t *testing.T, db *pgxpool.Pool, table string) {
	var count int
	r := db.QueryRow(ctx.Background(), fmt.Sprintf("SELECT COUNT(*) FROM %s", table))
	err := r.Scan(&count)
	assert.Nil(t, err)
	assert.Equal(t, 0, count)
}
