package scheduling

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
)

func TestNilSjpReturnsFalse(t *testing.T) {
	var nilSjp *ShortJobPenalty = nil
	job := shortTestJob(time.Now()).WithSucceeded(true)
	assert.False(t, nilSjp.shouldApplyPenalty(job))
}

func TestNilSjpIsSafeToCall(t *testing.T) {
	var nilSjp *ShortJobPenalty = nil
	job := shortTestJob(time.Now()).WithSucceeded(true)
	assert.NotPanics(t, func() {
		nilSjp.SetNow(time.Now())
		nilSjp.ReportFinishedJob(job)
	})
	assert.Nil(t, nilSjp.GetPenaltiesForPool(testfixtures.TestPool))
}

func TestTimeNotSetReturnsFalse(t *testing.T) {
	job := shortTestJob(time.Now()).WithSucceeded(true)
	assert.False(t, makeSut().shouldApplyPenalty(job))
}

func TestJobWithNoRunReturnsFalse(t *testing.T) {
	now := time.Now()
	sut := makeSut()
	sut.SetNow(now)

	job := testfixtures.Test32Cpu256GiJob("q", testfixtures.PriorityClass2).WithSucceeded(true)
	assert.False(t, sut.shouldApplyPenalty(job))
}

func TestJobWithNoRunningTimeReturnsFalse(t *testing.T) {
	now := time.Now()
	sut := makeSut()
	sut.SetNow(now)

	job := testfixtures.Test32Cpu256GiJob("q", testfixtures.PriorityClass2).
		WithNewRun("testExecutor", "test-node", "node", testfixtures.TestPool, 5).
		WithSucceeded(true)
	assert.Nil(t, job.LatestRun().RunningTime())
	assert.False(t, sut.shouldApplyPenalty(job))
}

func TestLongSucceededJobReturnsFalse(t *testing.T) {
	now := time.Now()
	sut := makeSut()
	sut.SetNow(now)

	job := longTestJob(now).WithSucceeded(true)
	assert.False(t, sut.shouldApplyPenalty(job))
}

func TestShortRunningJobReturnsFalse(t *testing.T) {
	sut := makeSut()
	now := time.Now()
	sut.SetNow(now)

	job := shortTestJob(now)
	assert.False(t, sut.shouldApplyPenalty(job))
}

func TestShortSucceededJobReturnsTrue(t *testing.T) {
	sut := makeSut()
	now := time.Now()
	sut.SetNow(now)

	job := shortTestJob(now).WithSucceeded(true)
	assert.True(t, sut.shouldApplyPenalty(job))
}

func TestShortCancelledJobReturnsTrue(t *testing.T) {
	now := time.Now()
	sut := makeSut()
	sut.SetNow(now)

	job := shortTestJob(now).WithCancelled(true)
	assert.True(t, sut.shouldApplyPenalty(job))
}

func TestShortFailedJobReturnsTrue(t *testing.T) {
	now := time.Now()
	sut := makeSut()
	sut.SetNow(now)

	job := shortTestJob(now).WithFailed(true)
	assert.True(t, sut.shouldApplyPenalty(job))
}

func TestShortPreemptedJobReturnsFalse(t *testing.T) {
	sut := makeSut()
	now := time.Now()
	sut.SetNow(now)

	job := shortTestJob(now).WithSucceeded(true)
	job = job.WithUpdatedRun(job.LatestRun().WithPreempted(true))
	assert.False(t, sut.shouldApplyPenalty(job))
}

func TestShortJobWithPreemptRequestedReturnsFalse(t *testing.T) {
	sut := makeSut()
	now := time.Now()
	sut.SetNow(now)

	job := shortTestJob(now).WithSucceeded(true)
	job = job.WithUpdatedRun(job.LatestRun().WithPreemptRequested(true))
	assert.False(t, sut.shouldApplyPenalty(job))
}

func TestShortJobWithPreemptedTimeSetReturnsFalse(t *testing.T) {
	sut := makeSut()
	now := time.Now()
	sut.SetNow(now)

	job := shortTestJob(now).WithSucceeded(true)
	job = job.WithUpdatedRun(job.LatestRun().WithPreemptedTime(&now))
	assert.False(t, sut.shouldApplyPenalty(job))
}

func makeSut() *ShortJobPenalty {
	return NewShortJobPenalty(map[string]time.Duration{testfixtures.TestPool: time.Minute})
}

func shortTestJob(now time.Time) *jobdb.Job {
	return testJob(now.Add(time.Second * -30))
}

func longTestJob(now time.Time) *jobdb.Job {
	return testJob(now.Add(-time.Hour))
}

func testJob(runningTime time.Time) *jobdb.Job {
	job := testfixtures.Test32Cpu256GiJob("q", testfixtures.PriorityClass2).WithNewRun("testExecutor", "test-node", "node", testfixtures.TestPool, 5)
	run := job.LatestRun()
	return job.WithUpdatedRun(run.WithRunningTime(&runningTime))
}

func TestAccumulatesPerQueue(t *testing.T) {
	now := time.Now()
	sut := makeSut()
	sut.SetNow(now)

	jobA := testJobForQueue("q1", now.Add(-30*time.Second)).WithSucceeded(true)
	jobB := testJobForQueue("q1", now.Add(-20*time.Second)).WithSucceeded(true)
	jobC := testJobForQueue("q2", now.Add(-20*time.Second)).WithSucceeded(true)

	sut.ReportFinishedJob(jobA)
	sut.ReportFinishedJob(jobB)
	sut.ReportFinishedJob(jobC)

	penalties := sut.GetPenaltiesForPool(testfixtures.TestPool)
	expectedTwoJobs := jobA.AllResourceRequirements().Add(jobB.AllResourceRequirements())
	assert.True(t, penalties["q1"].Equal(expectedTwoJobs))
	assert.True(t, penalties["q2"].Equal(jobC.AllResourceRequirements()))
}

func TestNonQualifyingJobIsNotCharged(t *testing.T) {
	now := time.Now()
	sut := makeSut()
	sut.SetNow(now)

	longJob := longTestJob(now).WithSucceeded(true)
	sut.ReportFinishedJob(longJob)

	penalties := sut.GetPenaltiesForPool(testfixtures.TestPool)
	assert.Empty(t, penalties)
}

func TestDedupSameJobReportedTwice(t *testing.T) {
	now := time.Now()
	sut := makeSut()
	sut.SetNow(now)

	job := testJobForQueue("q1", now.Add(-30*time.Second)).WithSucceeded(true)
	sut.ReportFinishedJob(job)
	sut.ReportFinishedJob(job)

	penalties := sut.GetPenaltiesForPool(testfixtures.TestPool)
	assert.True(t, penalties["q1"].Equal(job.AllResourceRequirements()))
}

func TestEntryExpiresExactlyAtDeadline(t *testing.T) {
	start := time.Now()
	sut := makeSut()
	job := testJobForQueue("q1", start).WithSucceeded(true)

	reportNow := start.Add(30 * time.Second)
	sut.SetNow(reportNow)
	sut.ReportFinishedJob(job)
	assert.True(t, sut.GetPenaltiesForPool(testfixtures.TestPool)["q1"].Equal(job.AllResourceRequirements()))

	atDeadline := start.Add(time.Minute)
	sut.SetNow(atDeadline)
	assert.Empty(t, sut.GetPenaltiesForPool(testfixtures.TestPool))
}

func TestPartialExpiryLeavesRemainder(t *testing.T) {
	start := time.Now()
	sut := makeSut()
	sut.SetNow(start)

	early := testJobForQueue("q1", start.Add(-50*time.Second)).WithSucceeded(true)
	late := testJobForQueue("q1", start.Add(-20*time.Second)).WithSucceeded(true)
	sut.ReportFinishedJob(early)
	sut.ReportFinishedJob(late)

	sut.SetNow(start.Add(20 * time.Second))
	penalties := sut.GetPenaltiesForPool(testfixtures.TestPool)
	assert.True(t, penalties["q1"].Equal(late.AllResourceRequirements()))
}

func TestPostExpiryReReportNeverReQualifies(t *testing.T) {
	start := time.Now()
	sut := makeSut()
	job := testJobForQueue("q1", start).WithSucceeded(true)

	sut.SetNow(start.Add(10 * time.Second))
	sut.ReportFinishedJob(job)
	sut.SetNow(start.Add(2 * time.Minute))
	assert.Empty(t, sut.GetPenaltiesForPool(testfixtures.TestPool))
	sut.ReportFinishedJob(job)
	assert.Empty(t, sut.GetPenaltiesForPool(testfixtures.TestPool))
}

func TestPerPoolCutoffAndPoolIsolation(t *testing.T) {
	now := time.Now()
	sut := NewShortJobPenalty(map[string]time.Duration{
		"poolA": time.Minute,
		"poolB": time.Hour,
	})
	sut.SetNow(now)

	jobA := testJobForPool("q1", "poolA", now.Add(-30*time.Minute)).WithSucceeded(true)
	jobB := testJobForPool("q1", "poolB", now.Add(-30*time.Minute)).WithSucceeded(true)
	jobC := testJobForPool("q1", "poolC", now.Add(-1*time.Second)).WithSucceeded(true)

	sut.ReportFinishedJob(jobA)
	sut.ReportFinishedJob(jobB)
	sut.ReportFinishedJob(jobC)

	assert.Empty(t, sut.GetPenaltiesForPool("poolA"))
	assert.True(t, sut.GetPenaltiesForPool("poolB")["q1"].Equal(jobB.AllResourceRequirements()))
	assert.Empty(t, sut.GetPenaltiesForPool("poolC"))
}

func TestGetPenaltiesForUnknownPoolIsEmpty(t *testing.T) {
	now := time.Now()
	sut := makeSut()
	sut.SetNow(now)
	assert.Empty(t, sut.GetPenaltiesForPool("does-not-exist"))
}

func TestShortJobPenalty_ConcurrentReportsAreCorrectAndRaceFree(t *testing.T) {
	now := time.Now()
	sut := makeSut()
	sut.SetNow(now)

	const numWorkers = 8
	const jobsPerWorker = 500
	runningTime := now.Add(-10 * time.Second)

	batches := make([][]*jobdb.Job, numWorkers)
	expected := internaltypes.ResourceList{}
	for w := range batches {
		batch := make([]*jobdb.Job, jobsPerWorker)
		for i := range batch {
			job := testJobForQueue("q1", runningTime).WithSucceeded(true)
			batch[i] = job
			expected = expected.Add(job.AllResourceRequirements())
		}
		batches[w] = batch
	}

	done := make(chan struct{})
	var readers sync.WaitGroup
	readers.Add(1)
	go func() {
		defer readers.Done()
		for {
			select {
			case <-done:
				return
			default:
				sut.GetPenaltiesForPool(testfixtures.TestPool)
			}
		}
	}()

	var writers sync.WaitGroup
	for _, batch := range batches {
		writers.Add(1)
		go func(batch []*jobdb.Job) {
			defer writers.Done()
			for _, job := range batch {
				sut.ReportFinishedJob(job)
			}
		}(batch)
	}
	writers.Wait()
	close(done)
	readers.Wait()

	penalties := sut.GetPenaltiesForPool(testfixtures.TestPool)
	assert.True(t, penalties["q1"].Equal(expected))
}

func testJobForQueue(queue string, runningTime time.Time) *jobdb.Job {
	return testJobForPool(queue, testfixtures.TestPool, runningTime)
}

func testJobForPool(queue string, pool string, runningTime time.Time) *jobdb.Job {
	job := testfixtures.Test32Cpu256GiJob(queue, testfixtures.PriorityClass2).WithNewRun("testExecutor", "test-node", "node", pool, 5)
	run := job.LatestRun()
	return job.WithUpdatedRun(run.WithRunningTime(&runningTime))
}
