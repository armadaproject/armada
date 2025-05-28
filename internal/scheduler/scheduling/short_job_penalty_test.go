package scheduling

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
)

func TestNilSjpReturnsFalse(t *testing.T) {
	var nilSjp *ShortJobPenalty = nil
	job := shortTestJob(time.Now()).WithSucceeded(true)
	assert.False(t, nilSjp.ShouldApplyPenalty(job))
}

func TestTimeNotSetReturnsFalse(t *testing.T) {
	job := shortTestJob(time.Now()).WithSucceeded(true)
	assert.False(t, makeSut().ShouldApplyPenalty(job))
}

func TestLongSucceededJobReturnsFalse(t *testing.T) {
	now := time.Now()
	sut := makeSut()
	sut.SetNow(now)

	job := longTestJob(now).WithSucceeded(true)
	assert.False(t, sut.ShouldApplyPenalty(job))
}

func TestShortRunningJobReturnsFalse(t *testing.T) {
	sut := makeSut()
	now := time.Now()
	sut.SetNow(now)

	job := shortTestJob(now)
	assert.False(t, sut.ShouldApplyPenalty(job))
}

func TestShortSucceededJobReturnsTrue(t *testing.T) {
	sut := makeSut()
	now := time.Now()
	sut.SetNow(now)

	job := shortTestJob(now).WithSucceeded(true)
	assert.True(t, sut.ShouldApplyPenalty(job))
}

func TestShortPreemptedJobReturnsFalse(t *testing.T) {
	sut := makeSut()
	now := time.Now()
	sut.SetNow(now)

	job := shortTestJob(now).WithSucceeded(true)
	job = job.WithUpdatedRun(job.LatestRun().WithPreempted(true))
	assert.False(t, sut.ShouldApplyPenalty(job))
}

func TestShortJobWithPreemptRequestedReturnsFalse(t *testing.T) {
	sut := makeSut()
	now := time.Now()
	sut.SetNow(now)

	job := shortTestJob(now).WithSucceeded(true)
	job = job.WithUpdatedRun(job.LatestRun().WithPreemptRequested(true))
	assert.False(t, sut.ShouldApplyPenalty(job))
}

func TestShortJobWithPreemptedTimeSetReturnsFalse(t *testing.T) {
	sut := makeSut()
	now := time.Now()
	sut.SetNow(now)

	job := shortTestJob(now).WithSucceeded(true)
	job = job.WithUpdatedRun(job.LatestRun().WithPreemptedTime(&now))
	assert.False(t, sut.ShouldApplyPenalty(job))
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
