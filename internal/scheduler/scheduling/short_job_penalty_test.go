package scheduling

import (
	"container/heap"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
)

func TestEntryHeapLess(t *testing.T) {
	base := time.Now()
	earlier := &penaltyEntry{deadline: base}
	later := &penaltyEntry{deadline: base.Add(time.Second)}

	tests := map[string]struct {
		h        entryHeap
		expected bool
	}{
		"earlier deadline is less than later":     {h: entryHeap{earlier, later}, expected: true},
		"later deadline is not less than earlier": {h: entryHeap{later, earlier}, expected: false},
		"equal deadlines are not less":            {h: entryHeap{earlier, earlier}, expected: false},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.h.Less(0, 1))
		})
	}
}

func TestEntryHeapPopsInDeadlineOrder(t *testing.T) {
	base := time.Now()
	offsets := []time.Duration{
		40 * time.Second,
		-10 * time.Second,
		0,
		90 * time.Second,
		10 * time.Second,
		-10 * time.Second,
		30 * time.Second,
	}

	h := &entryHeap{}
	for _, off := range offsets {
		heap.Push(h, &penaltyEntry{deadline: base.Add(off)})
	}

	sortedOffsets := slices.Clone(offsets)
	slices.Sort(sortedOffsets)

	var popped []time.Duration
	for h.Len() > 0 {
		assert.Equal(t, base.Add(sortedOffsets[len(popped)]), h.peek().deadline,
			"peek must always expose the earliest remaining deadline")
		e := heap.Pop(h).(*penaltyEntry)
		popped = append(popped, e.deadline.Sub(base))
	}

	assert.Equal(t, sortedOffsets, popped, "entries must pop in ascending deadline order")
}

func TestNilSjpIsSafeToCall(t *testing.T) {
	var nilSjp *ShortJobPenalty = nil
	job := shortTestJob(time.Now()).WithSucceeded(true)
	assert.NotPanics(t, func() {
		nilSjp.SetNow(time.Now())
		nilSjp.ReportFinishedJob(job)
	})
	assert.Nil(t, nilSjp.Snapshot().GetPenaltiesForPool(testfixtures.TestPool))
}

func TestShouldApplyPenalty(t *testing.T) {
	now := time.Now()

	withNow := func() *ShortJobPenalty {
		sut := makeSut()
		sut.SetNow(now)
		return sut
	}

	shortPreemptedJob := shortTestJob(now).WithSucceeded(true)
	shortPreemptedJob = shortPreemptedJob.WithUpdatedRun(shortPreemptedJob.LatestRun().WithPreempted(true))

	shortPreemptRequestedJob := shortTestJob(now).WithSucceeded(true)
	shortPreemptRequestedJob = shortPreemptRequestedJob.WithUpdatedRun(shortPreemptRequestedJob.LatestRun().WithPreemptRequested(true))

	shortPreemptedTimeJob := shortTestJob(now).WithSucceeded(true)
	shortPreemptedTimeJob = shortPreemptedTimeJob.WithUpdatedRun(shortPreemptedTimeJob.LatestRun().WithPreemptedTime(&now))

	tests := map[string]struct {
		sut      *ShortJobPenalty
		job      *jobdb.Job
		expected bool
	}{
		"nil penalty returns false": {
			sut:      nil,
			job:      shortTestJob(now).WithSucceeded(true),
			expected: false,
		},
		"now not set returns false": {
			sut:      makeSut(),
			job:      shortTestJob(now).WithSucceeded(true),
			expected: false,
		},
		"job with no run returns false": {
			sut:      withNow(),
			job:      testfixtures.Test32Cpu256GiJob("q", testfixtures.PriorityClass2).WithSucceeded(true),
			expected: false,
		},
		"job with no running time returns false": {
			sut:      withNow(),
			job:      testfixtures.Test32Cpu256GiJob("q", testfixtures.PriorityClass2).WithNewRun("testExecutor", "test-node", "node", testfixtures.TestPool, 5).WithSucceeded(true),
			expected: false,
		},
		"long succeeded job returns false": {
			sut:      withNow(),
			job:      longTestJob(now).WithSucceeded(true),
			expected: false,
		},
		"short running (non-terminal) job returns false": {
			sut:      withNow(),
			job:      shortTestJob(now),
			expected: false,
		},
		"short succeeded job returns true": {
			sut:      withNow(),
			job:      shortTestJob(now).WithSucceeded(true),
			expected: true,
		},
		"short cancelled job returns true": {
			sut:      withNow(),
			job:      shortTestJob(now).WithCancelled(true),
			expected: true,
		},
		"short failed job returns true": {
			sut:      withNow(),
			job:      shortTestJob(now).WithFailed(true),
			expected: true,
		},
		"short preempted job returns false": {
			sut:      withNow(),
			job:      shortPreemptedJob,
			expected: false,
		},
		"short job with preempt requested returns false": {
			sut:      withNow(),
			job:      shortPreemptRequestedJob,
			expected: false,
		},
		"short job with preempted time set returns false": {
			sut:      withNow(),
			job:      shortPreemptedTimeJob,
			expected: false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.sut.shouldApplyPenalty(tc.job))
		})
	}
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

type sjpStep func(t *testing.T, sut *ShortJobPenalty)

func setNow(now time.Time) sjpStep {
	return func(_ *testing.T, sut *ShortJobPenalty) { sut.SetNow(now) }
}

func report(job *jobdb.Job) sjpStep {
	return func(_ *testing.T, sut *ShortJobPenalty) { sut.ReportFinishedJob(job) }
}

func expectPenalty(pool, queue string, resources internaltypes.ResourceList) sjpStep {
	return func(t *testing.T, sut *ShortJobPenalty) {
		assert.True(t, sut.Snapshot().GetPenaltiesForPool(pool)[queue].Equal(resources))
	}
}

func expectPoolEmpty(pool string) sjpStep {
	return func(t *testing.T, sut *ShortJobPenalty) {
		assert.Empty(t, sut.Snapshot().GetPenaltiesForPool(pool))
	}
}

func TestPenaltyAccounting(t *testing.T) {
	now := time.Now()

	accumA := testJobForQueue("q1", now.Add(-30*time.Second)).WithSucceeded(true)
	accumB := testJobForQueue("q1", now.Add(-20*time.Second)).WithSucceeded(true)
	accumC := testJobForQueue("q2", now.Add(-20*time.Second)).WithSucceeded(true)

	dedupJob := testJobForQueue("q1", now.Add(-30*time.Second)).WithSucceeded(true)

	expiringJob := testJobForQueue("q1", now).WithSucceeded(true)

	early := testJobForQueue("q1", now.Add(-50*time.Second)).WithSucceeded(true)
	late := testJobForQueue("q1", now.Add(-20*time.Second)).WithSucceeded(true)

	reReportJob := testJobForQueue("q1", now).WithSucceeded(true)

	poolAJob := testJobForPool("q1", "poolA", now.Add(-30*time.Minute)).WithSucceeded(true)
	poolBJob := testJobForPool("q1", "poolB", now.Add(-30*time.Minute)).WithSucceeded(true)
	poolCJob := testJobForPool("q1", "poolC", now.Add(-1*time.Second)).WithSucceeded(true)

	tests := map[string]struct {
		newSut func() *ShortJobPenalty
		steps  []sjpStep
	}{
		"accumulates per queue": {
			steps: []sjpStep{
				setNow(now),
				report(accumA),
				report(accumB),
				report(accumC),
				expectPenalty(testfixtures.TestPool, "q1", accumA.AllResourceRequirements().Add(accumB.AllResourceRequirements())),
				expectPenalty(testfixtures.TestPool, "q2", accumC.AllResourceRequirements()),
			},
		},
		"non-qualifying job is not charged": {
			steps: []sjpStep{
				setNow(now),
				report(longTestJob(now).WithSucceeded(true)),
				expectPoolEmpty(testfixtures.TestPool),
			},
		},
		"dedup same job reported twice": {
			steps: []sjpStep{
				setNow(now),
				report(dedupJob),
				report(dedupJob),
				expectPenalty(testfixtures.TestPool, "q1", dedupJob.AllResourceRequirements()),
			},
		},
		"entry expires exactly at deadline": {
			steps: []sjpStep{
				setNow(now.Add(30 * time.Second)),
				report(expiringJob),
				expectPenalty(testfixtures.TestPool, "q1", expiringJob.AllResourceRequirements()),
				setNow(now.Add(time.Minute)),
				expectPoolEmpty(testfixtures.TestPool),
			},
		},
		"partial expiry leaves remainder": {
			steps: []sjpStep{
				setNow(now),
				report(early),
				report(late),
				setNow(now.Add(20 * time.Second)),
				expectPenalty(testfixtures.TestPool, "q1", late.AllResourceRequirements()),
			},
		},
		"post expiry re-report never re-qualifies": {
			steps: []sjpStep{
				setNow(now.Add(10 * time.Second)),
				report(reReportJob),
				setNow(now.Add(2 * time.Minute)),
				expectPoolEmpty(testfixtures.TestPool),
				report(reReportJob),
				expectPoolEmpty(testfixtures.TestPool),
			},
		},
		"per-pool cutoff and pool isolation": {
			newSut: func() *ShortJobPenalty {
				return NewShortJobPenalty(map[string]time.Duration{
					"poolA": time.Minute,
					"poolB": time.Hour,
				})
			},
			steps: []sjpStep{
				setNow(now),
				report(poolAJob),
				report(poolBJob),
				report(poolCJob),
				expectPoolEmpty("poolA"),
				expectPenalty("poolB", "q1", poolBJob.AllResourceRequirements()),
				expectPoolEmpty("poolC"),
			},
		},
		"penalties for unknown pool is empty": {
			steps: []sjpStep{
				setNow(now),
				expectPoolEmpty("does-not-exist"),
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			newSut := tc.newSut
			if newSut == nil {
				newSut = makeSut
			}
			sut := newSut()
			for _, step := range tc.steps {
				step(t, sut)
			}
		})
	}
}

func TestShortJobPenalty_ConcurrentReportsAreCorrect(t *testing.T) {
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

	penalties := sut.Snapshot().GetPenaltiesForPool(testfixtures.TestPool)
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
