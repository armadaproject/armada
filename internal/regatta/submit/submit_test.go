package submit

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/regatta/config"
	"github.com/armadaproject/armada/internal/regatta/load"
	"github.com/armadaproject/armada/pkg/api"
)

func TestFromLoadConfig_PlansEachQueuesSteps(t *testing.T) {
	specA, specB := &v1.PodSpec{}, &v1.PodSpec{}
	l := config.Load{Queues: []config.QueueLoad{
		{
			Name: "team-a", PriorityFactor: 2, JobSetId: "a-run", Namespace: "ns",
			Jobs:     []config.JobRef{{Count: 60, ResolvedSpec: specA}, {Count: 40, ResolvedSpec: specB}},
			Schedule: load.Schedule{Duration: time.Minute, Step: 10 * time.Second},
		},
		{Name: "team-b", JobSetId: "b-run", Jobs: []config.JobRef{{Count: 5, ResolvedSpec: specA}}},
	}}

	spec, err := FromLoadConfig(l)
	require.NoError(t, err)

	require.Len(t, spec.Queues, 2)
	a, b := spec.Queues[0], spec.Queues[1]
	require.Equal(t, "team-a", a.Queue)
	require.Equal(t, 2.0, a.PriorityFactor)
	require.Equal(t, "a-run", a.JobSetId)
	require.Equal(t, "ns", a.Namespace)
	require.Equal(t, 100, a.TotalJobs())
	require.Len(t, a.Steps, 6, "a minute at 10s steps")
	total := 0
	for _, s := range a.Steps {
		total += s.Count
	}
	require.Equal(t, 100, total, "the steps sum to the queue's jobs")

	require.Equal(t, []load.Step{{Offset: 0, Count: 5}}, b.Steps, "no schedule means all at once")
	require.Equal(t, 105, spec.TotalJobs())
}

func TestFromLoadConfig_PropagatesScheduleErrors(t *testing.T) {
	l := config.Load{Queues: []config.QueueLoad{{
		Name: "q", Jobs: []config.JobRef{{Count: 1}},
		Schedule: load.Schedule{Duration: -1},
	}}}
	_, err := FromLoadConfig(l)
	require.ErrorContains(t, err, `queue "q"`)
}

func TestTakeAndFlatten(t *testing.T) {
	a, b := &v1.PodSpec{}, &v1.PodSpec{}
	jobs := []JobItem{{Spec: a, Count: 3}, {Spec: b, Count: 2}}

	flat := flattenJobItems(jobs)
	require.Len(t, flat, 5)

	first := take(flat, 4)
	require.Equal(t, []JobItem{{Spec: a, Count: 3}, {Spec: b, Count: 1}}, first, "spans the boundary, in list order")
	rest := take(flat[len(first)+2:], 10)
	require.Len(t, rest, 1, "asking for more than is left returns what is left")
	require.Nil(t, take(nil, 3))
}

// fakeQueueClient implements just the calls EnsureQueues makes.
type fakeQueueClient struct {
	api.SubmitClient
	mu       sync.Mutex
	existing map[string]float64 // queues that already exist, with their priorityFactor
	created  map[string]float64
	failOn   string
	inFlight atomic.Int32
	maxSeen  atomic.Int32
}

func (f *fakeQueueClient) CreateQueue(_ context.Context, q *api.Queue, _ ...grpc.CallOption) (*types.Empty, error) {
	n := f.inFlight.Add(1)
	defer f.inFlight.Add(-1)
	for {
		seen := f.maxSeen.Load()
		if n <= seen || f.maxSeen.CompareAndSwap(seen, n) {
			break
		}
	}
	time.Sleep(2 * time.Millisecond)

	f.mu.Lock()
	defer f.mu.Unlock()
	if q.Name == f.failOn {
		return nil, status.Error(codes.PermissionDenied, "nope")
	}
	if _, ok := f.existing[q.Name]; ok {
		return nil, status.Error(codes.AlreadyExists, "exists")
	}
	f.created[q.Name] = q.PriorityFactor
	return &types.Empty{}, nil
}

func (f *fakeQueueClient) GetQueue(_ context.Context, r *api.QueueGetRequest, _ ...grpc.CallOption) (*api.Queue, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	factor, ok := f.existing[r.Name]
	if !ok {
		return nil, status.Error(codes.NotFound, "no such queue")
	}
	return &api.Queue{Name: r.Name, PriorityFactor: factor}, nil
}

func newFake() *fakeQueueClient {
	return &fakeQueueClient{existing: map[string]float64{}, created: map[string]float64{}}
}

func TestEnsureQueues_CreatesMissingAndLeavesExistingAlone(t *testing.T) {
	f := newFake()
	f.existing["old-same"] = 1
	f.existing["old-different"] = 7

	err := ensureQueues(context.Background(), f, []QueueToEnsure{
		{Name: "new", PriorityFactor: 2},
		{Name: "old-same", PriorityFactor: 1},
		{Name: "old-different", PriorityFactor: 1},
	})
	require.NoError(t, err)

	require.Equal(t, map[string]float64{"new": 2}, f.created, "only the missing queue is created, with the declared factor")
	require.Equal(t, 7.0, f.existing["old-different"], "an existing queue is never updated")
}

func TestEnsureQueues_FailsOnAnyOtherError(t *testing.T) {
	f := newFake()
	f.failOn = "b"
	err := ensureQueues(context.Background(), f, []QueueToEnsure{{Name: "a", PriorityFactor: 1}, {Name: "b", PriorityFactor: 1}})
	require.ErrorContains(t, err, `queue "b"`)
	require.ErrorContains(t, err, "nope", "the server's message reaches the caller")
}

func TestEnsureQueues_IsBoundedAndHandlesAThousandQueues(t *testing.T) {
	f := newFake()
	queues := make([]QueueToEnsure, 1000)
	for i := range queues {
		queues[i] = QueueToEnsure{Name: fmt.Sprintf("tenant-%04d", i+1), PriorityFactor: 1}
	}
	require.NoError(t, ensureQueues(context.Background(), f, queues))
	require.Len(t, f.created, 1000)
	require.LessOrEqual(t, int(f.maxSeen.Load()), ensureConcurrency, "never more calls in flight than the cap")
}

func TestSummariseEnsure(t *testing.T) {
	outcomes := []ensureOutcome{{created: true}, {created: true}, {}}
	require.Equal(t, []string{"queues: 2 created, 1 already existed (left unchanged)"}, summariseEnsure(outcomes))

	var many []ensureOutcome
	for i := 0; i < 8; i++ {
		many = append(many, ensureOutcome{drift: "drift " + string(rune('a'+i))})
	}
	lines := summariseEnsure(many)
	require.Len(t, lines, 1+driftLinesToLog+1, "the summary, the first few drifts, and one line for the rest")
	require.Equal(t, "queues: 3 more existing queues have a different priorityFactor than declared", lines[len(lines)-1])
}

func TestFromLoadConfig_ContinuousQueue(t *testing.T) {
	spec := &v1.PodSpec{}
	l := config.Load{Queues: []config.QueueLoad{{
		Name: "steady-1", JobSetId: "js",
		Continuous: &config.ContinuousLoad{Step: time.Minute, Rates: []config.JobRate{{ResolvedSpec: spec, PerStep: 2.5}}},
	}}}

	got, err := FromLoadConfig(l)
	require.NoError(t, err)

	require.True(t, got.Continuous())
	q := got.Queues[0]
	require.Equal(t, time.Minute, q.Continuous.Step)
	require.Equal(t, []JobRate{{Spec: spec, PerStep: 2.5}}, q.Continuous.Rates)
	require.Empty(t, q.Steps, "a continuous queue has no fixed schedule")
	require.Equal(t, 0, got.TotalJobs(), "and no total")
}

// recordingSubmitter stands in for the server.
type recordingSubmitter struct {
	mu      sync.Mutex
	batches []recordedBatch
	fail    error
}

type recordedBatch struct {
	queue string
	jobs  int
	specs map[*v1.PodSpec]int
}

func (r *recordingSubmitter) submit(queue, _, _ string, jobs []JobItem) ([]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fail != nil {
		return nil, r.fail
	}
	b := recordedBatch{queue: queue, specs: map[*v1.PodSpec]int{}}
	var ids []string
	for _, j := range jobs {
		b.jobs += j.Count
		b.specs[j.Spec] += j.Count
		for i := 0; i < j.Count; i++ {
			ids = append(ids, "id")
		}
	}
	r.batches = append(r.batches, b)
	return ids, nil
}

func (r *recordingSubmitter) total() (jobs int, bySpec map[*v1.PodSpec]int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	bySpec = map[*v1.PodSpec]int{}
	for _, b := range r.batches {
		jobs += b.jobs
		for s, n := range b.specs {
			bySpec[s] += n
		}
	}
	return jobs, bySpec
}

func continuousQueue(name string, step time.Duration, rates ...JobRate) QueueSpec {
	return QueueSpec{Queue: name, JobSetId: "js-" + name, Continuous: &ContinuousSpec{Step: step, Rates: rates}}
}

func TestRunContinuous_SubmitsEveryStepUntilCancelledThenStopsCleanly(t *testing.T) {
	a, b := &v1.PodSpec{}, &v1.PodSpec{}
	rec := &recordingSubmitter{}
	q := continuousQueue("q", 10*time.Millisecond, JobRate{Spec: a, PerStep: 1}, JobRate{Spec: b, PerStep: 0.5})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	var submitted atomic.Int64
	go func() {
		done <- runContinuous(ctx, q, make(chan struct{}, 4), &submitted, rec.submit)
	}()
	time.Sleep(200 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		require.NoError(t, err, "stopping a continuous queue is not an error")
	case <-time.After(2 * time.Second):
		t.Fatal("runContinuous did not return after cancellation")
	}

	jobs, bySpec := rec.total()
	require.Greater(t, jobs, 10, "it kept submitting while running")
	require.Equal(t, jobs, int(submitted.Load()))
	// a is 1 per step and b is 0.5 per step, so a has about twice as many; the carry keeps the ratio.
	require.InDelta(t, 2.0, float64(bySpec[a])/float64(bySpec[b]), 0.4)
}

func TestRunContinuous_SubmitErrorEndsTheQueueWithThatError(t *testing.T) {
	rec := &recordingSubmitter{fail: errors.New("server down")}
	q := continuousQueue("q", 10*time.Millisecond, JobRate{Spec: &v1.PodSpec{}, PerStep: 1})
	var submitted atomic.Int64

	err := runContinuous(context.Background(), q, make(chan struct{}, 1), &submitted, rec.submit)
	require.ErrorContains(t, err, "server down")
	require.ErrorContains(t, err, `queue "q"`)
}

func TestRunContinuous_FractionalRatesSkipStepsThatHaveNoJob(t *testing.T) {
	rec := &recordingSubmitter{}
	q := continuousQueue("q", 10*time.Millisecond, JobRate{Spec: &v1.PodSpec{}, PerStep: 0.25})
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	var submitted atomic.Int64

	require.NoError(t, runContinuous(ctx, q, make(chan struct{}, 1), &submitted, rec.submit))
	jobs, _ := rec.total()
	require.Greater(t, jobs, 0)
	require.LessOrEqual(t, len(rec.batches), jobs, "no empty batches are sent")
	for _, b := range rec.batches {
		require.Greater(t, b.jobs, 0)
	}
}

func TestRun_StopNormallyWithContinuousQueuesBoundedCancellationOtherwise(t *testing.T) {
	t.Run("a continuous queue ends with the run", func(t *testing.T) {
		rec := &recordingSubmitter{}
		spec := &Spec{Queues: []QueueSpec{
			continuousQueue("steady", 10*time.Millisecond, JobRate{Spec: &v1.PodSpec{}, PerStep: 1}),
			{Queue: "bounded", Jobs: []JobItem{{Spec: &v1.PodSpec{}, Count: 3}}, Steps: []load.Step{{Offset: 0, Count: 3}}},
		}}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- run(ctx, spec, rec.submit) }()
		time.Sleep(100 * time.Millisecond)
		cancel()
		require.NoError(t, <-done)
		jobs, _ := rec.total()
		require.Greater(t, jobs, 3, "the bounded queue's three jobs and the continuous queue's")
	})

	t.Run("a bounded run cancelled part-way is an error", func(t *testing.T) {
		spec := &Spec{Queues: []QueueSpec{{
			Queue: "bounded", Jobs: []JobItem{{Spec: &v1.PodSpec{}, Count: 2}},
			Steps: []load.Step{{Offset: time.Hour, Count: 2}},
		}}}
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		require.ErrorIs(t, run(ctx, spec, (&recordingSubmitter{}).submit), context.DeadlineExceeded)
	})
}
