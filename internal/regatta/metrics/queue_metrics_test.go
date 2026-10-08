package metrics

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestQueueMatcher(t *testing.T) {
	require.Equal(t, `queue="team-a"`, queueMatcher("queue", []string{"team-a"}), "one queue is an exact match")
	require.Equal(t, `queueName=~"team-a|team-b"`, queueMatcher("queueName", []string{"team-a", "team-b"}))
	require.Equal(t, `queue=~"team\\.a|team-b"`, queueMatcher("queue", []string{"team.a", "team-b"}),
		"regex metacharacters are escaped, so team.a does not also match teamXa")
}

func TestQueryBuildersUseTheRightLabelPerMetric(t *testing.T) {
	queues := []string{"a", "b"}
	require.Contains(t, queuedLatencyQuery(0.95, queues, "60s"), `queueName=~"a|b"`, "armada_job_* metrics use queueName")
	require.Contains(t, runLatencyQuery(0.5, queues, "60s"), `queueName=~"a|b"`)
	require.Contains(t, scheduledJobsQuery(queues, "60s"), `queue=~"a|b"`, "scheduler and queue metrics use queue")
	require.Contains(t, peakQueueSizeQuery(queues, "60s"), `queue=~"a|b",state="validated"`)
	require.Contains(t, peakLeasedPodCountQuery(queues, "60s"), `queue=~"a|b"`)
	require.Equal(t, `sum(armada_queue_size{queue=~"a|b",state="validated"})`, queueSizeQuery(queues))
	require.Equal(t, `sum(armada_queue_leased_pod_count{queue=~"a|b"})`, leasedPodCountQuery(queues))
	require.Equal(t, `sum(armada_queue_size{queue="a",state="validated"})`, queueSizeQuery([]string{"a"}))
}

// fakePrometheus records every request and answers with a fixed value.
type fakePrometheus struct {
	mu       sync.Mutex
	requests []recorded
	status   int
}

type recorded struct {
	method string
	query  string
	time   string
	ctype  string
	rawURL string
}

func (f *fakePrometheus) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	f.mu.Lock()
	f.requests = append(f.requests, recorded{r.Method, r.Form.Get("query"), r.Form.Get("time"), r.Header.Get("Content-Type"), r.URL.String()})
	f.mu.Unlock()
	if f.status != 0 {
		w.WriteHeader(f.status)
		return
	}
	_, _ = fmt.Fprint(w, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1,"42"]}]}}`)
}

func TestQuery_ShortExpressionsGoAsGET_LongOnesAsPOST(t *testing.T) {
	fake := &fakePrometheus{}
	server := httptest.NewServer(fake)
	defer server.Close()
	at := time.Unix(1700000000, 0)

	short := `sum(armada_queue_size{queue="a"})`
	val, err := query(context.Background(), server.URL, short, at)
	require.NoError(t, err)
	require.Equal(t, 42.0, *val)

	var names []string
	for i := 0; i < 1000; i++ {
		names = append(names, fmt.Sprintf("tenant-%04d", i+1))
	}
	long := queueSizeQuery(names)
	require.Greater(t, len(long), maxGetQueryLength)
	val, err = query(context.Background(), server.URL, long, at)
	require.NoError(t, err)
	require.Equal(t, 42.0, *val)

	require.Len(t, fake.requests, 2)
	require.Equal(t, http.MethodGet, fake.requests[0].method)
	require.Equal(t, short, fake.requests[0].query)

	post := fake.requests[1]
	require.Equal(t, http.MethodPost, post.method)
	require.Equal(t, "application/x-www-form-urlencoded", post.ctype)
	require.Equal(t, long, post.query, "the whole expression arrives intact in the form body")
	require.Equal(t, "1700000000", post.time)
	require.NotContains(t, post.rawURL, "tenant", "nothing from the expression is in the URL")
}

func TestQuery_ErrorMessagesDoNotDumpAHugeExpression(t *testing.T) {
	fake := &fakePrometheus{status: http.StatusBadRequest}
	server := httptest.NewServer(fake)
	defer server.Close()

	var names []string
	for i := 0; i < 1000; i++ {
		names = append(names, fmt.Sprintf("tenant-%04d", i+1))
	}
	_, err := query(context.Background(), server.URL, queueSizeQuery(names), time.Now())
	require.Error(t, err)
	require.Less(t, len(err.Error()), 600, "the error names the expression truncated")
	require.Contains(t, err.Error(), "bytes)")
}

func TestCollect_FiltersEveryPerQueueQueryToTheDeclaredQueues(t *testing.T) {
	fake := &fakePrometheus{}
	server := httptest.NewServer(fake)
	defer server.Close()

	now := time.Now()
	report, err := Collect(context.Background(), server.URL, []string{"team-a", "team-b"}, now.Add(-5*time.Minute), now)
	require.NoError(t, err)
	require.Equal(t, 2, report.QueueCount)
	require.Equal(t, 42.0, *report.EndToEndLatency.QueuedP95)
	require.Equal(t, 42.0, *report.QueueDepth.PeakLeased)
	require.Equal(t, 42.0, *report.APISurface.SubmitThroughput)
	require.Len(t, fake.requests, 20, "the query count does not depend on the number of queues")

	perQueue := 0
	for _, r := range fake.requests {
		if strings.Contains(r.query, "team-a|team-b") {
			perQueue++
		}
	}
	require.Equal(t, 9, perQueue, "latency x6, scheduled total, peak queue size, peak leased all name the queues")
}

func TestCollect_ErrorsOnlyWhenEveryQueryFails(t *testing.T) {
	bad := &fakePrometheus{status: http.StatusInternalServerError}
	server := httptest.NewServer(bad)
	defer server.Close()
	_, err := Collect(context.Background(), server.URL, []string{"a"}, time.Now().Add(-time.Minute), time.Now())
	require.ErrorContains(t, err, "every prometheus query failed")

	var calls int
	var mu sync.Mutex
	flaky := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls++
		n := calls
		mu.Unlock()
		if n%2 == 0 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, _ = fmt.Fprint(w, `{"data":{"result":[{"value":[1,"7"]}]}}`)
	}))
	defer flaky.Close()
	report, err := Collect(context.Background(), flaky.URL, []string{"a"}, time.Now().Add(-time.Minute), time.Now())
	require.NoError(t, err, "some queries failing just leaves those fields empty")
	require.NotNil(t, report)
}

func TestQuery_GivesUpOnAPrometheusThatNeverAnswers(t *testing.T) {
	previous := queryTimeout
	queryTimeout = 50 * time.Millisecond
	t.Cleanup(func() { queryTimeout = previous })
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-release }))
	t.Cleanup(func() { close(release); server.Close() })

	start := time.Now()
	_, err := query(context.Background(), server.URL, "up", time.Now())

	require.Error(t, err)
	require.Less(t, time.Since(start), 5*time.Second)
}

// drainPrometheus answers the instant drain queries with nothing left, and the windowed peaks with historyPeak.
func drainPrometheus(historyPeak string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		value := "0"
		if strings.HasPrefix(r.Form.Get("query"), "max_over_time") {
			value = historyPeak
		}
		_, _ = fmt.Fprintf(w, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1,%q]}]}}`, value)
	}))
}

func TestWaitForQueueDrain_JobsThatFinishedBeforeTheWaitBeganCountAsDrained(t *testing.T) {
	server := drainPrometheus("5") // the queues held jobs at some point since the run started
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	start := time.Now()
	WaitForQueueDrain(ctx, server.URL, []string{"q"}, time.Now().Add(-10*time.Minute))

	require.Less(t, time.Since(start), 2*time.Second, "it does not wait for activity that already happened")
	require.NoError(t, ctx.Err())
}

func TestWaitForQueueDrain_NoActivityAtAllIsNotMistakenForADrain(t *testing.T) {
	server := drainPrometheus("0") // nothing was ever seen, so the zero readings may just be a scrape gap
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 700*time.Millisecond)
	defer cancel()

	start := time.Now()
	WaitForQueueDrain(ctx, server.URL, []string{"q"}, time.Now().Add(-10*time.Minute))

	require.GreaterOrEqual(t, time.Since(start), 600*time.Millisecond, "it keeps waiting until the context ends")
}

func TestCollect_StatesTheLookbackItQueriedWith(t *testing.T) {
	end := time.Now()
	for name, tc := range map[string]struct {
		window       time.Duration
		wantWindow   float64
		wantLookback float64
		wantInQuery  string
	}{
		"a window under a minute is queried with a minute's lookback": {30 * time.Second, 30, 60, "[60s"},
		"a longer window is queried as it is":                         {5 * time.Minute, 300, 300, "[300s"},
	} {
		t.Run(name, func(t *testing.T) {
			fake := &fakePrometheus{}
			server := httptest.NewServer(fake)
			defer server.Close()
			start := end.Add(-tc.window)

			report, err := Collect(context.Background(), server.URL, []string{"q"}, start, end)

			require.NoError(t, err)
			require.True(t, report.Start.Equal(start), "the requested period is kept, so consecutive reports stay contiguous")
			require.True(t, report.End.Equal(end))
			require.Equal(t, tc.wantWindow, report.WindowSeconds)
			require.Equal(t, tc.wantLookback, report.LookbackSeconds)
			fake.mu.Lock()
			defer fake.mu.Unlock()
			queried := false
			for _, request := range fake.requests {
				if strings.Contains(request.query, tc.wantInQuery) {
					queried = true
				}
			}
			require.True(t, queried, "the queries use the lookback the report states")
		})
	}
}

func TestCounterQueriesCountGrowthExactlyAndJobLatencyStillUsesRate(t *testing.T) {
	queues := []string{"a", "b"}
	for name, expr := range map[string]string{
		"scheduled jobs":    scheduledJobsQuery(queues, "160s"),
		"submit throughput": submitThroughputQuery("160s", 160),
		"submit latency":    submitLatencyQuery(0.95, "160s"),
		"submit errors":     submitErrorsQuery("160s"),
		"schedule cycle":    scheduleCycleQuery(0.95, "160s"),
		"pulsar errors":     pulsarPublishErrorsQuery("160s"),
	} {
		require.NotContains(t, expr, "rate(", name+": a rate() misses a burst that falls between two samples")
		require.NotContains(t, expr, "increase(", name)
		require.Contains(t, expr, "offset 160s", name+": measured against the value at the window's start")
	}
	require.Contains(t, queuedLatencyQuery(0.95, queues, "160s"), "rate(", "series that vanish before the report need rate()")
	require.Contains(t, runLatencyQuery(0.95, queues, "160s"), "rate(")
	require.True(t, strings.HasSuffix(submitThroughputQuery("160s", 160), "/ 160"), "calls per second over the window")
	require.True(t, strings.HasSuffix(submitErrorsQuery("60s"), "or vector(0)"), "no errors reads as 0, not as a missing value")
}
