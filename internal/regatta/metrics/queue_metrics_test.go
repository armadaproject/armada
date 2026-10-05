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
