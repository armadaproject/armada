package scheduler

import (
	"context"
	"crypto/sha1"
	"encoding/json"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/mock/gomock"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/clock"
	"k8s.io/utils/pointer"
	"testing"
	"time"

	commonmetrics "github.com/armadaproject/armada/internal/common/metrics"
	"github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	schedulermocks "github.com/armadaproject/armada/internal/scheduler/mocks"
)

func TestMetricsCollector_TestCollect(t *testing.T) {
	gb := float64(1024 * 1024 * 1024)
	queuedJobs := make([]*jobdb.Job, 3)
	runningJobs := make([]*jobdb.Job, 3)
	for i := 0; i < len(queuedJobs); i++ {
		startTime := baseTime.Add(-time.Duration(100*i) * time.Second).UnixNano()
		queuedJobs[i] = testQueuedJobDbJob().WithCreated(startTime)
		runningJobs[i] = testRunningJobDbJob(startTime)
	}

	tests := map[string]struct {
		initialJobs  []*jobdb.Job
		defaultPool  string
		poolMappings map[string]string
		queues       []*database.Queue
		expected     []prometheus.Metric
	}{
		"queued metrics": {
			initialJobs: queuedJobs,
			queues:      []*database.Queue{testDbQueue()},
			defaultPool: testPool,
			expected: []prometheus.Metric{
				commonmetrics.NewQueueSizeMetric(3.0, testQueue),
				commonmetrics.NewQueueDuration(3, 300,
					map[float64]uint64{60: 1, 600: 3, 1800: 3, 3600: 3, 10800: 3, 43200: 3, 86400: 3, 172800: 3, 604800: 3},
					testPool, testDefaultPriorityClass, testQueue),
				commonmetrics.NewMinQueueDuration(0, testPool, testDefaultPriorityClass, testQueue),
				commonmetrics.NewMaxQueueDuration(200, testPool, testDefaultPriorityClass, testQueue),
				commonmetrics.NewMedianQueueDuration(100, testPool, testDefaultPriorityClass, testQueue),
				commonmetrics.NewQueueResources(3, testPool, testDefaultPriorityClass, testQueue, "cpu"),
				commonmetrics.NewMinQueueResources(1, testPool, testDefaultPriorityClass, testQueue, "cpu"),
				commonmetrics.NewMaxQueueResources(1, testPool, testDefaultPriorityClass, testQueue, "cpu"),
				commonmetrics.NewMedianQueueResources(1, testPool, testDefaultPriorityClass, testQueue, "cpu"),
				commonmetrics.NewCountQueueResources(3, testPool, testDefaultPriorityClass, testQueue, "cpu"),
				commonmetrics.NewQueueResources(3*gb, testPool, testDefaultPriorityClass, testQueue, "memory"),
				commonmetrics.NewMinQueueResources(gb, testPool, testDefaultPriorityClass, testQueue, "memory"),
				commonmetrics.NewMaxQueueResources(gb, testPool, testDefaultPriorityClass, testQueue, "memory"),
				commonmetrics.NewMedianQueueResources(gb, testPool, testDefaultPriorityClass, testQueue, "memory"),
				commonmetrics.NewCountQueueResources(3, testPool, testDefaultPriorityClass, testQueue, "memory"),
			},
		},
		"running metrics": {
			initialJobs: runningJobs,
			queues:      []*database.Queue{testDbQueue()},
			defaultPool: testPool,
			expected: []prometheus.Metric{
				commonmetrics.NewQueueSizeMetric(0.0, testQueue),
				commonmetrics.NewJobRunRunDuration(3, 300,
					map[float64]uint64{60: 1, 600: 3, 1800: 3, 3600: 3, 10800: 3, 43200: 3, 86400: 3, 172800: 3, 604800: 3},
					testPool, testDefaultPriorityClass, testQueue),
				commonmetrics.NewMinJobRunDuration(0, testPool, testDefaultPriorityClass, testQueue),
				commonmetrics.NewMaxJobRunDuration(200, testPool, testDefaultPriorityClass, testQueue),
				commonmetrics.NewMedianJobRunDuration(100, testPool, testDefaultPriorityClass, testQueue),
				commonmetrics.NewMinQueueAllocated(1, testPool, testDefaultPriorityClass, testQueue, "cpu"),
				commonmetrics.NewMaxQueueAllocated(1, testPool, testDefaultPriorityClass, testQueue, "cpu"),
				commonmetrics.NewMedianQueueAllocated(1, testPool, testDefaultPriorityClass, testQueue, "cpu"),
				commonmetrics.NewMinQueueAllocated(gb, testPool, testDefaultPriorityClass, testQueue, "memory"),
				commonmetrics.NewMaxQueueAllocated(gb, testPool, testDefaultPriorityClass, testQueue, "memory"),
				commonmetrics.NewMedianQueueAllocated(gb, testPool, testDefaultPriorityClass, testQueue, "memory"),
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			testClock := clock.NewFakeClock(baseTime)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			// set up job db with initial jobs
			jobDb := jobdb.NewJobDb()
			txn := jobDb.WriteTxn()
			err := jobDb.Upsert(txn, tc.initialJobs)
			require.NoError(t, err)
			txn.Commit()

			queueRepository := schedulermocks.NewMockQueueRepository(ctrl)
			queueRepository.EXPECT().GetAllQueues().Return(tc.queues, nil).Times(1)
			poolAssigner := &MockPoolAssigner{tc.defaultPool, tc.poolMappings}

			collector := NewMetricsCollector(
				jobDb,
				queueRepository,
				poolAssigner,
				2*time.Second,
			)
			collector.clock = testClock
			err = collector.refresh(ctx)
			require.NoError(t, err)
			metricChan := make(chan prometheus.Metric, 1000) // large buffer so we don't block
			collector.Collect(metricChan)
			close(metricChan)
			actual := make([]prometheus.Metric, 0)
			for m := range metricChan {
				actual = append(actual, m)
			}
			require.NoError(t, err)
			for i := 0; i < len(tc.expected); i++ {
				a1 := actual[i]
				e1 := tc.expected[i]
				require.Equal(t, e1, a1)
			}
		})
	}
}

func TestHashing(t *testing.T) {
	job := schedulerobjects.JobSchedulingInfo{
		Lifetime:          1,
		AtMostOnce:        true,
		Preemptible:       true,
		ConcurrencySafe:   true,
		PriorityClassName: "armada-default",
		SubmitTime:        time.Now(),
		Priority:          10,
		ObjectRequirements: []*schedulerobjects.ObjectRequirements{
			{
				Requirements: &schedulerobjects.ObjectRequirements_PodRequirements{
					PodRequirements: &schedulerobjects.PodRequirements{
						NodeSelector: map[string]string{
							"property1": "value1",
							"property3": "value3",
						},
						Affinity: &v1.Affinity{
							NodeAffinity: &v1.NodeAffinity{
								RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
									NodeSelectorTerms: []v1.NodeSelectorTerm{
										{
											MatchExpressions: []v1.NodeSelectorRequirement{
												{
													Key:      "k1",
													Operator: "o1",
													Values:   []string{"v1", "v2"},
												},
											},
											MatchFields: []v1.NodeSelectorRequirement{
												{
													Key:      "k2",
													Operator: "o2",
													Values:   []string{"v10", "v20"},
												},
											},
										},
									},
								},
							},
							PodAffinity: &v1.PodAffinity{
								RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
									{
										LabelSelector: &metav1.LabelSelector{
											MatchLabels: map[string]string{
												"label1": "labelval1",
												"label2": "labelval2",
												"label3": "labelval3",
											},
											MatchExpressions: []metav1.LabelSelectorRequirement{
												{
													Key:      "k1",
													Operator: "o1",
													Values:   []string{"v1", "v2", "v3"},
												},
											},
										},
										Namespaces:  []string{"n1, n2, n3"},
										TopologyKey: "topkey1",
										NamespaceSelector: &metav1.LabelSelector{
											MatchLabels: map[string]string{
												"label10": "labelval1",
												"label20": "labelval2",
												"label30": "labelval3",
											},
											MatchExpressions: []metav1.LabelSelectorRequirement{
												{
													Key:      "k10",
													Operator: "o10",
													Values:   []string{"v10", "v20", "v30"},
												},
											},
										},
									},
								},
							},
							PodAntiAffinity: nil,
						},
						Tolerations: []v1.Toleration{{
							Key:               "a",
							Operator:          "b",
							Value:             "b",
							Effect:            "d",
							TolerationSeconds: pointer.Int64(1),
						}},
						Annotations: map[string]string{
							"foo":  "bar",
							"fish": "chips",
							"salt": "pepper",
						},
						Priority:         1,
						PreemptionPolicy: "abc",
						ResourceRequirements: v1.ResourceRequirements{
							Limits: map[v1.ResourceName]resource.Quantity{
								"cpu":    resource.MustParse("1"),
								"memory": resource.MustParse("2"),
								"gpu":    resource.MustParse("3"),
							},
							Requests: map[v1.ResourceName]resource.Quantity{
								"cpu":    resource.MustParse("2"),
								"memory": resource.MustParse("2"),
								"gpu":    resource.MustParse("2"),
							},
						},
					},
				},
			},
		},
	}
	b, _ := json.Marshal(&job)
	hash := sha1.Sum(b)
	const numIterations = 100
	start := time.Now()
	for i := 0; i < numIterations; i++ {
		b, _ = proto.Marshal(&job)
		hash2 := sha1.Sum(b)
		require.Equal(t, hash, hash2)
	}
	taken := time.Since(start)
	log.Infof("marshalled %d in %s", numIterations, taken)
}

type MockPoolAssigner struct {
	defaultPool string
	poolsById   map[string]string
}

func (m MockPoolAssigner) Refresh(_ context.Context) error {
	return nil
}

func (m MockPoolAssigner) AssignPool(j *jobdb.Job) (string, error) {
	pool, ok := m.poolsById[j.Id()]
	if !ok {
		pool = m.defaultPool
	}
	return pool, nil
}
