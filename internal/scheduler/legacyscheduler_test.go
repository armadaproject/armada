package scheduler

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common/util"
	schedulerconfig "github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/interfaces"
	"github.com/armadaproject/armada/internal/scheduler/nodedb"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
	"github.com/armadaproject/armada/pkg/api"
)

// func TestQueueCandidateGangIterator(t *testing.T) {
// 	tests := map[string]struct {
// 		Reqs                   []*schedulerobjects.PodRequirements
// 		InitialUsageByPriority schedulerobjects.QuantityByPriorityAndResourceType
// 		SchedulingConstraints  schedulerconstraints.SchedulingConstraints
// 		// If true, jobs are actually leased.
// 		LeaseJobs bool
// 		// Indices of the reqs expected to be returned.
// 		ExpectedIndices []int
// 	}{
// 		"all jobs schedulable": {
// 			Reqs:            testfixtures.TestNSmallCpuJob("A", 0, 3),
// 			ExpectedIndices: []int{0, 1, 2},
// 		},
// 		"minimum job size below limit": {
// 			Reqs: append(testfixtures.TestNSmallCpuJob("A", 0, 3), testfixtures.TestNLargeCpuJob("A", 0, 2)...),
// 			SchedulingConstraints: schedulerconstraints.SchedulingConstraints{
// 				MinimumJobSize: schedulerobjects.ResourceList{
// 					Resources: map[string]resource.Quantity{
// 						"cpu": resource.MustParse("31"),
// 					},
// 				},
// 			},
// 			ExpectedIndices: []int{3, 4},
// 		},
// 		"lookback limit hit": {
// 			Reqs: testfixtures.TestNSmallCpuJob("A", 0, 10),
// 			SchedulingConstraints: schedulerconstraints.SchedulingConstraints{
// 				MaxLookbackPerQueue: 4,
// 			},
// 			ExpectedIndices: []int{0, 1, 2, 3},
// 		},
// 		"minimum job size at limit": {
// 			Reqs: append(testfixtures.TestNSmallCpuJob("A", 0, 3), testfixtures.TestNLargeCpuJob("A", 0, 2)...),
// 			SchedulingConstraints: schedulerconstraints.SchedulingConstraints{
// 				MinimumJobSize: schedulerobjects.ResourceList{
// 					Resources: map[string]resource.Quantity{
// 						"cpu": resource.MustParse("32"),
// 					},
// 				},
// 			},
// 			ExpectedIndices: []int{3, 4},
// 		},
// 		"MaximalResourceFractionToSchedulePerQueue": {
// 			Reqs: testfixtures.TestNSmallCpuJob("A", 0, 3),
// 			SchedulingConstraints: schedulerconstraints.SchedulingConstraints{
// 				TotalResources: schedulerobjects.ResourceList{
// 					Resources: map[string]resource.Quantity{
// 						"cpu": resource.MustParse("32"),
// 					},
// 				},
// 				MaximalResourceFractionToSchedulePerQueue: armadaresource.ComputeResourcesFloat{
// 					"cpu": 2.0 / 32.0,
// 				},
// 			},
// 			LeaseJobs:       true,
// 			ExpectedIndices: []int{0, 1},
// 		},
// 		"MaximalResourceFractionPerQueue": {
// 			Reqs: testfixtures.TestNSmallCpuJob("A", 0, 3),
// 			SchedulingConstraints: SchedulingConstraints{
// 				TotalResources: schedulerobjects.ResourceList{
// 					Resources: map[string]resource.Quantity{
// 						"cpu": resource.MustParse("32"),
// 					},
// 				},
// 				MaximalResourceFractionPerQueue: armadaresource.ComputeResourcesFloat{
// 					"cpu": 2.0 / 32.0,
// 				},
// 			},
// 			LeaseJobs:       true,
// 			ExpectedIndices: []int{0, 1},
// 		},
// 		"MaximalResourceFractionPerQueue with initial usage": {
// 			Reqs: testfixtures.TestNSmallCpuJob("A", 0, 3),
// 			SchedulingConstraints: SchedulingConstraints{
// 				TotalResources: schedulerobjects.ResourceList{
// 					Resources: map[string]resource.Quantity{
// 						"cpu": resource.MustParse("32"),
// 					},
// 				},
// 				MaximalResourceFractionPerQueue: armadaresource.ComputeResourcesFloat{
// 					"cpu": 2.0 / 32.0,
// 				},
// 			},
// 			InitialUsageByPriority: schedulerobjects.QuantityByPriorityAndResourceType{
// 				0: schedulerobjects.ResourceList{
// 					Resources: map[string]resource.Quantity{
// 						"cpu": resource.MustParse("1"),
// 					},
// 				},
// 			},
// 			LeaseJobs:       true,
// 			ExpectedIndices: []int{0},
// 		},
// 		"MaxConsecutiveUnschedulableJobs": {
// 			Reqs: append(append(testfixtures.TestNSmallCpuJob("A", 0, 1), testfixtures.TestNGpuJob("A", 0, 3)...), testfixtures.TestNSmallCpuJob("A", 0, 1)...),
// 			SchedulingConstraints: SchedulingConstraints{
// 				TotalResources: schedulerobjects.ResourceList{
// 					Resources: map[string]resource.Quantity{
// 						"cpu": resource.MustParse("32"),
// 						"gpu": resource.MustParse("1"),
// 					},
// 				},
// 				MaximalResourceFractionPerQueue: armadaresource.ComputeResourcesFloat{
// 					"gpu": 0,
// 				},
// 				MaxLookbackPerQueue: 3,
// 			},
// 			ExpectedIndices: []int{0},
// 		},
// 		"MaximalCumulativeResourceFractionPerQueueAndPriority": {
// 			Reqs: append(append(testfixtures.TestNSmallCpuJob("A", 9, 11), testfixtures.TestNSmallCpuJob("A", 7, 11)...), testfixtures.TestNSmallCpuJob("A", 3, 11)...),
// 			SchedulingConstraints: SchedulingConstraints{
// 				TotalResources: schedulerobjects.ResourceList{
// 					Resources: map[string]resource.Quantity{
// 						"cpu": resource.MustParse("32"),
// 					},
// 				},
// 				MaximalCumulativeResourceFractionPerQueueAndPriority: map[int32]map[string]float64{
// 					3: {"cpu": 30.0 / 32.0},
// 					7: {"cpu": 20.0 / 32.0},
// 					9: {"cpu": 10.0 / 32.0},
// 				},
// 			},
// 			LeaseJobs:       true,
// 			ExpectedIndices: append(append(testfixtures.IntRange(0, 9), testfixtures.IntRange(11, 20)...), testfixtures.IntRange(22, 31)...),
// 		},
// 		"MaximalCumulativeResourceFractionPerQueueAndPriority with initial usage": {
// 			Reqs: append(append(testfixtures.TestNSmallCpuJob("A", 9, 11), testfixtures.TestNSmallCpuJob("A", 7, 11)...), testfixtures.TestNSmallCpuJob("A", 3, 11)...),
// 			SchedulingConstraints: SchedulingConstraints{
// 				TotalResources: schedulerobjects.ResourceList{
// 					Resources: map[string]resource.Quantity{
// 						"cpu": resource.MustParse("32"),
// 					},
// 				},
// 				MaximalCumulativeResourceFractionPerQueueAndPriority: map[int32]map[string]float64{
// 					3: {"cpu": 30.0 / 32.0},
// 					7: {"cpu": 20.0 / 32.0},
// 					9: {"cpu": 10.0 / 32.0},
// 				},
// 			},
// 			InitialUsageByPriority: schedulerobjects.QuantityByPriorityAndResourceType{
// 				3: {Resources: map[string]resource.Quantity{"cpu": resource.MustParse("1")}},
// 				7: {Resources: map[string]resource.Quantity{"cpu": resource.MustParse("2")}},
// 				9: {Resources: map[string]resource.Quantity{"cpu": resource.MustParse("3")}},
// 			},
// 			LeaseJobs:       true,
// 			ExpectedIndices: append(append(testfixtures.IntRange(0, 6), testfixtures.IntRange(11, 18)...), testfixtures.IntRange(22, 30)...),
// 		},
// 	}
// 	for name, tc := range tests {
// 		t.Run(name, func(t *testing.T) {
// 			repo := newMockJobRepository()
// 			jobs := make([]*api.Job, len(tc.Reqs))
// 			indexByJobId := make(map[string]int)
// 			for i, req := range tc.Reqs {
// 				// Queue name doesn't matter.
// 				jobs[i] = apiJobFromPodSpec("A", podSpecFromPodRequirements(req))
// 				repo.Enqueue(jobs[i])
// 				indexByJobId[jobs[i].Id] = i
// 			}

// 			expected := make([]*api.Job, len(tc.ExpectedIndices))
// 			for i, j := range tc.ExpectedIndices {
// 				expected[i] = jobs[j]
// 			}

// 			ctx := context.Background()
// 			queuedJobsIterator, err := NewQueuedJobsIterator(ctx, "A", repo)
// 			if !assert.NoError(t, err) {
// 				return
// 			}
// 			queuedGangIterator := NewQueuedGangIterator(
// 				ctx,
// 				queuedJobsIterator,
// 				tc.SchedulingConstraints.MaxLookbackPerQueue,
// 			)
// 			it := &QueueCandidateGangIterator{
// 				ctx:                     ctx,
// 				SchedulingConstraints:   tc.SchedulingConstraints,
// 				QueueSchedulingContexts: schedulercontext.NewQueueSchedulingContext("A", "executor", 0, tc.InitialUsageByPriority),
// 				queuedGangIterator:      queuedGangIterator,
// 			}

// 			actual := make([]*api.Job, 0)
// 			actualIndices := make([]int, 0)
// 			for gctx, err := it.Next(); gctx != nil; gctx, err = it.Next() {
// 				if !assert.NoError(t, err) {
// 					return
// 				}
// 				for _, jctx := range gctx.JobSchedulingContexts {
// 					if tc.LeaseJobs {
// 						it.QueueSchedulingContexts.AddJobSchedulingContext(jctx, false)
// 					}
// 					actual = append(actual, jctx.Job.(*api.Job))
// 					actualIndices = append(actualIndices, indexByJobId[jctx.Job.GetId()])
// 				}
// 			}
// 			assert.Equal(t, tc.ExpectedIndices, actualIndices) // Redundant, but useful to debug tests.
// 			assert.Equal(t, expected, actual, "")
// 		})
// 	}
// }

type InMemoryNodeIterator struct {
	i     int
	nodes []*schedulerobjects.Node
}

func NewInMemoryNodeIterator(nodes []*schedulerobjects.Node) *InMemoryNodeIterator {
	return &InMemoryNodeIterator{
		nodes: slices.Clone(nodes),
	}
}

func (it *InMemoryNodeIterator) NextNode() *schedulerobjects.Node {
	if it.i >= len(it.nodes) {
		return nil
	}
	v := it.nodes[it.i]
	it.i++
	return v
}

func TestPodRequirementFromLegacySchedulerJob(t *testing.T) {
	resourceLimit := v1.ResourceList{
		"cpu":               resource.MustParse("1"),
		"memory":            resource.MustParse("128Mi"),
		"ephemeral-storage": resource.MustParse("8Gi"),
	}
	requirements := v1.ResourceRequirements{
		Limits:   resourceLimit,
		Requests: resourceLimit,
	}

	j := &api.Job{
		Id:       util.NewULID(),
		Queue:    "test",
		JobSetId: "set1",
		Priority: 1,
		Annotations: map[string]string{
			"something":                             "test",
			configuration.GangIdAnnotation:          "gang-id",
			configuration.GangCardinalityAnnotation: "1",
		},
		PodSpecs: []*v1.PodSpec{
			{
				Containers: []v1.Container{
					{
						Resources: requirements,
					},
				},
				PriorityClassName: "armada-default",
			},
		},
	}

	expected := &schedulerobjects.PodRequirements{
		Priority:             1,
		PreemptionPolicy:     string(v1.PreemptLowerPriority),
		ResourceRequirements: requirements,
		Annotations: map[string]string{
			configuration.GangIdAnnotation:          "gang-id",
			configuration.GangCardinalityAnnotation: "1",
			schedulerconfig.JobIdAnnotation:         j.Id,
			schedulerconfig.QueueAnnotation:         j.Queue,
		},
	}

	result := PodRequirementFromLegacySchedulerJob(j, map[string]configuration.PriorityClass{"armada-default": {Priority: int32(1)}})

	assert.Equal(t, expected, result)
}

func TestEvictOversubscribed(t *testing.T) {
	nodes := testfixtures.TestNCpuNode(1, testfixtures.TestPriorities)
	node := nodes[0]
	var err error
	jobs := append(
		LegacySchedulerJobsFromPodReqs("A", "priority-0", testfixtures.TestNSmallCpuJob("A", 0, 20)),
		LegacySchedulerJobsFromPodReqs("A", "priority-1", testfixtures.TestNSmallCpuJob("A", 1, 20))...,
	)
	reqs := PodRequirementsFromLegacySchedulerJobs(jobs, testfixtures.TestPriorityClasses)
	for _, req := range reqs {
		node, err = nodedb.BindPodToNode(req, node)
		require.NoError(t, err)
	}
	nodes[0] = node

	jobRepo := NewInMemoryJobRepository(testfixtures.TestPriorityClasses)
	jobRepo.EnqueueMany(jobs)
	evictor := NewOversubscribedEvictor(
		jobRepo,
		testfixtures.TestPriorityClasses,
		1,
	)
	it := NewInMemoryNodeIterator(nodes)
	result, err := evictor.Evict(context.Background(), it)
	require.NoError(t, err)

	prioritiesByName := configuration.PriorityByPriorityClassName(testfixtures.TestPriorityClasses)
	priorities := maps.Values(prioritiesByName)
	slices.Sort(priorities)
	for nodeId, node := range result.AffectedNodesById {
		for _, p := range priorities {
			for resourceType, q := range node.AllocatableByPriorityAndResource[p].Resources {
				assert.NotEqual(t, -1, q.Cmp(resource.Quantity{}), "resource %s oversubscribed by %s on node %s", resourceType, q.String(), nodeId)
			}
		}
	}
}

func apiJobsFromPodReqs(queue string, reqs []*schedulerobjects.PodRequirements) []*api.Job {
	rv := make([]*api.Job, len(reqs))
	for i, req := range reqs {
		rv[i] = apiJobFromPodSpec(queue, podSpecFromPodRequirements(req))
		rv[i].Annotations = maps.Clone(req.Annotations)
	}
	return rv
}

func LegacySchedulerJobsFromPodReqs(queue, priorityClassName string, reqs []*schedulerobjects.PodRequirements) []interfaces.LegacySchedulerJob {
	rv := make([]interfaces.LegacySchedulerJob, len(reqs))
	T := time.Now()
	// TODO: This only works if each PC has a unique priority.
	priorityClassNameByPriority := make(map[int32]string)
	for priorityClassName, priorityClass := range testfixtures.TestPriorityClasses {
		if _, ok := priorityClassNameByPriority[priorityClass.Priority]; ok {
			panic(fmt.Sprintf("duplicate priority %d", priorityClass.Priority))
		}
		priorityClassNameByPriority[priorityClass.Priority] = priorityClassName
	}
	for i, req := range reqs {
		// TODO: Let's find a better way to pass around PCs. And for setting, e.g., created.
		podSpec := podSpecFromPodRequirements(req)
		priorityClassName := priorityClassNameByPriority[*podSpec.Priority]
		if priorityClassName == "" {
			panic(fmt.Sprintf("no priority class with priority %d", *podSpec.Priority))
		}
		podSpec.PriorityClassName = priorityClassName
		job := apiJobFromPodSpec(queue, podSpec)
		job.Annotations = maps.Clone(req.Annotations)
		job.Created = T.Add(time.Duration(i) * time.Second)
		if jobId := job.Annotations[schedulerconfig.JobIdAnnotation]; jobId != "" {
			job.Id = jobId
		}
		rv[i] = job
	}
	return rv
}

type resourceLimits struct {
	Minimum schedulerobjects.ResourceList
	Maximum schedulerobjects.ResourceList
}

func newResourceLimits(minimum map[string]resource.Quantity, maximum map[string]resource.Quantity) resourceLimits {
	return resourceLimits{
		Minimum: schedulerobjects.ResourceList{Resources: minimum},
		Maximum: schedulerobjects.ResourceList{Resources: maximum},
	}
}

func assertResourceLimitsSatisfied(t *testing.T, limits resourceLimits, resources schedulerobjects.ResourceList) bool {
	for resource, min := range limits.Minimum.Resources {
		actual := resources.Resources[resource]
		if !assert.NotEqual(t, 1, min.Cmp(actual), "%s limits not satisfied: min is %s, but actual is %s", resource, min.String(), actual.String()) {
			return false
		}
	}
	for resource, actual := range resources.Resources {
		if max, ok := limits.Maximum.Resources[resource]; ok {
			if !assert.NotEqual(t, -1, max.Cmp(actual), "%s limits not satisfied: max is %s, but actual is %s", resource, max.String(), actual.String()) {
				return false
			}
		}
	}
	return true
}

func jobIdsByQueueFromJobs(jobs []interfaces.LegacySchedulerJob) map[string][]string {
	rv := make(map[string][]string)
	for _, job := range jobs {
		rv[job.GetQueue()] = append(rv[job.GetQueue()], job.GetId())
	}
	return rv
}

func usageByQueue(jobs []interfaces.LegacySchedulerJob, priorityClasses map[string]configuration.PriorityClass) map[string]schedulerobjects.ResourceList {
	rv := make(map[string]schedulerobjects.ResourceList)
	for queue, quantityByPriorityAndResourceType := range usageByQueueAndPriority(jobs, priorityClasses) {
		rv[queue] = quantityByPriorityAndResourceType.AggregateByResource()
	}
	return rv
}

func usageByQueueAndPriority(jobs []interfaces.LegacySchedulerJob, priorityByPriorityClassName map[string]configuration.PriorityClass) map[string]schedulerobjects.QuantityByPriorityAndResourceType {
	rv := make(map[string]schedulerobjects.QuantityByPriorityAndResourceType)
	for _, job := range jobs {
		m, ok := rv[job.GetQueue()]
		if !ok {
			m = make(schedulerobjects.QuantityByPriorityAndResourceType)
			rv[job.GetQueue()] = m
		}
		priority := PodRequirementFromJobSchedulingInfo(job.GetRequirements(priorityByPriorityClassName)).Priority
		rl, ok := m[priority]
		if !ok {
			rl.Resources = make(map[string]resource.Quantity)
		}
		rl.Add(job.GetRequirements(priorityByPriorityClassName).GetTotalResourceRequest())
		m[priority] = rl
	}
	return rv
}

func apiJobFromPodSpec(queue string, podSpec *v1.PodSpec) *api.Job {
	return &api.Job{
		Id:      util.NewULID(),
		PodSpec: podSpec,
		Queue:   queue,
	}
}

func podSpecFromPodRequirements(req *schedulerobjects.PodRequirements) *v1.PodSpec {
	return &v1.PodSpec{
		NodeSelector:     req.NodeSelector,
		Affinity:         req.Affinity,
		Tolerations:      req.Tolerations,
		Priority:         &req.Priority,
		PreemptionPolicy: (*v1.PreemptionPolicy)(&req.PreemptionPolicy),
		Containers: []v1.Container{
			{
				Resources: req.ResourceRequirements,
			},
		},
	}
}

type mockJobRepository struct {
	jobsByQueue map[string][]*api.Job
	jobsById    map[string]*api.Job
	// Ids of all jobs hat were leased to an executor.
	leasedJobs          map[string]bool
	getQueueJobIdsDelay time.Duration
}

func newMockJobRepository() *mockJobRepository {
	return &mockJobRepository{
		jobsByQueue: make(map[string][]*api.Job),
		jobsById:    make(map[string]*api.Job),
		leasedJobs:  make(map[string]bool),
	}
}

func (repo *mockJobRepository) EnqueueMany(jobs []*api.Job) {
	for _, job := range jobs {
		repo.Enqueue(job)
	}
}

func (repo *mockJobRepository) Enqueue(job *api.Job) {
	repo.jobsByQueue[job.Queue] = append(repo.jobsByQueue[job.Queue], job)
	repo.jobsById[job.Id] = job
}

func (repo *mockJobRepository) GetJobIterator(ctx context.Context, queue string) (JobIterator, error) {
	return NewQueuedJobsIterator(ctx, queue, repo)
}

func (repo *mockJobRepository) GetQueueJobIds(queue string) ([]string, error) {
	time.Sleep(repo.getQueueJobIdsDelay)
	if jobs, ok := repo.jobsByQueue[queue]; ok {
		rv := make([]string, 0, len(jobs))
		for _, job := range jobs {
			if !repo.leasedJobs[job.Id] {
				rv = append(rv, job.Id)
			}
		}
		return rv, nil
	} else {
		return make([]string, 0), nil
	}
}

func (repo *mockJobRepository) GetExistingJobsByIds(jobIds []string) ([]interfaces.LegacySchedulerJob, error) {
	rv := make([]interfaces.LegacySchedulerJob, len(jobIds))
	for i, jobId := range jobIds {
		if job, ok := repo.jobsById[jobId]; ok {
			rv[i] = job
		}
	}
	return rv, nil
}

func (repo *mockJobRepository) TryLeaseJobs(clusterId string, queue string, jobs []*api.Job) ([]*api.Job, error) {
	successfullyLeasedJobs := make([]*api.Job, 0, len(jobs))
	for _, job := range jobs {
		if !repo.leasedJobs[job.Id] {
			successfullyLeasedJobs = append(successfullyLeasedJobs, job)
			repo.leasedJobs[job.Id] = true
		}
	}
	return successfullyLeasedJobs, nil
}

func benchmarkQuantityComparison(b *testing.B, q1, q2 resource.Quantity) {
	for i := 0; i < b.N; i++ {
		q1.Cmp(q2)
	}
}

func BenchmarkQuantityComparison(b *testing.B) {
	benchmarkQuantityComparison(b, resource.MustParse("1"), resource.MustParse("2"))
}

func BenchmarkIntComparison(b *testing.B) {
	result := 0
	v1 := 1
	v2 := 2
	for i := 0; i < b.N; i++ {
		if v1 == v2 {
			result += 1
		}
	}
}

func CreateNodeDb(nodes []*schedulerobjects.Node) (*nodedb.NodeDb, error) {
	db, err := nodedb.NewNodeDb(
		testfixtures.TestPriorityClasses,
		testfixtures.TestMaxExtraNodesToConsider,
		testfixtures.TestResources,
		testfixtures.TestIndexedTaints,
		testfixtures.TestIndexedNodeLabels,
	)
	if err != nil {
		return nil, err
	}
	if err := db.UpsertMany(nodes); err != nil {
		return nil, err
	}
	return db, nil
}
