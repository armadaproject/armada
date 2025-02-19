package testfixtures

// This file contains test fixtures to be used throughout the tests for this package.
import (
	"encoding/binary"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/oklog/ulid"
	"golang.org/x/exp/maps"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	protoutil "github.com/armadaproject/armada/internal/common/proto"
	"github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/common/stringinterner"
	"github.com/armadaproject/armada/internal/common/types"
	"github.com/armadaproject/armada/internal/common/util"
	schedulerconfiguration "github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/floatingresources"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/internal/server/configuration"
	"github.com/armadaproject/armada/pkg/api"
)

const (
	TestJobset                               = "testJobset"
	TestQueue                                = "testQueue"
	TestQueue1                               = "testQueue1"
	TestQueue2                               = "testQueue2"
	TestPool                                 = "testPool"
	TestPool2                                = "testPool2"
	AwayPool                                 = "awayPool"
	TestHostnameLabel                        = "kubernetes.io/hostname"
	ClusterNameLabel                         = "cluster"
	PoolNameLabel                            = "pool"
	NodeTypeLabel                            = "nodetype"
	PriorityClass0                           = "priority-0"
	PriorityClass1                           = "priority-1"
	PriorityClass2                           = "priority-2"
	PriorityClass2NonPreemptible             = "priority-2-non-preemptible"
	PriorityClass3                           = "priority-3"
	PriorityClass4PreemptibleAway            = "armada-preemptible-away"
	PriorityClass5PreemptibleAwayLowPriority = "armada-preemptible-away-lower"
	PriorityClass6Preemptible                = "armada-preemptible"
)

var (
	TestResourceListFactory    = MakeTestResourceListFactory()
	TestEmptyFloatingResources = MakeTestFloatingResourceTypes(nil)
	TestFloatingResources      = MakeTestFloatingResourceTypes(TestFloatingResourceConfig)
	TestFloatingResourceConfig = []schedulerconfiguration.FloatingResourceConfig{
		{
			Name: "test-floating-resource",
			Pools: []schedulerconfiguration.FloatingResourcePoolConfig{
				{
					Name:     "pool",
					Quantity: resource.MustParse("10"),
				},
			},
		},
	}
	BaseTime, _         = time.Parse("2006-01-02T15:04:05.000Z", "2022-03-01T15:04:05.000Z")
	BasetimeProto       = protoutil.ToTimestamp(BaseTime)
	TestPriorityClasses = map[string]types.PriorityClass{
		PriorityClass0:                           {Priority: 0, Preemptible: true},
		PriorityClass1:                           {Priority: 1, Preemptible: true},
		PriorityClass2:                           {Priority: 2, Preemptible: true},
		PriorityClass2NonPreemptible:             {Priority: 2, Preemptible: false},
		PriorityClass3:                           {Priority: 3, Preemptible: false},
		PriorityClass4PreemptibleAway:            {Priority: 30000, Preemptible: true, AwayNodeTypes: []types.AwayNodeType{{Priority: 29000, WellKnownNodeTypeName: "gpu"}, {Priority: 29000, WellKnownNodeTypeName: "large"}}},
		PriorityClass5PreemptibleAwayLowPriority: {Priority: 30000, Preemptible: true, AwayNodeTypes: []types.AwayNodeType{{Priority: 28000, WellKnownNodeTypeName: "gpu"}, {Priority: 28000, WellKnownNodeTypeName: "large"}}},
		PriorityClass6Preemptible:                {Priority: 30000, Preemptible: true},
	}
	TestDefaultPriorityClass = PriorityClass3
	TestPriorities           = []int32{0, 1, 2, 3}
	TestResources            = []schedulerconfiguration.ResourceType{
		{Name: "cpu", Resolution: resource.MustParse("1")},
		{Name: "memory", Resolution: resource.MustParse("128Mi")},
		{Name: "nvidia.com/gpu", Resolution: resource.MustParse("1")},
	}
	TestResourceNames = slices.Map(
		TestResources,
		func(v schedulerconfiguration.ResourceType) string { return v.Name },
	)
	TestIndexedTaints      = []string{"largeJobsOnly", "gpu"}
	TestIndexedNodeLabels  = []string{"largeJobsOnly", "gpu", ClusterNameLabel, PoolNameLabel, NodeTypeLabel}
	TestWellKnownNodeTypes = []schedulerconfiguration.WellKnownNodeType{
		{
			Name:   "gpu",
			Taints: []v1.Taint{{Key: "gpu", Value: "true", Effect: v1.TaintEffectNoSchedule}},
		},
		{
			Name:   "large",
			Taints: []v1.Taint{{Key: "largeJobsOnly", Value: "true", Effect: v1.TaintEffectNoSchedule}},
		},
	}
	TestNodeFactory = internaltypes.NewNodeFactory(TestIndexedTaints, TestIndexedNodeLabels, TestPriorityClasses, TestResourceListFactory)
	jobTimestamp    atomic.Int64
	// SchedulingKeyGenerator to use in testing.
	// Has to be consistent since creating one involves generating a random key.
	// If this key isn't consistent, scheduling keys generated are not either.
	// We use the all-zeros key here to ensure scheduling keys are cosnsitent between tests.
	SchedulingKeyGenerator = internaltypes.NewSchedulingKeyGeneratorWithKey(make([]byte, 32))
	// Used for job creation.
	JobDb = NewJobDb(TestResourceListFactory)
)

func NewJobDbWithJobs(jobs []*jobdb.Job) *jobdb.JobDb {
	jobDb := NewJobDb(TestResourceListFactory)
	txn := jobDb.WriteTxn()
	defer txn.Abort()
	if err := txn.Upsert(jobs); err != nil {
		panic(err)
	}
	txn.Commit()
	return jobDb
}

// NewJobDb returns a new default jobDb with defaults to use in tests.
func NewJobDb(resourceListFactory *internaltypes.ResourceListFactory) *jobdb.JobDb {
	jobDb := jobdb.NewJobDbWithSchedulingKeyGenerator(
		TestPriorityClasses,
		TestDefaultPriorityClass,
		SchedulingKeyGenerator,
		stringinterner.New(1024),
		resourceListFactory,
	)
	// Mock out the clock and uuid provider to ensure consistent ids and timestamps are generated.
	jobDb.SetClock(NewMockPassiveClock())
	jobDb.SetUUIDProvider(NewMockIDProvider())
	return jobDb
}

func NewJob(
	jobId string,
	jobSet string,
	queue string,
	priority uint32,
	price float64,
	schedulingInfo *internaltypes.JobSchedulingInfo,
	queued bool,
	queuedVersion int32,
	cancelRequested bool,
	cancelByJobSetRequested bool,
	cancelled bool,
	created int64,
	validated bool,
) *jobdb.Job {
	job, err := JobDb.NewJob(jobId,
		jobSet,
		queue,
		priority,
		price,
		schedulingInfo,
		queued,
		queuedVersion,
		cancelRequested,
		cancelByJobSetRequested,
		cancelled,
		created,
		validated,
		[]string{},
	)
	if err != nil {
		panic(err)
	}
	return job
}

func IntRange(a, b int) []int {
	rv := make([]int, b-a+1)
	for i := range rv {
		rv[i] = a + i
	}
	return rv
}

func Repeat[T any](v T, n int) []T {
	rv := make([]T, n)
	for i := 0; i < n; i++ {
		rv[i] = v
	}
	return rv
}

func TestSchedulingConfig() schedulerconfiguration.SchedulingConfig {
	return TestSchedulingConfigWithPools([]schedulerconfiguration.PoolConfig{{Name: TestPool}})
}

func TestSchedulingConfigWithPools(pools []schedulerconfiguration.PoolConfig) schedulerconfiguration.SchedulingConfig {
	return schedulerconfiguration.SchedulingConfig{
		PriorityClasses:                             maps.Clone(TestPriorityClasses),
		DefaultPriorityClassName:                    TestDefaultPriorityClass,
		MaximumSchedulingRate:                       math.Inf(1),
		MaximumSchedulingBurst:                      math.MaxInt,
		MaximumPerQueueSchedulingRate:               math.Inf(1),
		MaximumPerQueueSchedulingBurst:              math.MaxInt,
		IndexedResources:                            TestResources,
		IndexedNodeLabels:                           TestIndexedNodeLabels,
		IndexedTaints:                               TestIndexedTaints,
		WellKnownNodeTypes:                          TestWellKnownNodeTypes,
		EnablePreferLargeJobOrdering:                true,
		DominantResourceFairnessResourcesToConsider: TestResourceNames,
		ExecutorTimeout:                             15 * time.Minute,
		MaxUnacknowledgedJobsPerExecutor:            math.MaxInt,
		SupportedResourceTypes:                      GetTestSupportedResourceTypes(),
		Pools:                                       pools,
	}
}

func WithMaxUnacknowledgedJobsPerExecutorConfig(v uint, config schedulerconfiguration.SchedulingConfig) schedulerconfiguration.SchedulingConfig {
	config.MaxUnacknowledgedJobsPerExecutor = v
	return config
}

func WithProtectedFractionOfFairShareConfig(v float64, config schedulerconfiguration.SchedulingConfig) schedulerconfiguration.SchedulingConfig {
	config.ProtectedFractionOfFairShare = v
	return config
}

func WithRoundLimitsConfig(limits map[string]float64, config schedulerconfiguration.SchedulingConfig) schedulerconfiguration.SchedulingConfig {
	config.MaximumResourceFractionToSchedule = limits
	return config
}

func WithRoundLimitsPoolConfig(limits map[string]map[string]float64, config schedulerconfiguration.SchedulingConfig) schedulerconfiguration.SchedulingConfig {
	config.MaximumResourceFractionToScheduleByPool = limits
	return config
}

func WithPerPriorityLimitsConfig(limits map[string]map[string]float64, config schedulerconfiguration.SchedulingConfig) schedulerconfiguration.SchedulingConfig {
	for priorityClassName, limit := range limits {
		priorityClass, ok := config.PriorityClasses[priorityClassName]
		if !ok {
			panic(fmt.Sprintf("no priority class with name %s", priorityClassName))
		}
		// We need to make a copy to avoid mutating the priorityClasses, which are used by other tests too.
		config.PriorityClasses[priorityClassName] = types.PriorityClass{
			Priority:                              priorityClass.Priority,
			Preemptible:                           priorityClass.Preemptible,
			MaximumResourceFractionPerQueue:       limit,
			MaximumResourceFractionPerQueueByPool: priorityClass.MaximumResourceFractionPerQueueByPool,
		}
	}
	return config
}

func WithIndexedResourcesConfig(indexResources []schedulerconfiguration.ResourceType, config schedulerconfiguration.SchedulingConfig) schedulerconfiguration.SchedulingConfig {
	config.IndexedResources = indexResources
	return config
}

func WithGlobalSchedulingRateLimiterConfig(maximumSchedulingRate float64, maximumSchedulingBurst int, config schedulerconfiguration.SchedulingConfig) schedulerconfiguration.SchedulingConfig {
	config.MaximumSchedulingRate = maximumSchedulingRate
	config.MaximumSchedulingBurst = maximumSchedulingBurst
	return config
}

func WithPerQueueSchedulingLimiterConfig(maximumPerQueueSchedulingRate float64, maximumPerQueueSchedulingBurst int, config schedulerconfiguration.SchedulingConfig) schedulerconfiguration.SchedulingConfig {
	config.MaximumPerQueueSchedulingRate = maximumPerQueueSchedulingRate
	config.MaximumPerQueueSchedulingBurst = maximumPerQueueSchedulingBurst
	return config
}

func WithMaxLookbackPerQueueConfig(n uint, config schedulerconfiguration.SchedulingConfig) schedulerconfiguration.SchedulingConfig {
	config.MaxQueueLookback = n
	return config
}

func WithIndexedTaintsConfig(indexedTaints []string, config schedulerconfiguration.SchedulingConfig) schedulerconfiguration.SchedulingConfig {
	config.IndexedTaints = append(config.IndexedTaints, indexedTaints...)
	return config
}

func WithIndexedNodeLabelsConfig(indexedNodeLabels []string, config schedulerconfiguration.SchedulingConfig) schedulerconfiguration.SchedulingConfig {
	config.IndexedNodeLabels = append(config.IndexedNodeLabels, indexedNodeLabels...)
	return config
}

func WithMaxQueueLookbackConfig(maxQueueLookback uint, config schedulerconfiguration.SchedulingConfig) schedulerconfiguration.SchedulingConfig {
	config.MaxQueueLookback = maxQueueLookback
	return config
}

func WithUsedResourcesNodes(p int32, rl internaltypes.ResourceList, nodes []*internaltypes.Node) []*internaltypes.Node {
	for _, node := range nodes {
		internaltypes.MarkAllocated(node.AllocatableByPriority, p, rl)
	}
	return nodes
}

func WithNodeTypeNodes(nodeType *internaltypes.NodeType, nodes []*internaltypes.Node) []*internaltypes.Node {
	result := make([]*internaltypes.Node, len(nodes))
	for i, node := range nodes {
		result[i] = internaltypes.CreateNode(node.GetId(),
			nodeType,
			node.GetIndex(),
			node.GetExecutor(),
			node.GetName(),
			node.GetPool(),
			node.GetReportingNodeType(),
			node.GetTaints(),
			node.GetLabels(),
			false,
			node.GetTotalResources(),
			node.GetAllocatableResources(),
			node.AllocatableByPriority,
			node.AllocatedByQueue,
			node.AllocatedByJobId,
			node.EvictedJobRunIds,
			nil)
	}
	return result
}

func WithIdNodes(nodeId string, nodes []*internaltypes.Node) []*internaltypes.Node {
	result := make([]*internaltypes.Node, len(nodes))
	for i, node := range nodes {
		result[i] = internaltypes.CreateNode(nodeId,
			node.GetNodeType(),
			node.GetIndex(),
			node.GetExecutor(),
			node.GetName(),
			node.GetPool(),
			node.GetReportingNodeType(),
			node.GetTaints(),
			node.GetLabels(),
			false,
			node.GetTotalResources(),
			node.GetAllocatableResources(),
			node.AllocatableByPriority,
			node.AllocatedByQueue,
			node.AllocatedByJobId,
			node.EvictedJobRunIds,
			nil,
		)
	}
	return result
}

func WithIndexNode(idx uint64, node *internaltypes.Node) *internaltypes.Node {
	return internaltypes.CreateNode(node.GetId(),
		node.GetNodeType(),
		idx,
		node.GetExecutor(),
		node.GetName(),
		node.GetPool(),
		node.GetReportingNodeType(),
		node.GetTaints(),
		node.GetLabels(),
		false,
		node.GetTotalResources(),
		node.GetAllocatableResources(),
		node.AllocatableByPriority,
		node.AllocatedByQueue,
		node.AllocatedByJobId,
		node.EvictedJobRunIds,
		nil,
	)
}

func WithPriorityJobs(priority uint32, jobs []*jobdb.Job) []*jobdb.Job {
	for i, job := range jobs {
		jobs[i] = job.WithPriority(priority)
	}
	return jobs
}

func WithNodeUniformityLabelAnnotationJobs(label string, jobs []*jobdb.Job) []*jobdb.Job {
	for _, job := range jobs {
		req := job.PodRequirements()
		if req.Annotations == nil {
			req.Annotations = make(map[string]string)
		}
		req.Annotations[configuration.GangNodeUniformityLabelAnnotation] = label
	}
	return jobs
}

func WithNodeAffinityJobs(nodeSelectorTerms []v1.NodeSelectorTerm, jobs []*jobdb.Job) []*jobdb.Job {
	for _, job := range jobs {
		req := job.PodRequirements()
		if req.Affinity == nil {
			req.Affinity = &v1.Affinity{}
		}
		if req.Affinity.NodeAffinity == nil {
			req.Affinity.NodeAffinity = &v1.NodeAffinity{}
		}
		if req.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
			req.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = &v1.NodeSelector{}
		}
		req.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms = append(
			req.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms,
			nodeSelectorTerms...,
		)
	}
	return jobs
}

func WithRequestsJobs(rl schedulerobjects.ResourceList, jobs []*jobdb.Job) []*jobdb.Job {
	newJobs := make([]*jobdb.Job, len(jobs))
	for i, job := range jobs {
		newSchedInfo := job.JobSchedulingInfo().DeepCopy()
		maps.Copy(
			newSchedInfo.PodRequirements.ResourceRequirements.Requests,
			schedulerobjects.V1ResourceListFromResourceList(rl),
		)
		newJob, err := job.WithJobSchedulingInfo(newSchedInfo)
		if err != nil {
			panic(err)
		}
		newJobs[i] = newJob
	}
	return newJobs
}

func WithNodeSelectorJobs(selector map[string]string, jobs []*jobdb.Job) []*jobdb.Job {
	for _, job := range jobs {
		job.JobSchedulingInfo().PodRequirements.NodeSelector = maps.Clone(selector)
	}
	return jobs
}

func WithNodeSelectorJob(selector map[string]string, job *jobdb.Job) *jobdb.Job {
	job = job.DeepCopy()
	job.JobSchedulingInfo().PodRequirements.NodeSelector = maps.Clone(selector)
	return job
}

func WithGangAnnotationsJobs(jobs []*jobdb.Job) []*jobdb.Job {
	gangId := uuid.NewString()
	gangCardinality := fmt.Sprintf("%d", len(jobs))
	return WithAnnotationsJobs(
		map[string]string{configuration.GangIdAnnotation: gangId, configuration.GangCardinalityAnnotation: gangCardinality},
		jobs,
	)
}

func WithPools(jobs []*jobdb.Job, pools []string) []*jobdb.Job {
	result := make([]*jobdb.Job, 0, len(jobs))
	for _, job := range jobs {
		result = append(result, job.WithPools(pools))
	}
	return result
}

func WithNodeUniformityGangAnnotationsJobs(jobs []*jobdb.Job, nodeUniformityLabel string) []*jobdb.Job {
	gangId := uuid.NewString()
	gangCardinality := fmt.Sprintf("%d", len(jobs))
	return WithAnnotationsJobs(
		map[string]string{configuration.GangIdAnnotation: gangId, configuration.GangCardinalityAnnotation: gangCardinality, configuration.GangNodeUniformityLabelAnnotation: nodeUniformityLabel},
		jobs,
	)
}

func WithAnnotationsJobs(annotations map[string]string, jobs []*jobdb.Job) []*jobdb.Job {
	for _, job := range jobs {
		if job.PodRequirements().Annotations == nil {
			job.PodRequirements().Annotations = make(map[string]string)
		}
		maps.Copy(job.PodRequirements().Annotations, annotations)
	}
	return jobs
}

func N1Cpu4GiJobsWithPrice(queue string, bidPrice float64, n int) []*jobdb.Job {
	rv := make([]*jobdb.Job, n)
	for i := 0; i < n; i++ {
		j := Test1Cpu4GiJob(queue, PriorityClass0)
		j = j.WithBidPrice(bidPrice)
		rv[i] = j
	}
	return rv
}

func N1Cpu4GiJobs(queue string, priorityClassName string, n int) []*jobdb.Job {
	rv := make([]*jobdb.Job, n)
	for i := 0; i < n; i++ {
		rv[i] = Test1Cpu4GiJob(queue, priorityClassName)
	}
	return rv
}

func N1Cpu16GiJobs(queue string, priorityClassName string, n int) []*jobdb.Job {
	rv := make([]*jobdb.Job, n)
	for i := 0; i < n; i++ {
		rv[i] = Test1Cpu16GiJob(queue, priorityClassName)
	}
	return rv
}

func N16Cpu128GiJobs(queue string, priorityClassName string, n int) []*jobdb.Job {
	rv := make([]*jobdb.Job, n)
	for i := 0; i < n; i++ {
		rv[i] = Test16Cpu128GiJob(queue, priorityClassName)
	}
	return rv
}

func N32Cpu256GiJobs(queue string, priorityClassName string, n int) []*jobdb.Job {
	rv := make([]*jobdb.Job, n)
	for i := 0; i < n; i++ {
		rv[i] = Test32Cpu256GiJob(queue, priorityClassName)
	}
	return rv
}

func N1GpuJobs(queue string, priorityClassName string, n int) []*jobdb.Job {
	rv := make([]*jobdb.Job, n)
	for i := 0; i < n; i++ {
		rv[i] = Test1GpuJob(queue, priorityClassName)
	}
	return rv
}

func extractPriority(priorityClassName string) int32 {
	priorityClass, ok := TestPriorityClasses[priorityClassName]
	if !ok {
		panic(fmt.Sprintf("no priority class with name %s", priorityClassName))
	}
	return priorityClass.Priority
}

func TestJob(queue string, jobId ulid.ULID, priorityClassName string, req *internaltypes.PodRequirements) *jobdb.Job {
	created := jobTimestamp.Add(1)
	submitTime := time.Time{}.Add(time.Millisecond * time.Duration(created))
	job, _ := JobDb.NewJob(
		jobId.String(),
		TestJobset,
		queue,
		// This is the per-queue priority of this job, which is unrelated to `priorityClassName`.
		1000,
		0.0,
		&internaltypes.JobSchedulingInfo{
			PriorityClassName: priorityClassName,
			SubmitTime:        submitTime,
			PodRequirements:   req,
		},
		false,
		0,
		false,
		false,
		false,
		created,
		false,
		[]string{TestPool},
	)
	return job
}

func Test1Cpu4GiJob(queue string, priorityClassName string) *jobdb.Job {
	jobId := util.ULID()
	return TestJob(queue, jobId, priorityClassName, Test1Cpu4GiPodReqs(queue, jobId, extractPriority(priorityClassName)))
}

func Test1Cpu16GiJob(queue string, priorityClassName string) *jobdb.Job {
	jobId := util.ULID()
	return TestJob(queue, jobId, priorityClassName, Test1Cpu16GiPodReqs(queue, jobId, extractPriority(priorityClassName)))
}

func Test16Cpu128GiJob(queue string, priorityClassName string) *jobdb.Job {
	jobId := util.ULID()
	return TestJob(queue, jobId, priorityClassName, Test16Cpu128GiPodReqs(queue, jobId, extractPriority(priorityClassName)))
}

func Test32Cpu256GiJob(queue string, priorityClassName string) *jobdb.Job {
	jobId := util.ULID()
	return TestJob(queue, jobId, priorityClassName, Test32Cpu256GiPodReqs(queue, jobId, extractPriority(priorityClassName)))
}

func Test1GpuJob(queue string, priorityClassName string) *jobdb.Job {
	jobId := util.ULID()
	return TestJob(queue, jobId, priorityClassName, Test1GpuPodReqs(queue, jobId, extractPriority(priorityClassName)))
}

func N1CpuPodReqs(queue string, priority int32, n int) []*internaltypes.PodRequirements {
	rv := make([]*internaltypes.PodRequirements, n)
	for i := 0; i < n; i++ {
		rv[i] = Test1Cpu4GiPodReqs(queue, util.ULID(), priority)
	}
	return rv
}

func TestPodReqs(queue string, jobId ulid.ULID, priority int32, requests v1.ResourceList) *internaltypes.PodRequirements {
	return &internaltypes.PodRequirements{
		ResourceRequirements: v1.ResourceRequirements{Requests: requests},
		Annotations:          make(map[string]string),
		NodeSelector:         make(map[string]string),
	}
}

func Test1Cpu4GiPodReqs(queue string, jobId ulid.ULID, priority int32) *internaltypes.PodRequirements {
	return TestPodReqs(
		queue,
		jobId,
		priority,
		v1.ResourceList{
			"cpu":    resource.MustParse("1"),
			"memory": resource.MustParse("4Gi"),
		},
	)
}

func Test1Cpu16GiPodReqs(queue string, jobId ulid.ULID, priority int32) *internaltypes.PodRequirements {
	return TestPodReqs(
		queue,
		jobId,
		priority,
		v1.ResourceList{
			"cpu":    resource.MustParse("1"),
			"memory": resource.MustParse("16Gi"),
		},
	)
}

func Test16Cpu128GiPodReqs(queue string, jobId ulid.ULID, priority int32) *internaltypes.PodRequirements {
	req := TestPodReqs(
		queue,
		jobId,
		priority,
		v1.ResourceList{
			"cpu":    resource.MustParse("16"),
			"memory": resource.MustParse("128Gi"),
		},
	)
	req.Tolerations = []v1.Toleration{
		{
			Key:   "largeJobsOnly",
			Value: "true",
		},
	}
	return req
}

func Test32Cpu256GiPodReqs(queue string, jobId ulid.ULID, priority int32) *internaltypes.PodRequirements {
	req := TestPodReqs(
		queue,
		jobId,
		priority,
		v1.ResourceList{
			"cpu":    resource.MustParse("32"),
			"memory": resource.MustParse("256Gi"),
		},
	)
	req.Tolerations = []v1.Toleration{
		{
			Key:   "largeJobsOnly",
			Value: "true",
		},
	}
	return req
}

func Test1GpuPodReqs(queue string, jobId ulid.ULID, priority int32) *internaltypes.PodRequirements {
	req := TestPodReqs(
		queue,
		jobId,
		priority,
		v1.ResourceList{
			"cpu":            resource.MustParse("8"),
			"memory":         resource.MustParse("128Gi"),
			"nvidia.com/gpu": resource.MustParse("1"),
		},
	)
	req.Tolerations = []v1.Toleration{
		{
			Key:   "gpu",
			Value: "true",
		},
	}
	return req
}

func TestUnitReqs() *internaltypes.PodRequirements {
	return &internaltypes.PodRequirements{
		ResourceRequirements: v1.ResourceRequirements{
			Requests: v1.ResourceList{
				"cpu":    resource.MustParse("1"),
				"memory": resource.MustParse("1Gi"),
			},
		},
		Annotations:  make(map[string]string),
		NodeSelector: make(map[string]string),
	}
}

func N32CpuNodes(n int, priorities []int32) []*internaltypes.Node {
	rv := make([]*internaltypes.Node, n)
	for i := 0; i < n; i++ {
		rv[i] = Test32CpuNode(priorities)
	}
	return rv
}

func NTainted32CpuNodes(n int, priorities []int32) []*internaltypes.Node {
	rv := make([]*internaltypes.Node, n)
	for i := 0; i < n; i++ {
		rv[i] = TestTainted32CpuNode(priorities)
	}
	return rv
}

func N8GpuNodes(n int, priorities []int32) []*internaltypes.Node {
	rv := make([]*internaltypes.Node, n)
	for i := 0; i < n; i++ {
		rv[i] = Test8GpuNode(priorities)
	}
	return rv
}

func SingleQueuePriorityOne(name string) []*api.Queue {
	return []*api.Queue{{Name: name, PriorityFactor: 1.0}}
}

func TestSchedulerObjectsNode(priorities []int32, resources map[string]resource.Quantity) *schedulerobjects.Node {
	id := uuid.NewString()
	return &schedulerobjects.Node{
		Id:             id,
		Name:           id,
		Pool:           TestPool,
		TotalResources: schedulerobjects.ResourceList{Resources: resources},
		AllocatableByPriorityAndResource: schedulerobjects.NewAllocatableByPriorityAndResourceType(
			priorities,
			schedulerobjects.ResourceList{Resources: resources},
		),
		StateByJobRunId: make(map[string]schedulerobjects.JobRunState),
		Labels: map[string]string{
			TestHostnameLabel: id,
			// TODO(albin): Nodes should be created from the NodeDb to ensure this label is set automatically.
			schedulerconfiguration.NodeIdLabel: id,
		},
	}
}

func TestSimpleNode(id string) *internaltypes.Node {
	return internaltypes.CreateNode(
		id,
		nil,
		0,
		"",
		"",
		"",
		"",
		nil,
		nil,
		false,
		internaltypes.ResourceList{},
		internaltypes.ResourceList{},
		nil,
		nil,
		nil,
		nil,
		nil)
}

func TestNode(priorities []int32, resources map[string]resource.Quantity) *internaltypes.Node {
	rl := TestNodeFactory.ResourceListFactory().FromNodeProto(resources)
	id := uuid.NewString()
	return TestNodeFactory.CreateNodeAndType(id,
		"executor1",
		id,
		TestPool,
		"type",
		false,
		[]v1.Taint{},
		map[string]string{
			TestHostnameLabel:                  id,
			schedulerconfiguration.NodeIdLabel: id,
		},
		rl,
		rl,
		map[int32]internaltypes.ResourceList{},
		internaltypes.NewAllocatableByPriorityAndResourceType(priorities, rl))
}

func Test32CpuNode(priorities []int32) *internaltypes.Node {
	return TestNode(
		priorities,
		map[string]resource.Quantity{
			"cpu":    resource.MustParse("32"),
			"memory": resource.MustParse("256Gi"),
		},
	)
}

func TestTainted32CpuNode(priorities []int32) *internaltypes.Node {
	node := Test32CpuNode(priorities)

	node = TestNodeFactory.AddTaints([]*internaltypes.Node{node},
		[]v1.Taint{
			{
				Key:    "largeJobsOnly",
				Value:  "true",
				Effect: v1.TaintEffectNoSchedule,
			},
		})[0]

	node = TestNodeFactory.AddLabels(
		[]*internaltypes.Node{node},
		map[string]string{"largeJobsOnly": "true"},
	)[0]

	return node
}

func Test8GpuNode(priorities []int32) *internaltypes.Node {
	node := TestNode(
		priorities,
		map[string]resource.Quantity{
			"cpu":            resource.MustParse("64"),
			"memory":         resource.MustParse("1024Gi"),
			"nvidia.com/gpu": resource.MustParse("8"),
		},
	)
	return TestNodeFactory.AddLabels(
		[]*internaltypes.Node{node},
		map[string]string{"gpu": "true"},
	)[0]
}

func MakeTestQueue() *api.Queue {
	return &api.Queue{
		Name:           TestQueue,
		PriorityFactor: 100,
	}
}

func MakeTestQueue2() *api.Queue {
	return &api.Queue{
		Name:           TestQueue2,
		PriorityFactor: 100,
	}
}

func MakeTestQueueCordoned() *api.Queue {
	return &api.Queue{
		Name:           TestQueue1,
		PriorityFactor: 100,
		Cordoned:       true,
	}
}

func TestQueuedJobDbJob() *jobdb.Job {
	job, _ := JobDb.NewJob(
		util.NewULID(),
		TestJobset,
		TestQueue,
		0,
		0.0,
		&internaltypes.JobSchedulingInfo{
			PriorityClassName: TestDefaultPriorityClass,
			SubmitTime:        BaseTime,
			PodRequirements:   TestUnitReqs(),
		},
		true,
		0,
		false,
		false,
		false,
		BaseTime.UnixNano(),
		false,
		[]string{TestPool},
	)
	return job
}

func TestRunningJobDbJob(startTime int64) *jobdb.Job {
	return TestQueuedJobDbJob().
		WithQueued(false).
		WithUpdatedRun(jobdb.MinimalRun(uuid.NewString(), startTime).WithPool(TestPool))
}

type MockIDProvider struct {
	i  uint64
	mu sync.Mutex
}

func NewMockIDProvider() *MockIDProvider {
	return &MockIDProvider{}
}

func (p *MockIDProvider) New() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.i += 1 // Increment before write to avoid using the all-zeros UUID.
	return UUIDFromInt(p.i).String()
}

func UUIDFromInt(i uint64) uuid.UUID {
	var rv uuid.UUID
	binary.LittleEndian.PutUint64(rv[:], i)
	return rv
}

type MockPassiveClock struct {
	t  time.Time
	d  time.Duration
	mu sync.Mutex
}

func NewMockPassiveClock() *MockPassiveClock {
	return &MockPassiveClock{t: time.Unix(0, 0), d: time.Second}
}

func (p *MockPassiveClock) Now() time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()
	rv := p.t
	p.t = p.t.Add(p.d)
	return rv
}

func (p *MockPassiveClock) Since(time.Time) time.Duration {
	panic("Not implemented")
}

func MakeTestResourceListFactory() *internaltypes.ResourceListFactory {
	result, _ := internaltypes.NewResourceListFactory(GetTestSupportedResourceTypes(), TestFloatingResourceConfig)
	return result
}

func MakeTestFloatingResourceTypes(config []schedulerconfiguration.FloatingResourceConfig) *floatingresources.FloatingResourceTypes {
	result, _ := floatingresources.NewFloatingResourceTypes(config, TestResourceListFactory)
	return result
}

func GetTestSupportedResourceTypes() []schedulerconfiguration.ResourceType {
	return []schedulerconfiguration.ResourceType{
		{Name: "memory", Resolution: resource.MustParse("1")},
		{Name: "cpu", Resolution: resource.MustParse("1m")},
		{Name: "nvidia.com/gpu", Resolution: resource.MustParse("1m")},
	}
}

func Cpu(cpu string) internaltypes.ResourceList {
	return CpuMemGpu(cpu, "0", "0")
}

func CpuMem(cpu string, memory string) internaltypes.ResourceList {
	return CpuMemGpu(cpu, memory, "0")
}

func CpuMemGpu(cpu string, memory string, gpu string) internaltypes.ResourceList {
	return TestResourceListFactory.FromNodeProto(
		map[string]resource.Quantity{
			"cpu":            resource.MustParse(cpu),
			"memory":         resource.MustParse(memory),
			"nvidia.com/gpu": resource.MustParse(gpu),
		},
	)
}
