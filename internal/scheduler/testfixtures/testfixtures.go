package testfixtures

// This file contains test fixtures to be used throughout the tests for this package.
import (
	"context"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/oklog/ulid"
	"github.com/sirupsen/logrus"
	"golang.org/x/exp/maps"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common/types"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

const (
	TestJobset                   = "testJobset"
	TestQueue                    = "testQueue"
	TestPool                     = "testPool"
	TestHostnameLabel            = "kubernetes.io/hostname"
	PriorityClass0               = "priority-0"
	PriorityClass1               = "priority-1"
	PriorityClass2               = "priority-2"
	PriorityClass2NonPreemptible = "priority-2-non-preemptible"
	PriorityClass3               = "priority-3"
)

var (
	BaseTime, _         = time.Parse("2006-01-02T15:04:05.000Z", "2022-03-01T15:04:05.000Z")
	TestPriorityClasses = map[string]types.PriorityClass{
		PriorityClass0:               {Priority: 0, Preemptible: true},
		PriorityClass1:               {Priority: 1, Preemptible: true},
		PriorityClass2:               {Priority: 2, Preemptible: true},
		PriorityClass2NonPreemptible: {Priority: 2, Preemptible: false},
		PriorityClass3:               {Priority: 3, Preemptible: false},
	}
	TestDefaultPriorityClass         = PriorityClass3
	TestPriorities                   = []int32{0, 1, 2, 3}
	TestMaxExtraNodesToConsider uint = 1
	TestResources                    = []configuration.IndexedResource{
		{Name: "cpu", Resolution: resource.MustParse("1")},
		{Name: "memory", Resolution: resource.MustParse("128Mi")},
		{Name: "gpu", Resolution: resource.MustParse("1")},
	}
	TestResourceNames = util.Map(
		TestResources,
		func(v configuration.IndexedResource) string { return v.Name },
	)
	TestIndexedResourceResolutionMillis = util.Map(
		TestResources,
		func(v configuration.IndexedResource) int64 { return v.Resolution.MilliValue() },
	)
	TestIndexedTaints     = []string{"largeJobsOnly", "gpu"}
	TestIndexedNodeLabels = []string{"largeJobsOnly", "gpu"}
	jobTimestamp          atomic.Int64
)

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

func ContextWithDefaultLogger(ctx context.Context) context.Context {
	return ctxlogrus.ToContext(ctx, logrus.NewEntry(logrus.New()))
}

func TestSchedulingConfig() configuration.SchedulingConfig {
	return configuration.SchedulingConfig{
		ResourceScarcity: map[string]float64{"cpu": 1},
		Preemption: configuration.PreemptionConfig{
			PriorityClasses:                         maps.Clone(TestPriorityClasses),
			DefaultPriorityClass:                    TestDefaultPriorityClass,
			NodeEvictionProbability:                 1.0,
			NodeOversubscriptionEvictionProbability: 1.0,
		},
		IndexedResources:  TestResources,
		IndexedNodeLabels: TestIndexedNodeLabels,
		DominantResourceFairnessResourcesToConsider: TestResourceNames,
		ExecutorTimeout:                  15 * time.Minute,
		MaxUnacknowledgedJobsPerExecutor: math.MaxInt,
	}
}

func WithUnifiedSchedulingByPoolConfig(config configuration.SchedulingConfig) configuration.SchedulingConfig {
	config.UnifiedSchedulingByPool = true
	return config
}

func WithMaxUnacknowledgedJobsPerExecutorConfig(v uint, config configuration.SchedulingConfig) configuration.SchedulingConfig {
	config.MaxUnacknowledgedJobsPerExecutor = v
	return config
}

func WithProtectedFractionOfFairShareConfig(v float64, config configuration.SchedulingConfig) configuration.SchedulingConfig {
	config.Preemption.ProtectedFractionOfFairShare = v
	return config
}

func WithDominantResourceFairnessConfig(config configuration.SchedulingConfig) configuration.SchedulingConfig {
	config.FairnessModel = configuration.DominantResourceFairness
	return config
}

func WithNodeEvictionProbabilityConfig(p float64, config configuration.SchedulingConfig) configuration.SchedulingConfig {
	config.Preemption.NodeEvictionProbability = p
	return config
}

func WithNodeOversubscriptionEvictionProbabilityConfig(p float64, config configuration.SchedulingConfig) configuration.SchedulingConfig {
	config.Preemption.NodeOversubscriptionEvictionProbability = p
	return config
}

func WithRoundLimitsConfig(limits map[string]float64, config configuration.SchedulingConfig) configuration.SchedulingConfig {
	config.MaximumResourceFractionToSchedule = limits
	return config
}

func WithRoundLimitsPoolConfig(limits map[string]map[string]float64, config configuration.SchedulingConfig) configuration.SchedulingConfig {
	config.MaximumResourceFractionToScheduleByPool = limits
	return config
}

func WithPerPriorityLimitsConfig(limits map[string]map[string]float64, config configuration.SchedulingConfig) configuration.SchedulingConfig {
	for priorityClassName, limit := range limits {
		priorityClass, ok := config.Preemption.PriorityClasses[priorityClassName]
		if !ok {
			panic(fmt.Sprintf("no priority class with name %s", priorityClassName))
		}
		// We need to make a copy to avoid mutating the priorityClasses, which are used by other tests too.
		config.Preemption.PriorityClasses[priorityClassName] = types.PriorityClass{
			Priority:                              priorityClass.Priority,
			Preemptible:                           priorityClass.Preemptible,
			MaximumResourceFractionPerQueue:       limit,
			MaximumResourceFractionPerQueueByPool: priorityClass.MaximumResourceFractionPerQueueByPool,
		}
	}
	return config
}

func WithIndexedResourcesConfig(indexResources []configuration.IndexedResource, config configuration.SchedulingConfig) configuration.SchedulingConfig {
	config.IndexedResources = indexResources
	return config
}

func WithMaxJobsToScheduleConfig(n uint, config configuration.SchedulingConfig) configuration.SchedulingConfig {
	config.MaximumJobsToSchedule = n
	return config
}

func WithMaxGangsToScheduleConfig(n uint, config configuration.SchedulingConfig) configuration.SchedulingConfig {
	config.MaximumGangsToSchedule = n
	return config
}

func WithMaxLookbackPerQueueConfig(n uint, config configuration.SchedulingConfig) configuration.SchedulingConfig {
	// For legacy reasons, it's called QueueLeaseBatchSize in config.
	config.MaxQueueLookback = n
	return config
}

func WithIndexedTaintsConfig(indexedTaints []string, config configuration.SchedulingConfig) configuration.SchedulingConfig {
	config.IndexedTaints = append(config.IndexedTaints, indexedTaints...)
	return config
}

func WithIndexedNodeLabelsConfig(indexedNodeLabels []string, config configuration.SchedulingConfig) configuration.SchedulingConfig {
	config.IndexedNodeLabels = append(config.IndexedNodeLabels, indexedNodeLabels...)
	return config
}

func WithMaxQueueLookbackConfig(maxQueueLookback uint, config configuration.SchedulingConfig) configuration.SchedulingConfig {
	config.MaxQueueLookback = maxQueueLookback
	return config
}

func WithUsedResourcesNodes(p int32, rl schedulerobjects.ResourceList, nodes []*schedulerobjects.Node) []*schedulerobjects.Node {
	for _, node := range nodes {
		schedulerobjects.AllocatableByPriorityAndResourceType(node.AllocatableByPriorityAndResource).MarkAllocated(p, rl)
	}
	return nodes
}

func WithNodeTypeIdNodes(nodeTypeId uint64, nodes []*schedulerobjects.Node) []*schedulerobjects.Node {
	for _, node := range nodes {
		node.NodeTypeId = nodeTypeId
	}
	return nodes
}

func WithLabelsNodes(labels map[string]string, nodes []*schedulerobjects.Node) []*schedulerobjects.Node {
	for _, node := range nodes {
		if node.Labels == nil {
			node.Labels = maps.Clone(labels)
		} else {
			maps.Copy(node.Labels, labels)
		}
	}
	return nodes
}

func WithNodeSelectorPodReqs(selector map[string]string, reqs []*schedulerobjects.PodRequirements) []*schedulerobjects.PodRequirements {
	for _, req := range reqs {
		req.NodeSelector = maps.Clone(selector)
	}
	return reqs
}

func WithNodeSelectorPodReq(selector map[string]string, req *schedulerobjects.PodRequirements) *schedulerobjects.PodRequirements {
	req.NodeSelector = maps.Clone(selector)
	return req
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

func WithGangAnnotationsPodReqs(reqs []*schedulerobjects.PodRequirements) []*schedulerobjects.PodRequirements {
	gangId := uuid.NewString()
	gangCardinality := fmt.Sprintf("%d", len(reqs))
	return WithAnnotationsPodReqs(
		map[string]string{configuration.GangIdAnnotation: gangId, configuration.GangCardinalityAnnotation: gangCardinality},
		reqs,
	)
}

func WithAnnotationsPodReqs(annotations map[string]string, reqs []*schedulerobjects.PodRequirements) []*schedulerobjects.PodRequirements {
	for _, req := range reqs {
		if req.Annotations == nil {
			req.Annotations = make(map[string]string)
		}
		maps.Copy(req.Annotations, annotations)
	}
	return reqs
}

func WithRequestsJobs(rl schedulerobjects.ResourceList, jobs []*jobdb.Job) []*jobdb.Job {
	for _, job := range jobs {
		for _, req := range job.JobSchedulingInfo().GetObjectRequirements() {
			maps.Copy(
				req.GetPodRequirements().ResourceRequirements.Requests,
				schedulerobjects.V1ResourceListFromResourceList(rl),
			)
		}
	}
	return jobs
}

func WithNodeSelectorJobs(selector map[string]string, jobs []*jobdb.Job) []*jobdb.Job {
	for _, job := range jobs {
		for _, req := range job.JobSchedulingInfo().GetObjectRequirements() {
			req.GetPodRequirements().NodeSelector = maps.Clone(selector)
		}
	}
	return jobs
}

func WithNodeSelectorJob(selector map[string]string, job *jobdb.Job) *jobdb.Job {
	for _, req := range job.JobSchedulingInfo().GetObjectRequirements() {
		req.GetPodRequirements().NodeSelector = maps.Clone(selector)
	}
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

func WithAnnotationsJobs(annotations map[string]string, jobs []*jobdb.Job) []*jobdb.Job {
	for _, job := range jobs {
		for _, req := range job.JobSchedulingInfo().GetObjectRequirements() {
			if req.GetPodRequirements().Annotations == nil {
				req.GetPodRequirements().Annotations = make(map[string]string)
			}
			maps.Copy(req.GetPodRequirements().Annotations, annotations)
		}
	}
	return jobs
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

func TestJob(queue string, jobId ulid.ULID, priorityClassName string, req *schedulerobjects.PodRequirements) *jobdb.Job {
	created := jobTimestamp.Add(1)
	submitTime := time.Time{}.Add(time.Millisecond * time.Duration(created))
	return jobdb.NewJob(
		jobId.String(),
		TestJobset,
		queue,
		// This is the per-queue priority of this job, which is unrelated to `priorityClassName`.
		1000,
		&schedulerobjects.JobSchedulingInfo{
			PriorityClassName: priorityClassName,
			SubmitTime:        submitTime,
			ObjectRequirements: []*schedulerobjects.ObjectRequirements{
				{
					Requirements: &schedulerobjects.ObjectRequirements_PodRequirements{
						PodRequirements: req,
					},
				},
			},
		},
		false,
		0,
		false,
		false,
		false,
		created,
	)
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

func N1CpuPodReqs(queue string, priority int32, n int) []*schedulerobjects.PodRequirements {
	rv := make([]*schedulerobjects.PodRequirements, n)
	for i := 0; i < n; i++ {
		rv[i] = Test1Cpu4GiPodReqs(queue, util.ULID(), priority)
	}
	return rv
}

func N16CpuPodReqs(queue string, priority int32, n int) []*schedulerobjects.PodRequirements {
	rv := make([]*schedulerobjects.PodRequirements, n)
	for i := 0; i < n; i++ {
		rv[i] = Test16Cpu128GiPodReqs(queue, util.ULID(), priority)
	}
	return rv
}

func N32CpuPodReqs(queue string, priority int32, n int) []*schedulerobjects.PodRequirements {
	rv := make([]*schedulerobjects.PodRequirements, n)
	for i := 0; i < n; i++ {
		rv[i] = Test32Cpu256GiPodReqs(queue, util.ULID(), priority)
	}
	return rv
}

func N1GpuPodReqs(queue string, priority int32, n int) []*schedulerobjects.PodRequirements {
	rv := make([]*schedulerobjects.PodRequirements, n)
	for i := 0; i < n; i++ {
		rv[i] = Test1GpuPodReqs(queue, util.ULID(), priority)
	}
	return rv
}

func TestPodReqs(queue string, jobId ulid.ULID, priority int32, requests v1.ResourceList) *schedulerobjects.PodRequirements {
	return &schedulerobjects.PodRequirements{
		Priority:             priority,
		ResourceRequirements: v1.ResourceRequirements{Requests: requests},
		Annotations:          make(map[string]string),
		NodeSelector:         make(map[string]string),
	}
}

func Test1Cpu4GiPodReqs(queue string, jobId ulid.ULID, priority int32) *schedulerobjects.PodRequirements {
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

func Test1Cpu16GiPodReqs(queue string, jobId ulid.ULID, priority int32) *schedulerobjects.PodRequirements {
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

func Test16Cpu128GiPodReqs(queue string, jobId ulid.ULID, priority int32) *schedulerobjects.PodRequirements {
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

func Test32Cpu256GiPodReqs(queue string, jobId ulid.ULID, priority int32) *schedulerobjects.PodRequirements {
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

func Test1GpuPodReqs(queue string, jobId ulid.ULID, priority int32) *schedulerobjects.PodRequirements {
	req := TestPodReqs(
		queue,
		jobId,
		priority,
		v1.ResourceList{
			"cpu":    resource.MustParse("8"),
			"memory": resource.MustParse("128Gi"),
			"gpu":    resource.MustParse("1"),
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

func TestUnitReqs(priority int32) *schedulerobjects.PodRequirements {
	return &schedulerobjects.PodRequirements{
		Priority: priority,
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

func TestCluster() []*schedulerobjects.Node {
	return []*schedulerobjects.Node{
		{
			Id:         "node1",
			NodeTypeId: 1,
			NodeType:   &schedulerobjects.NodeType{Id: 1},
			AllocatableByPriorityAndResource: map[int32]schedulerobjects.ResourceList{
				0: {Resources: map[string]resource.Quantity{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Gi")}},
				1: {Resources: map[string]resource.Quantity{"cpu": resource.MustParse("2"), "memory": resource.MustParse("2Gi")}},
				2: {Resources: map[string]resource.Quantity{"cpu": resource.MustParse("3"), "memory": resource.MustParse("3Gi")}},
			},
			TotalResources: schedulerobjects.ResourceList{
				Resources: map[string]resource.Quantity{
					"cpu":    resource.MustParse("3"),
					"memory": resource.MustParse("3Gi"),
				},
			},
			Labels: map[string]string{
				TestHostnameLabel: "node1",
			},
		},
		{
			Id:         "node2",
			NodeTypeId: 2,
			NodeType:   &schedulerobjects.NodeType{Id: 2},
			AllocatableByPriorityAndResource: map[int32]schedulerobjects.ResourceList{
				0: {Resources: map[string]resource.Quantity{"cpu": resource.MustParse("4"), "memory": resource.MustParse("4Gi")}},
				1: {Resources: map[string]resource.Quantity{"cpu": resource.MustParse("5"), "memory": resource.MustParse("5Gi")}},
				2: {Resources: map[string]resource.Quantity{"cpu": resource.MustParse("6"), "memory": resource.MustParse("6Gi")}},
			},
			TotalResources: schedulerobjects.ResourceList{
				Resources: map[string]resource.Quantity{
					"cpu":    resource.MustParse("6"),
					"memory": resource.MustParse("6Gi"),
				},
			},
			Labels: map[string]string{
				TestHostnameLabel: "node2",
			},
		},
		{
			Id:         "node3",
			NodeTypeId: 3,
			NodeType:   &schedulerobjects.NodeType{Id: 3},
			AllocatableByPriorityAndResource: map[int32]schedulerobjects.ResourceList{
				0: {Resources: map[string]resource.Quantity{"cpu": resource.MustParse("7"), "memory": resource.MustParse("7Gi")}},
				1: {Resources: map[string]resource.Quantity{"cpu": resource.MustParse("8"), "memory": resource.MustParse("8Gi")}},
				2: {Resources: map[string]resource.Quantity{"cpu": resource.MustParse("9"), "memory": resource.MustParse("9Gi")}},
			},
			TotalResources: schedulerobjects.ResourceList{
				Resources: map[string]resource.Quantity{
					"cpu":    resource.MustParse("9"),
					"memory": resource.MustParse("9Gi"),
				},
			},
			Labels: map[string]string{
				TestHostnameLabel: "node3",
			},
		},
	}
}

func N32CpuNodes(n int, priorities []int32) []*schedulerobjects.Node {
	rv := make([]*schedulerobjects.Node, n)
	for i := 0; i < n; i++ {
		rv[i] = Test32CpuNode(priorities)
	}
	return rv
}

func NTainted32CpuNodes(n int, priorities []int32) []*schedulerobjects.Node {
	rv := make([]*schedulerobjects.Node, n)
	for i := 0; i < n; i++ {
		rv[i] = TestTainted32CpuNode(priorities)
	}
	return rv
}

func N8GpuNodes(n int, priorities []int32) []*schedulerobjects.Node {
	rv := make([]*schedulerobjects.Node, n)
	for i := 0; i < n; i++ {
		rv[i] = Test8GpuNode(priorities)
	}
	return rv
}

func TestNode(priorities []int32, resources map[string]resource.Quantity) *schedulerobjects.Node {
	id := uuid.NewString()
	return &schedulerobjects.Node{
		Id:             id,
		Name:           id,
		TotalResources: schedulerobjects.ResourceList{Resources: resources},
		AllocatableByPriorityAndResource: schedulerobjects.NewAllocatableByPriorityAndResourceType(
			priorities,
			schedulerobjects.ResourceList{Resources: resources},
		),
		StateByJobRunId: make(map[string]schedulerobjects.JobRunState),
		Labels: map[string]string{
			TestHostnameLabel: id,
		},
	}
}

func Test32CpuNode(priorities []int32) *schedulerobjects.Node {
	return TestNode(
		priorities,
		map[string]resource.Quantity{
			"cpu":    resource.MustParse("32"),
			"memory": resource.MustParse("256Gi"),
		},
	)
}

func TestTainted32CpuNode(priorities []int32) *schedulerobjects.Node {
	node := Test32CpuNode(priorities)
	node.Taints = []v1.Taint{
		{
			Key:    "largeJobsOnly",
			Value:  "true",
			Effect: v1.TaintEffectNoSchedule,
		},
	}
	node.Labels["largeJobsOnly"] = "true"
	return node
}

func Test8GpuNode(priorities []int32) *schedulerobjects.Node {
	node := TestNode(
		priorities,
		map[string]resource.Quantity{
			"cpu":    resource.MustParse("64"),
			"memory": resource.MustParse("1024Gi"),
			"gpu":    resource.MustParse("8"),
		},
	)
	node.Labels["gpu"] = "true"
	return node
}

func WithLastUpdateTimeExecutor(lastUpdateTime time.Time, executor *schedulerobjects.Executor) *schedulerobjects.Executor {
	executor.LastUpdateTime = lastUpdateTime
	return executor
}

func Test1Node32CoreExecutor(executorId string) *schedulerobjects.Executor {
	node := Test32CpuNode(TestPriorities)
	node.Name = fmt.Sprintf("%s-node", executorId)
	node.Executor = executorId
	return &schedulerobjects.Executor{
		Id:             executorId,
		Pool:           TestPool,
		Nodes:          []*schedulerobjects.Node{node},
		LastUpdateTime: BaseTime,
	}
}

func TestDbQueue() *database.Queue {
	return &database.Queue{
		Name:   TestQueue,
		Weight: 100,
	}
}

func TestQueuedJobDbJob() *jobdb.Job {
	return jobdb.
		EmptyJob(util.NewULID()).
		WithQueue(TestQueue).
		WithJobset(TestJobset).
		WithQueued(true).
		WithCreated(BaseTime.UnixNano()).
		WithJobSchedulingInfo(&schedulerobjects.JobSchedulingInfo{
			PriorityClassName: TestDefaultPriorityClass,
			SubmitTime:        BaseTime,
			ObjectRequirements: []*schedulerobjects.ObjectRequirements{
				{
					Requirements: &schedulerobjects.ObjectRequirements_PodRequirements{
						PodRequirements: TestUnitReqs(1),
					},
				},
			},
		})
}

func WithJobDbJobPodRequirements(job *jobdb.Job, reqs *schedulerobjects.PodRequirements) *jobdb.Job {
	return job.WithJobSchedulingInfo(&schedulerobjects.JobSchedulingInfo{
		PriorityClassName: job.JobSchedulingInfo().PriorityClassName,
		SubmitTime:        job.JobSchedulingInfo().SubmitTime,
		ObjectRequirements: []*schedulerobjects.ObjectRequirements{
			{
				Requirements: &schedulerobjects.ObjectRequirements_PodRequirements{
					PodRequirements: reqs,
				},
			},
		},
	})
}

func TestRunningJobDbJob(startTime int64) *jobdb.Job {
	return TestQueuedJobDbJob().
		WithQueued(false).
		WithUpdatedRun(jobdb.MinimalRun(uuid.New(), startTime))
}
