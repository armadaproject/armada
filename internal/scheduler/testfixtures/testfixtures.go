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
	"github.com/sirupsen/logrus"
	"golang.org/x/exp/maps"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common/util"
	schedulerconfig "github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

const (
	TestJobset        = "testJobset"
	TestQueue         = "testQueue"
	TestPool          = "testPool"
	TestHostnameLabel = "kubernetes.io/hostname"
	PriorityClass0    = "priority-0"
	PriorityClass1    = "priority-1"
	PriorityClass2    = "priority-2"
	PriorityClass3    = "priority-3"
)

var (
	BaseTime, _         = time.Parse("2006-01-02T15:04:05.000Z", "2022-03-01T15:04:05.000Z")
	TestPriorityClasses = map[string]configuration.PriorityClass{
		PriorityClass0: {0, true, nil},
		PriorityClass1: {1, true, nil},
		PriorityClass2: {2, true, nil},
		PriorityClass3: {3, false, nil},
	}
	TestDefaultPriorityClass         = "priority-3"
	TestPriorities                   = []int32{0, 1, 2, 3}
	TestMaxExtraNodesToConsider uint = 1
	TestResources                    = []string{"cpu", "memory", "gpu"}
	TestIndexedTaints                = []string{"largeJobsOnly", "gpu"}
	TestIndexedNodeLabels            = []string{"largeJobsOnly", "gpu"}
	jobTimestamp                atomic.Int64
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
		ResourceScarcity: map[string]float64{"cpu": 1, "memory": 0},
		Preemption: configuration.PreemptionConfig{
			PriorityClasses:                         maps.Clone(TestPriorityClasses),
			DefaultPriorityClass:                    TestDefaultPriorityClass,
			NodeEvictionProbability:                 1.0,
			NodeOversubscriptionEvictionProbability: 1.0,
		},
		IndexedResources:                 []string{"cpu", "memory"},
		ExecutorTimeout:                  15 * time.Minute,
		MaxUnacknowledgedJobsPerExecutor: math.MaxInt,
	}
}

func WithMaxUnacknowledgedJobsPerExecutor(i uint, config configuration.SchedulingConfig) configuration.SchedulingConfig {
	config.MaxUnacknowledgedJobsPerExecutor = i
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
	config.MaximalClusterFractionToSchedule = limits
	return config
}

func WithPerQueueLimitsConfig(limits map[string]float64, config configuration.SchedulingConfig) configuration.SchedulingConfig {
	config.MaximalResourceFractionPerQueue = limits
	return config
}

func WithPerPriorityLimitsConfig(limits map[int32]map[string]float64, config configuration.SchedulingConfig) configuration.SchedulingConfig {
	for k, v := range config.Preemption.PriorityClasses {
		config.Preemption.PriorityClasses[k] = configuration.PriorityClass{
			Priority:                        v.Priority,
			MaximalResourceFractionPerQueue: limits[v.Priority],
		}
	}
	return config
}

func WithPerQueueRoundLimitsConfig(limits map[string]float64, config configuration.SchedulingConfig) configuration.SchedulingConfig {
	config.MaximalResourceFractionToSchedulePerQueue = limits
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
	config.QueueLeaseBatchSize = n
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

func WithQueueLeaseBatchSizeConfig(queueLeasebatchSize uint, config configuration.SchedulingConfig) configuration.SchedulingConfig {
	config.QueueLeaseBatchSize = queueLeasebatchSize
	return config
}

func WithUsedResourcesNodes(p int32, rl schedulerobjects.ResourceList, nodes []*schedulerobjects.Node) []*schedulerobjects.Node {
	for _, node := range nodes {
		schedulerobjects.AllocatableByPriorityAndResourceType(node.AllocatableByPriorityAndResource).MarkAllocated(p, rl)
	}
	return nodes
}

func WithNodeTypeIdNodes(nodeTypeId string, nodes []*schedulerobjects.Node) []*schedulerobjects.Node {
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

func WithNodeSelectorJobs(selector map[string]string, jobs []*jobdb.Job) []*jobdb.Job {
	for _, job := range jobs {
		for _, req := range job.GetRequirements(nil).GetObjectRequirements() {
			req.GetPodRequirements().NodeSelector = maps.Clone(selector)
		}
	}
	return jobs
}

func WithNodeSelectorPodReq(selector map[string]string, req *schedulerobjects.PodRequirements) *schedulerobjects.PodRequirements {
	req.NodeSelector = maps.Clone(selector)
	return req
}

func WithNodeAffinityPodReqs(nodeSelectorTerms []v1.NodeSelectorTerm, reqs []*schedulerobjects.PodRequirements) []*schedulerobjects.PodRequirements {
	for _, req := range reqs {
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
	return reqs
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
		for _, req := range job.GetRequirements(nil).GetObjectRequirements() {
			if req.GetPodRequirements().Annotations == nil {
				req.GetPodRequirements().Annotations = make(map[string]string)
			}
			maps.Copy(req.GetPodRequirements().Annotations, annotations)
		}
	}
	return jobs
}

func WithRequestsPodReqs(rl schedulerobjects.ResourceList, reqs []*schedulerobjects.PodRequirements) []*schedulerobjects.PodRequirements {
	for _, req := range reqs {
		maps.Copy(
			req.ResourceRequirements.Requests,
			schedulerobjects.V1ResourceListFromResourceList(rl),
		)
	}
	return reqs
}

func NSmallCpuJob(queue string, priorityClassName string, n int) []*jobdb.Job {
	rv := make([]*jobdb.Job, n)
	for i := 0; i < n; i++ {
		rv[i] = SmallCpuJob(queue, priorityClassName)
	}
	return rv
}

func NLargeCpuJob(queue string, priorityClassName string, n int) []*jobdb.Job {
	rv := make([]*jobdb.Job, n)
	for i := 0; i < n; i++ {
		rv[i] = LargeCpuJob(queue, priorityClassName)
	}
	return rv
}

func NGpuJob(queue string, priorityClassName string, n int) []*jobdb.Job {
	rv := make([]*jobdb.Job, n)
	for i := 0; i < n; i++ {
		rv[i] = GpuJob(queue, priorityClassName)
	}
	return rv
}

func SmallCpuJob(queue string, priorityClassName string) *jobdb.Job {
	jobId := uuid.NewString()
	priorityClass, ok := TestPriorityClasses[priorityClassName]
	if !ok {
		panic(fmt.Sprintf("no priority class with name %s", priorityClassName))
	}
	req := &schedulerobjects.PodRequirements{
		Priority: priorityClass.Priority,
		ResourceRequirements: v1.ResourceRequirements{
			Requests: v1.ResourceList{
				"cpu":    resource.MustParse("1"),
				"memory": resource.MustParse("4Gi"),
			},
		},
		Annotations: map[string]string{
			schedulerconfig.JobIdAnnotation: jobId,
			schedulerconfig.QueueAnnotation: queue,
		},
		NodeSelector: make(map[string]string),
	}
	created := jobTimestamp.Add(1)
	submitTime := time.Time{}.Add(time.Millisecond * time.Duration(created))
	return jobdb.NewJob(
		jobId,
		"",
		queue,
		0,
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

func LargeCpuJob(queue string, priorityClassName string) *jobdb.Job {
	jobId := uuid.NewString()
	priorityClass, ok := TestPriorityClasses[priorityClassName]
	if !ok {
		panic(fmt.Sprintf("no priority class with name %s", priorityClassName))
	}
	req := &schedulerobjects.PodRequirements{
		Priority: priorityClass.Priority,
		ResourceRequirements: v1.ResourceRequirements{
			Requests: v1.ResourceList{
				"cpu":    resource.MustParse("32"),
				"memory": resource.MustParse("256Gi"),
			},
		},
		Annotations: map[string]string{
			schedulerconfig.JobIdAnnotation: jobId,
			schedulerconfig.QueueAnnotation: queue,
		},
		Tolerations: []v1.Toleration{
			{
				Key:   "largeJobsOnly",
				Value: "true",
			},
		},
		NodeSelector: make(map[string]string),
	}
	created := jobTimestamp.Add(1)
	submitTime := time.Time{}.Add(time.Millisecond * time.Duration(created))
	return jobdb.NewJob(
		jobId,
		"",
		queue,
		0,
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

func GpuJob(queue string, priorityClassName string) *jobdb.Job {
	jobId := uuid.NewString()
	priorityClass, ok := TestPriorityClasses[priorityClassName]
	if !ok {
		panic(fmt.Sprintf("no priority class with name %s", priorityClassName))
	}
	req := &schedulerobjects.PodRequirements{
		Priority: priorityClass.Priority,
		ResourceRequirements: v1.ResourceRequirements{
			Requests: v1.ResourceList{
				"cpu":    resource.MustParse("4"),
				"memory": resource.MustParse("16Gi"),
				"gpu":    resource.MustParse("1"),
			},
		},
		Tolerations: []v1.Toleration{
			{
				Key:   "gpu",
				Value: "true",
			},
		},
		Annotations: map[string]string{
			schedulerconfig.JobIdAnnotation: jobId,
			schedulerconfig.QueueAnnotation: queue,
		},
		NodeSelector: make(map[string]string),
	}
	created := jobTimestamp.Add(1)
	submitTime := time.Time{}.Add(time.Millisecond * time.Duration(created))
	return jobdb.NewJob(
		jobId,
		"",
		queue,
		0,
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

func TestNSmallCpuJob(queue string, priority int32, n int) []*schedulerobjects.PodRequirements {
	rv := make([]*schedulerobjects.PodRequirements, n)
	for i := 0; i < n; i++ {
		rv[i] = TestSmallCpuJob(queue, priority)
	}
	return rv
}

func TestNLargeCpuJob(queue string, priority int32, n int) []*schedulerobjects.PodRequirements {
	rv := make([]*schedulerobjects.PodRequirements, n)
	for i := 0; i < n; i++ {
		rv[i] = TestLargeCpuJob(queue, priority)
	}
	return rv
}

func TestNGpuJob(queue string, priority int32, n int) []*schedulerobjects.PodRequirements {
	rv := make([]*schedulerobjects.PodRequirements, n)
	for i := 0; i < n; i++ {
		rv[i] = TestGpuJob(queue, priority)
	}
	return rv
}

func TestSmallCpuJob(queue string, priority int32) *schedulerobjects.PodRequirements {
	return &schedulerobjects.PodRequirements{
		Priority: priority,
		ResourceRequirements: v1.ResourceRequirements{
			Requests: v1.ResourceList{
				"cpu":    resource.MustParse("1"),
				"memory": resource.MustParse("4Gi"),
			},
		},
		Annotations: map[string]string{
			schedulerconfig.JobIdAnnotation: util.NewULID(),
			schedulerconfig.QueueAnnotation: queue,
		},
	}
}

func TestLargeCpuJob(queue string, priority int32) *schedulerobjects.PodRequirements {
	return &schedulerobjects.PodRequirements{
		Priority: priority,
		ResourceRequirements: v1.ResourceRequirements{
			Requests: v1.ResourceList{
				"cpu":    resource.MustParse("32"),
				"memory": resource.MustParse("256Gi"),
			},
		},
		Tolerations: []v1.Toleration{
			{
				Key:   "largeJobsOnly",
				Value: "true",
			},
		},
		Annotations: map[string]string{
			schedulerconfig.JobIdAnnotation: util.NewULID(),
			schedulerconfig.QueueAnnotation: queue,
		},
	}
}

func TestGpuJob(queue string, priority int32) *schedulerobjects.PodRequirements {
	return &schedulerobjects.PodRequirements{
		Priority: priority,
		ResourceRequirements: v1.ResourceRequirements{
			Requests: v1.ResourceList{
				"cpu":    resource.MustParse("4"),
				"memory": resource.MustParse("16Gi"),
				"gpu":    resource.MustParse("1"),
			},
		},
		Tolerations: []v1.Toleration{
			{
				Key:   "gpu",
				Value: "true",
			},
		},
		Annotations: map[string]string{
			schedulerconfig.JobIdAnnotation: util.NewULID(),
			schedulerconfig.QueueAnnotation: queue,
		},
	}
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
		Annotations: map[string]string{
			schedulerconfig.JobIdAnnotation: util.NewULID(),
			schedulerconfig.QueueAnnotation: TestQueue,
		},
	}
}

func TestCluster() []*schedulerobjects.Node {
	return []*schedulerobjects.Node{
		{
			Id:         "node1",
			NodeTypeId: "foo",
			NodeType:   &schedulerobjects.NodeType{Id: "foo"},
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
			NodeTypeId: "foo",
			NodeType:   &schedulerobjects.NodeType{Id: "foo"},
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
			NodeTypeId: "bar",
			NodeType:   &schedulerobjects.NodeType{Id: "bar"},
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

func TestNCpuNode(n int, priorities []int32) []*schedulerobjects.Node {
	rv := make([]*schedulerobjects.Node, n)
	for i := 0; i < n; i++ {
		rv[i] = TestCpuNode(priorities)
	}
	return rv
}

func TestNTaintedCpuNode(n int, priorities []int32) []*schedulerobjects.Node {
	rv := make([]*schedulerobjects.Node, n)
	for i := 0; i < n; i++ {
		rv[i] = TestTaintedCpuNode(priorities)
	}
	return rv
}

func TestNGpuNode(n int, priorities []int32) []*schedulerobjects.Node {
	rv := make([]*schedulerobjects.Node, n)
	for i := 0; i < n; i++ {
		rv[i] = TestGpuNode(priorities)
	}
	return rv
}

func TestCpuNode(priorities []int32) *schedulerobjects.Node {
	id := uuid.NewString()
	return &schedulerobjects.Node{
		Id: id,
		TotalResources: schedulerobjects.ResourceList{
			Resources: map[string]resource.Quantity{
				"cpu":    resource.MustParse("32"),
				"memory": resource.MustParse("256Gi"),
			},
		},
		AllocatableByPriorityAndResource: schedulerobjects.NewAllocatableByPriorityAndResourceType(
			priorities,
			schedulerobjects.ResourceList{
				Resources: map[string]resource.Quantity{
					"cpu":    resource.MustParse("32"),
					"memory": resource.MustParse("256Gi"),
				},
			},
		),
		Labels: map[string]string{
			TestHostnameLabel: id,
		},
	}
}

func TestTaintedCpuNode(priorities []int32) *schedulerobjects.Node {
	id := uuid.NewString()
	taints := []v1.Taint{
		{
			Key:    "largeJobsOnly",
			Value:  "true",
			Effect: v1.TaintEffectNoSchedule,
		},
	}
	labels := map[string]string{
		TestHostnameLabel: id,
		"largeJobsOnly":   "true",
	}
	return &schedulerobjects.Node{
		Id:     id,
		Taints: taints,
		Labels: labels,
		TotalResources: schedulerobjects.ResourceList{
			Resources: map[string]resource.Quantity{
				"cpu":    resource.MustParse("32"),
				"memory": resource.MustParse("256Gi"),
			},
		},
		AllocatableByPriorityAndResource: schedulerobjects.NewAllocatableByPriorityAndResourceType(
			priorities,
			schedulerobjects.ResourceList{
				Resources: map[string]resource.Quantity{
					"cpu":    resource.MustParse("32"),
					"memory": resource.MustParse("256Gi"),
				},
			},
		),
	}
}

func TestGpuNode(priorities []int32) *schedulerobjects.Node {
	id := uuid.NewString()
	labels := map[string]string{
		TestHostnameLabel: id,
		"gpu":             "true",
	}
	return &schedulerobjects.Node{
		Id:     id,
		Labels: labels,
		TotalResources: schedulerobjects.ResourceList{
			Resources: map[string]resource.Quantity{
				"cpu":    resource.MustParse("64"),
				"memory": resource.MustParse("1024Gi"),
				"gpu":    resource.MustParse("8"),
			},
		},
		AllocatableByPriorityAndResource: schedulerobjects.NewAllocatableByPriorityAndResourceType(
			priorities,
			schedulerobjects.ResourceList{
				Resources: map[string]resource.Quantity{
					"cpu":    resource.MustParse("64"),
					"memory": resource.MustParse("1024Gi"),
					"gpu":    resource.MustParse("8"),
				},
			},
		),
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
