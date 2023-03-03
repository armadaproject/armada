package scheduler

// This file contains test fixtures to be used throughout the tests for this package.
import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"golang.org/x/exp/maps"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

const (
	testJobset        = "testJobset"
	testQueue         = "testQueue"
	testPool          = "testPool"
	testHostnameLabel = "kubernetes.io/hostname"
)

var (
	baseTime, _         = time.Parse("2006-01-02T15:04:05.000Z", "2022-03-01T15:04:05.000Z")
	testPriorityClasses = map[string]configuration.PriorityClass{
		"priority-0": {0, true, nil},
		"priority-1": {1, true, nil},
		"priority-2": {2, true, nil},
		"priority-3": {3, false, nil},
	}
	testDefaultPriorityClass         = "priority-3"
	testPriorities                   = []int32{0, 1, 2, 3}
	testMaxExtraNodesToConsider uint = 1
	testResources                    = []string{"cpu", "memory", "gpu"}
	testIndexedTaints                = []string{"largeJobsOnly", "gpu"}
	testIndexedNodeLabels            = []string{"largeJobsOnly", "gpu"}
)

func intRange(a, b int) []int {
	rv := make([]int, b-a+1)
	for i := range rv {
		rv[i] = a + i
	}
	return rv
}

func repeat[T any](v T, n int) []T {
	rv := make([]T, n)
	for i := 0; i < n; i++ {
		rv[i] = v
	}
	return rv
}

func testSchedulingConfig() configuration.SchedulingConfig {
	return configuration.SchedulingConfig{
		ResourceScarcity: map[string]float64{"cpu": 1, "memory": 0},
		Preemption: configuration.PreemptionConfig{
			PriorityClasses:      maps.Clone(testPriorityClasses),
			DefaultPriorityClass: testDefaultPriorityClass,
		},
		IndexedResources: []string{"cpu", "memory"},
		ExecutorTimeout:  15 * time.Minute,
	}
}

func withRoundLimitsConfig(limits map[string]float64, config configuration.SchedulingConfig) configuration.SchedulingConfig {
	config.MaximalClusterFractionToSchedule = limits
	return config
}

func withPerQueueLimitsConfig(limits map[string]float64, config configuration.SchedulingConfig) configuration.SchedulingConfig {
	config.MaximalResourceFractionPerQueue = limits
	return config
}

func withPerPriorityLimitsConfig(limits map[int32]map[string]float64, config configuration.SchedulingConfig) configuration.SchedulingConfig {
	for k, v := range config.Preemption.PriorityClasses {
		config.Preemption.PriorityClasses[k] = configuration.PriorityClass{
			Priority:                        v.Priority,
			MaximalResourceFractionPerQueue: limits[v.Priority],
		}
	}
	return config
}

func withPerQueueRoundLimitsConfig(limits map[string]float64, config configuration.SchedulingConfig) configuration.SchedulingConfig {
	config.MaximalResourceFractionToSchedulePerQueue = limits
	return config
}

func withMaxJobsToScheduleConfig(n uint, config configuration.SchedulingConfig) configuration.SchedulingConfig {
	config.MaximumJobsToSchedule = n
	return config
}

func withMaxLookbackPerQueueConfig(n uint, config configuration.SchedulingConfig) configuration.SchedulingConfig {
	// For legacy reasons, it's called QueueLeaseBatchSize in config.
	config.QueueLeaseBatchSize = n
	return config
}

func withIndexedTaintsConfig(indexedTaints []string, config configuration.SchedulingConfig) configuration.SchedulingConfig {
	config.IndexedTaints = append(config.IndexedTaints, indexedTaints...)
	return config
}

func withIndexedNodeLabelsConfig(indexedNodeLabels []string, config configuration.SchedulingConfig) configuration.SchedulingConfig {
	config.IndexedNodeLabels = append(config.IndexedNodeLabels, indexedNodeLabels...)
	return config
}

func withPodReqsNodes(reqs map[int][]*schedulerobjects.PodRequirements, nodes []*schedulerobjects.Node) []*schedulerobjects.Node {
	for i := range nodes {
		for _, req := range reqs[i] {
			node, err := BindPodToNode(req, nodes[i])
			if err != nil {
				panic(err)
			}
			nodes[i] = node
		}
	}
	return nodes
}

func withQueueLeaseBatchSizeConfig(queueLeasebatchSize uint, config configuration.SchedulingConfig) configuration.SchedulingConfig {
	config.QueueLeaseBatchSize = queueLeasebatchSize
	return config
}

func withUsedResourcesNodes(p int32, rl schedulerobjects.ResourceList, nodes []*schedulerobjects.Node) []*schedulerobjects.Node {
	for _, node := range nodes {
		schedulerobjects.AllocatableByPriorityAndResourceType(node.AllocatableByPriorityAndResource).MarkAllocated(p, rl)
	}
	return nodes
}

func withLabelsNodes(labels map[string]string, nodes []*schedulerobjects.Node) []*schedulerobjects.Node {
	for _, node := range nodes {
		if node.Labels == nil {
			node.Labels = maps.Clone(labels)
		} else {
			maps.Copy(node.Labels, labels)
		}
	}
	return nodes
}

func withNodeSelectorPodReqs(selector map[string]string, reqs []*schedulerobjects.PodRequirements) []*schedulerobjects.PodRequirements {
	for _, req := range reqs {
		req.NodeSelector = maps.Clone(selector)
	}
	return reqs
}

func withNodeAffinityPodReqs(nodeSelectorTerms []v1.NodeSelectorTerm, reqs []*schedulerobjects.PodRequirements) []*schedulerobjects.PodRequirements {
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

func withGangAnnotationsPodReqs(reqs []*schedulerobjects.PodRequirements) []*schedulerobjects.PodRequirements {
	gangId := uuid.NewString()
	gangCardinality := fmt.Sprintf("%d", len(reqs))
	return withAnnotationsPodReqs(
		map[string]string{configuration.GangIdAnnotation: gangId, configuration.GangCardinalityAnnotation: gangCardinality},
		reqs,
	)
}

func withAnnotationsPodReqs(annotations map[string]string, reqs []*schedulerobjects.PodRequirements) []*schedulerobjects.PodRequirements {
	for _, req := range reqs {
		if req.Annotations == nil {
			req.Annotations = make(map[string]string)
		}
		maps.Copy(req.Annotations, annotations)
	}
	return reqs
}

func withRequestsPodReqs(rl schedulerobjects.ResourceList, reqs []*schedulerobjects.PodRequirements) []*schedulerobjects.PodRequirements {
	for _, req := range reqs {
		maps.Copy(
			req.ResourceRequirements.Requests,
			schedulerobjects.V1ResourceListFromResourceList(rl),
		)
	}
	return reqs
}

func testNSmallCpuJob(queue string, priority int32, n int) []*schedulerobjects.PodRequirements {
	rv := make([]*schedulerobjects.PodRequirements, n)
	for i := 0; i < n; i++ {
		rv[i] = testSmallCpuJob(queue, priority)
	}
	return rv
}

func testNLargeCpuJob(queue string, priority int32, n int) []*schedulerobjects.PodRequirements {
	rv := make([]*schedulerobjects.PodRequirements, n)
	for i := 0; i < n; i++ {
		rv[i] = testLargeCpuJob(queue, priority)
	}
	return rv
}

func testNGpuJob(queue string, priority int32, n int) []*schedulerobjects.PodRequirements {
	rv := make([]*schedulerobjects.PodRequirements, n)
	for i := 0; i < n; i++ {
		rv[i] = testGpuJob(queue, priority)
	}
	return rv
}

func testSmallCpuJob(queue string, priority int32) *schedulerobjects.PodRequirements {
	return &schedulerobjects.PodRequirements{
		Priority: priority,
		ResourceRequirements: v1.ResourceRequirements{
			Requests: v1.ResourceList{
				"cpu":    resource.MustParse("1"),
				"memory": resource.MustParse("4Gi"),
			},
		},
		Annotations: map[string]string{
			JobIdAnnotation: util.NewULID(),
			QueueAnnotation: queue,
		},
	}
}

func testLargeCpuJob(queue string, priority int32) *schedulerobjects.PodRequirements {
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
			JobIdAnnotation: util.NewULID(),
			QueueAnnotation: queue,
		},
	}
}

func testGpuJob(queue string, priority int32) *schedulerobjects.PodRequirements {
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
			JobIdAnnotation: util.NewULID(),
			QueueAnnotation: queue,
		},
	}
}

func testUnitReqs(priority int32) *schedulerobjects.PodRequirements {
	return &schedulerobjects.PodRequirements{
		Priority: priority,
		ResourceRequirements: v1.ResourceRequirements{
			Requests: v1.ResourceList{
				"cpu":    resource.MustParse("1"),
				"memory": resource.MustParse("1Gi"),
			},
		},
		Annotations: map[string]string{
			JobIdAnnotation: util.NewULID(),
			QueueAnnotation: testQueue,
		},
	}
}

func testCluster() []*schedulerobjects.Node {
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
				testHostnameLabel: "node1",
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
				testHostnameLabel: "node2",
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
				testHostnameLabel: "node3",
			},
		},
	}
}

func testNCpuNode(n int, priorities []int32) []*schedulerobjects.Node {
	rv := make([]*schedulerobjects.Node, n)
	for i := 0; i < n; i++ {
		rv[i] = testCpuNode(priorities)
	}
	return rv
}

func testNTaintedCpuNode(n int, priorities []int32) []*schedulerobjects.Node {
	rv := make([]*schedulerobjects.Node, n)
	for i := 0; i < n; i++ {
		rv[i] = testTaintedCpuNode(priorities)
	}
	return rv
}

func testNGpuNode(n int, priorities []int32) []*schedulerobjects.Node {
	rv := make([]*schedulerobjects.Node, n)
	for i := 0; i < n; i++ {
		rv[i] = testGpuNode(priorities)
	}
	return rv
}

func testCpuNode(priorities []int32) *schedulerobjects.Node {
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
			testHostnameLabel: id,
		},
	}
}

func testTaintedCpuNode(priorities []int32) *schedulerobjects.Node {
	id := uuid.NewString()
	taints := []v1.Taint{
		{
			Key:    "largeJobsOnly",
			Value:  "true",
			Effect: v1.TaintEffectNoSchedule,
		},
	}
	labels := map[string]string{
		testHostnameLabel: id,
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

func testGpuNode(priorities []int32) *schedulerobjects.Node {
	id := uuid.NewString()
	labels := map[string]string{
		testHostnameLabel: id,
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

func testDbQueue() *database.Queue {
	return &database.Queue{
		Name:   testQueue,
		Weight: 100,
	}
}

func createNodeDb(nodes []*schedulerobjects.Node) (*NodeDb, error) {
	db, err := NewNodeDb(
		testPriorityClasses,
		testMaxExtraNodesToConsider,
		testResources,
		testIndexedTaints,
		testIndexedNodeLabels,
	)
	if err != nil {
		return nil, err
	}
	if err := db.UpsertMany(nodes); err != nil {
		return nil, err
	}
	return db, nil
}

func testQueuedJobDbJob() *jobdb.Job {
	return jobdb.
		EmptyJob(util.NewULID()).
		WithQueue(testQueue).
		WithJobset(testJobset).
		WithQueued(true).
		WithCreated(baseTime.UnixNano()).
		WithJobSchedulingInfo(&schedulerobjects.JobSchedulingInfo{
			PriorityClassName: testDefaultPriorityClass,
			SubmitTime:        baseTime,
			ObjectRequirements: []*schedulerobjects.ObjectRequirements{
				{
					Requirements: &schedulerobjects.ObjectRequirements_PodRequirements{
						PodRequirements: testUnitReqs(1),
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

func testRunningJobDbJob(startTime int64) *jobdb.Job {
	return testQueuedJobDbJob().
		WithQueued(false).
		WithUpdatedRun(jobdb.MinimalRun(uuid.New(), startTime))
}
