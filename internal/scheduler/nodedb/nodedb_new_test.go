package nodedb

//
//import (
//	"fmt"
//	"math"
//	"testing"
//	"time"
//
//	"github.com/armadaproject/armada/internal/common/slices"
//	"github.com/armadaproject/armada/internal/common/types"
//	schedulerconfig "github.com/armadaproject/armada/internal/scheduler/configuration"
//	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
//	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
//	"github.com/armadaproject/armada/internal/scheduler/jobdb"
//	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
//	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
//	"github.com/elliotchance/orderedmap/v2"
//	"github.com/stretchr/testify/require"
//	orderedmap2 "github.com/wk8/go-ordered-map/v2"
//	v1 "k8s.io/api/core/v1"
//	k8sResource "k8s.io/apimachinery/pkg/api/resource"
//)
//
//type job struct {
//	validNodeTypes map[string]bool
//	resource       internaltypes.ResourceList
//}
//
//type node struct {
//	nodeType string
//	resource internaltypes.ResourceList
//}
//
//var (
//	validTypes = []string{"test-1", "test-2", "test-3", "test-4", "test-5", "test-6"}
//)
//
//func TestPerf(t *testing.T) {
//	factory, err := internaltypes.MakeResourceListFactory([]schedulerconfig.ResourceType{
//		{Name: "memory", Resolution: k8sResource.MustParse("1")},
//		{Name: "ephemeral-storage", Resolution: k8sResource.MustParse("1")},
//		{Name: "cpu", Resolution: k8sResource.MustParse("1m")},
//	})
//	require.NoError(t, err)
//
//	job1 := job{
//		validNodeTypes: map[string]bool{"test-1": true, "test-3": true, "test-5": true},
//		resource: factory.FromJobResourceListIgnoreUnknown(map[string]k8sResource.Quantity{
//			"cpu":               k8sResource.MustParse("1"),
//			"memory":            k8sResource.MustParse("700Gi"),
//			"ephemeral-storage": k8sResource.MustParse("6Gi"),
//		}),
//	}
//
//	nodeCount := 10000
//	nodes := make([]*node, 0, nodeCount)
//
//	for i := 0; i < nodeCount; i++ {
//		memory := "600Gi"
//		if i == nodeCount-1 {
//			memory = "800Gi"
//		}
//
//		nodes = append(nodes, &node{
//			nodeType: "test-2",
//			resource: factory.FromJobResourceListIgnoreUnknown(map[string]k8sResource.Quantity{
//				"cpu":               k8sResource.MustParse("1"),
//				"memory":            k8sResource.MustParse(memory),
//				"ephemeral-storage": k8sResource.MustParse("60Gi"),
//			}),
//		})
//	}
//
//	start := time.Now()
//
//	selectNode := -1
//	for i, node := range nodes {
//		if !job1.validNodeTypes[node.nodeType] {
//			continue
//		}
//
//		if exceeds := job1.resource.ExceedsAvailableFast(node.resource); !exceeds {
//			selectNode = i
//			break
//		}
//	}
//
//	fmt.Println(fmt.Sprintf("Selected node %d", selectNode))
//	fmt.Println(fmt.Sprintf("Time taken %s", time.Now().Sub(start)))
//}
//
//// Inserting: 44.680691ms, Iterating:  530.01µs, Deletion: 20.382502ms, Total: 65.593203ms
//// Inserting: 28.16342ms, Iterating:  593.183µs, Deletion: 15.928467ms, Total: 44.68507ms
//// Inserting: 36.426714ms, Iterating:  801.352µs, Deletion: 17.942768ms, Total: 55.170834ms
//func TestEvictingJobsPerf_OrderedMap_wk8(t *testing.T) {
//	numberOfJobs := 50000
//
//	jobContexts := make([]*schedulercontext.JobSchedulingContext, 0, numberOfJobs)
//
//	for i := 0; i < numberOfJobs; i++ {
//		job := testfixtures.Test1Cpu4GiJob("A", testfixtures.PriorityClass0)
//
//		jctx := schedulercontext.JobSchedulingContextFromJob(job)
//		jctx.IsEvicted = true
//
//		jobContexts = append(jobContexts, jctx)
//	}
//	m := orderedmap2.New[string, *schedulercontext.JobSchedulingContext]()
//
//	insertStart := time.Now()
//	for _, jctx := range jobContexts {
//		m.Set(jctx.JobId, jctx)
//	}
//	insertTimeTaken := time.Now().Sub(insertStart)
//
//	iterationStart := time.Now()
//
//	count := 0
//	for pair := m.Oldest(); pair != nil; pair = pair.Next() {
//		count++
//	}
//
//	if count != numberOfJobs {
//		panic("not expected number of jobs in iteration")
//	}
//	iterationTimeTaken := time.Now().Sub(iterationStart)
//
//	slices.Shuffle(jobContexts)
//
//	deletionStart := time.Now()
//	for _, jctx := range jobContexts {
//		m.Delete(jctx.JobId)
//	}
//	deletionTimeTaken := time.Now().Sub(deletionStart)
//
//	totalTimeTaken := insertTimeTaken + iterationTimeTaken + deletionTimeTaken
//	fmt.Println(fmt.Sprintf("Inserting: %s, Iterating:  %s, Deletion: %s, Total: %s", insertTimeTaken, iterationTimeTaken, deletionTimeTaken, totalTimeTaken))
//}
//
//// Inserting: 25.515083ms, Iterating:  501.403µs, Deletion: 13.525281ms, Total: 39.541767ms
//// Inserting: 28.036532ms, Iterating:  1.001241ms, Deletion: 18.990787ms, Total: 48.02856ms
//// Inserting: 22.440536ms, Iterating:  365.314µs, Deletion: 12.461751ms, Total: 35.267601ms
//func TestEvictingJobsPerf_OrderedMap_ElliotChance(t *testing.T) {
//	numberOfJobs := 50000
//
//	jobContexts := make([]*schedulercontext.JobSchedulingContext, 0, numberOfJobs)
//
//	for i := 0; i < numberOfJobs; i++ {
//		job := testfixtures.Test1Cpu4GiJob("A", testfixtures.PriorityClass0)
//
//		jctx := schedulercontext.JobSchedulingContextFromJob(job)
//		jctx.IsEvicted = true
//
//		jobContexts = append(jobContexts, jctx)
//	}
//
//	m := orderedmap.NewOrderedMap[string, *schedulercontext.JobSchedulingContext]()
//
//	insertStart := time.Now()
//	for _, jctx := range jobContexts {
//		m.Set(jctx.JobId, jctx)
//	}
//	insertTimeTaken := time.Now().Sub(insertStart)
//
//	iterationStart := time.Now()
//
//	count := 0
//	for el := m.Back(); el != nil; el = el.Prev() {
//		count++
//	}
//
//	if count != numberOfJobs {
//		panic("not expected number of jobs in iteration")
//	}
//	iterationTimeTaken := time.Now().Sub(iterationStart)
//
//	slices.Shuffle(jobContexts)
//
//	deletionStart := time.Now()
//	for _, jctx := range jobContexts {
//		m.Delete(jctx.JobId)
//	}
//	deletionTimeTaken := time.Now().Sub(deletionStart)
//
//	totalTimeTaken := insertTimeTaken + iterationTimeTaken + deletionTimeTaken
//	fmt.Println(fmt.Sprintf("Inserting: %s, Iterating:  %s, Deletion: %s, Total: %s", insertTimeTaken, iterationTimeTaken, deletionTimeTaken, totalTimeTaken))
//}
//
//// Inserting: 455.99968ms, Iterating:  6.616757ms, Deletion: 883.661519ms, Total: 1.346277956s
//// Inserting: 430.751692ms, Iterating:  6.917378ms, Deletion: 972.291583ms, Total: 1.409960653s
//// Inserting: 450.391007ms, Iterating:  6.873196ms, Deletion: 1.205725379s, Total: 1.662989582s
//func TestEvictingJobsPerf(t *testing.T) {
//	nodeDb, err := newNodeDbWithNodes([]*schedulerobjects.Node{})
//	require.NoError(t, err)
//
//	numberOfJobs := 50000
//
//	jobContexts := make([]*schedulercontext.JobSchedulingContext, 0, numberOfJobs)
//
//	for i := 0; i < numberOfJobs; i++ {
//		job := testfixtures.Test1Cpu4GiJob("A", testfixtures.PriorityClass0)
//
//		jctx := schedulercontext.JobSchedulingContextFromJob(job)
//		jctx.IsEvicted = true
//
//		jobContexts = append(jobContexts, jctx)
//	}
//
//	insertStart := time.Now()
//	txn := nodeDb.Txn(true)
//	for i, jctx := range jobContexts {
//		err = nodeDb.AddEvictedJobSchedulingContextWithTxn(txn, i, jctx)
//		require.NoError(t, err)
//	}
//	txn.Commit()
//	insertTimeTaken := time.Now().Sub(insertStart)
//
//	iterationStart := time.Now()
//	newTxn := nodeDb.Txn(true)
//	it, err := newTxn.ReverseLowerBound("evictedJobs", "index", math.MaxInt)
//	if err != nil {
//		require.NoError(t, err)
//	}
//	count := 0
//	for obj := it.Next(); obj != nil; obj = it.Next() {
//		count++
//	}
//	if count != numberOfJobs {
//		panic("not expected number of jobs in iteration")
//	}
//	newTxn.Commit()
//	iterationTimeTaken := time.Now().Sub(iterationStart)
//
//	slices.Shuffle(jobContexts)
//
//	deletionStart := time.Now()
//	for _, jctx := range jobContexts {
//		newerTxn := nodeDb.Txn(true)
//		err := deleteEvictedJobSchedulingContextIfExistsWithTxn(newerTxn, jctx.JobId)
//		if err != nil {
//			panic("failed to deleted evicted jctx")
//		}
//		newerTxn.Commit()
//	}
//
//	deletionTimeTaken := time.Now().Sub(deletionStart)
//
//	totalTimeTaken := insertTimeTaken + iterationTimeTaken + deletionTimeTaken
//	fmt.Println(fmt.Sprintf("Inserting: %s, Iterating:  %s, Deletion: %s, Total: %s", insertTimeTaken, iterationTimeTaken, deletionTimeTaken, totalTimeTaken))
//}
//
//// cheap v2 - 108.462266ms - 143.305703ms - 120.385581ms
//// cheap - 125.531634ms - 111.21427ms - 135.58215ms
//// baseline - 363.5733ms - 259.607641ms - 350.097307ms
//
//func TestFindingNodes_Complex(t *testing.T) {
//	taints := []v1.Taint{
//		{
//			Key:    "test1",
//			Value:  "true",
//			Effect: v1.TaintEffectNoSchedule,
//		},
//		{
//			Key:    "test2",
//			Value:  "true",
//			Effect: v1.TaintEffectNoSchedule,
//		},
//		{
//			Key:    "test3",
//			Value:  "true",
//			Effect: v1.TaintEffectNoSchedule,
//		},
//	}
//	start := time.Now()
//	numberOfNodes := 5
//	nodes := make([]*schedulerobjects.Node, 0, numberOfNodes)
//	nodesWithTaints := 0
//	for i := 0; i < numberOfNodes; i++ {
//		node := testfixtures.Test32CpuNode(testfixtures.TestPriorities)
//		node.Taints = []v1.Taint{}
//		numberOfTaints := 0
//		for i := 0; i < numberOfTaints; i++ {
//			node.Taints = append(node.Taints, taints[i])
//		}
//		if numberOfTaints > 0 {
//			nodesWithTaints++
//		}
//		nodes = append(nodes, node)
//	}
//	fmt.Println(fmt.Sprintf("number of nodes with taints %d", nodesWithTaints))
//	nodeDb, err := newNodeDbWithNodes(nodes)
//	require.NoError(t, err)
//	fmt.Println(fmt.Sprintf("Created nodes %s", time.Now().Sub(start)))
//
//	internalNodes := make([]*internaltypes.Node, 0, len(nodes))
//
//	for _, node := range nodes {
//		internalNode, err := nodeDb.GetNode(node.Id)
//		require.NoError(t, err)
//		internalNodes = append(internalNodes, internalNode)
//	}
//
//	jobContexts := make([]*schedulercontext.JobSchedulingContext, 0, len(nodes)*30)
//
//	for i := 0; i < 30; i++ {
//		for i, node := range internalNodes {
//			job := testfixtures.Test1Cpu4GiJob("A", testfixtures.PriorityClass0)
//			node, err = nodeDb.bindJobToNode(node, job, job.PodRequirements().Priority)
//			require.NoError(t, err)
//
//			_, node, err = nodeDb.EvictJobsFromNode(map[string]types.PriorityClass{}, func(job *jobdb.Job) bool {
//				return false
//			}, []*jobdb.Job{job}, node)
//			require.NoError(t, err)
//
//			jctx := schedulercontext.JobSchedulingContextFromJob(job)
//			jctx.IsEvicted = true
//			jctx.AddNodeSelector(schedulerconfig.NodeIdLabel, node.GetId())
//
//			jobContexts = append(jobContexts, jctx)
//			internalNodes[i] = node
//		}
//	}
//	fmt.Println(fmt.Sprintf("Added jobs to nodes %s", time.Now().Sub(start)))
//
//	err = nodeDb.UpsertMany(internalNodes)
//
//	fmt.Println(fmt.Sprintf("Upserted %s", time.Now().Sub(start)))
//
//	iterationStart := time.Now()
//	counter := 0
//	for range jobContexts {
//		counter++
//	}
//	fmt.Println(fmt.Sprintf("Iterated in %s", time.Now().Sub(iterationStart)))
//
//	txn := nodeDb.Txn(true)
//	for i, jctx := range jobContexts {
//		err = nodeDb.AddEvictedJobSchedulingContextWithTxn(txn, i, jctx)
//		require.NoError(t, err)
//	}
//
//	it, err := txn.ReverseLowerBound("evictedJobs", "index", math.MaxInt)
//	if err != nil {
//		require.NoError(t, err)
//	}
//	count := 0
//	for obj := it.Next(); obj != nil; obj = it.Next() {
//		count++
//	}
//	fmt.Println(count)
//	//
//	//fmt.Println(fmt.Sprintf("evicted jobs %s", time.Now().Sub(start)))
//	//
//	//job2 := testfixtures.Test16Cpu128GiJob("B", testfixtures.PriorityClass0)
//	//jctx2 := schedulercontext.JobSchedulingContextFromJob(job2)
//	//pctx := &schedulercontext.PodSchedulingContext{
//	//	Created:                  time.Now(),
//	//	ScheduledAtPriority:      jctx2.PodRequirements.Priority,
//	//	PreemptedAtPriority:      MinPriority,
//	//	NumNodes:                 nodeDb.numNodes,
//	//	NumExcludedNodesByReason: make(map[string]int),
//	//}
//	//jctx2.PodSchedulingContext = pctx
//
//	//schedulingStart := time.Now()
//	//runtime.SetCPUProfileRate(10000)
//	//
//	//f, err := os.Create("test-2")
//	//require.NoError(t, err)
//	//err = pprof.StartCPUProfile(f)
//
//	// for i := 0; i < 100; i++ {
//	//result, err := nodeDb.selectNodeForJobWithFairPreemption(txn, jctx2)
//	//require.NoError(t, err)
//	//assert.NotNil(t, result)
//	//}
//	//
//	//pprof.StopCPUProfile()
//	//fmt.Println(fmt.Sprintf("Time taken %s", time.Now().Sub(schedulingStart)))
//
//	//
//	//job3 := testfixtures.Test16Cpu128GiJob("B", testfixtures.PriorityClass0)
//	//jctx3 := schedulercontext.JobSchedulingContextFromJob(job3)
//	//pctx2 := &schedulercontext.PodSchedulingContext{
//	//	Created:                  time.Now(),
//	//	ScheduledAtPriority:      jctx3.PodRequirements.Priority,
//	//	PreemptedAtPriority:      MinPriority,
//	//	NumNodes:                 nodeDb.numNodes,
//	//	NumExcludedNodesByReason: make(map[string]int),
//	//}
//	//jctx3.PodSchedulingContext = pctx2
//	//
//	//schedulingStart = time.Now()
//	//result, err = nodeDb.selectNodeForJobWithFairPreemption_new(txn, jctx3)
//	//fmt.Println(fmt.Sprintf("Time taken %s", time.Now().Sub(schedulingStart)))
//	//require.NoError(t, err)
//	//assert.NotNil(t, result)
//}
