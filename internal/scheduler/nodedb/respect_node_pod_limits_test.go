package nodedb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/common/pointer"
	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	"github.com/armadaproject/armada/internal/common/stringinterner"
	schedulerconfig "github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
)

// TestRespectNodePodLimits_EvictionReleasesPodSlot verifies the preemption/eviction
// round-trip for pod-slot accounting: a job bound to a pods=1 node consumes the slot
// (allocatable pods -> 0), and evicting it returns the slot (allocatable pods -> 1).
// Without this test the release path was only "correct by construction" from sharing
// KubernetesResourceRequirements between bind and unbind.
func TestRespectNodePodLimits_EvictionReleasesPodSlot(t *testing.T) {
	resourceTypes := []schedulerconfig.ResourceType{
		{Name: "cpu", Resolution: resource.MustParse("1m")},
		{Name: "memory", Resolution: resource.MustParse("1")},
		{Name: armadaresource.PodsResourceName, Resolution: resource.MustParse("1")},
	}
	rlFactory, err := internaltypes.NewResourceListFactory(resourceTypes, nil)
	require.NoError(t, err)

	jobDb := jobdb.NewJobDbWithSchedulingKeyGenerator(
		testfixtures.TestPriorityClasses,
		testfixtures.TestDefaultPriorityClass,
		testfixtures.SchedulingKeyGenerator,
		stringinterner.New(1024),
		rlFactory,
	)
	jobDb.SetRespectNodePodLimits(true)

	nodeDb, err := NewNodeDb(
		testfixtures.TestPriorityClasses,
		resourceTypes,
		testfixtures.TestIndexedTaints,
		testfixtures.TestIndexedNodeLabels,
		testfixtures.TestWellKnownNodeTypes,
		rlFactory,
	)
	require.NoError(t, err)

	nodeProto := testfixtures.TestSchedulerObjectsNode(testfixtures.TestPriorities, map[string]*resource.Quantity{
		"cpu":                           pointer.MustParseResource("10"),
		"memory":                        pointer.MustParseResource("64Gi"),
		armadaresource.PodsResourceName: pointer.MustParseResource("1"),
	})
	nodeFactory := internaltypes.NewNodeFactory(
		testfixtures.TestIndexedTaints,
		testfixtures.TestIndexedNodeLabels,
		testfixtures.TestPriorityClasses,
		rlFactory,
	)
	node := nodeFactory.FromSchedulerObjectsNode(nodeProto)

	txn := nodeDb.Txn(true)
	require.NoError(t, nodeDb.CreateAndInsertWithJobDbJobsWithTxn(txn, nil, node))
	txn.Commit()

	job := newPodsJob(t, jobDb, "jobA")
	priority := job.PriorityClass().Priority

	entry, err := nodeDb.GetNode(node.GetId())
	require.NoError(t, err)
	initialPods := entry.AllocatableByPriority[priority].GetByNameZeroIfMissing(armadaresource.PodsResourceName)
	require.Equal(t, int64(1), initialPods.Value(), "node should start with pods=1 available")

	boundNode, err := nodeDb.BindJobToNode(entry, job, priority)
	require.NoError(t, err)
	boundPods := boundNode.AllocatableByPriority[priority].GetByNameZeroIfMissing(armadaresource.PodsResourceName)
	assert.Equal(t, int64(0), boundPods.Value(), "bind should consume the pod slot")

	evictedNode, err := nodeDb.EvictJobsFromNode([]*jobdb.Job{job}, boundNode)
	require.NoError(t, err)
	unboundNode, err := nodeDb.UnbindJobFromNode(job, evictedNode)
	require.NoError(t, err)
	releasedPods := unboundNode.AllocatableByPriority[priority].GetByNameZeroIfMissing(armadaresource.PodsResourceName)
	assert.Equal(t, int64(1), releasedPods.Value(), "evict+unbind should free the pod slot")

	followUp := newPodsJob(t, jobDb, "jobB")
	_, err = nodeDb.BindJobToNode(unboundNode, followUp, followUp.PriorityClass().Priority)
	assert.NoError(t, err, "second job should bind after pod slot is freed")
}

func newPodsJob(t *testing.T, db *jobdb.JobDb, jobId string) *jobdb.Job {
	t.Helper()
	info := &internaltypes.JobSchedulingInfo{
		PriorityClass: testfixtures.PriorityClass0,
		PodRequirements: testfixtures.TestPodReqs(v1.ResourceList{
			"cpu":    resource.MustParse("1"),
			"memory": resource.MustParse("1Gi"),
		}),
	}
	job, err := db.NewJob(jobId, "jobSet", "queue", 1, info, false, 0, false, false, false, 0, false, []string{testfixtures.TestPool}, 0)
	require.NoError(t, err)
	return job
}
