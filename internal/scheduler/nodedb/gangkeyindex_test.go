package nodedb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
)

func evictedCtxForJob(job *jobdb.Job) *EvictedJobSchedulingContext {
	jctx := context.JobSchedulingContextFromJob(job)
	return &EvictedJobSchedulingContext{JobId: job.Id(), JobSchedulingContext: jctx}
}

func TestGangKeyIndex_FromObject_GangJob(t *testing.T) {
	sut := createGangKeyIndex()
	job := testfixtures.WithGangJobDetails(
		[]*jobdb.Job{testfixtures.Test1Cpu4GiJob("queue-a", testfixtures.PriorityClass0)},
		"gang-1", 2, "",
	)[0]

	ok, val, err := sut.FromObject(evictedCtxForJob(job))
	assert.True(t, ok)
	assert.Nil(t, err)
	assert.Equal(t, []byte("queue-a\x00gang-1\x00"), val)
}

func TestGangKeyIndex_FromObject_NonGangJobNotIndexed(t *testing.T) {
	sut := createGangKeyIndex()
	job := testfixtures.Test1Cpu4GiJob("queue-a", testfixtures.PriorityClass0)

	ok, val, err := sut.FromObject(evictedCtxForJob(job))
	assert.False(t, ok)
	assert.Nil(t, val)
	assert.Nil(t, err)
}

func TestGangKeyIndex_FromObject_WrongType(t *testing.T) {
	sut := createGangKeyIndex()
	ok, val, err := sut.FromObject("not an evicted context")
	assert.False(t, ok)
	assert.Nil(t, val)
	assert.NotNil(t, err)
}

func TestGangKeyIndex_FromArgs(t *testing.T) {
	sut := createGangKeyIndex()

	val, err := sut.FromArgs("queue-a", "gang-1")
	assert.Nil(t, err)
	assert.Equal(t, []byte("queue-a\x00gang-1\x00"), val)

	// FromObject and FromArgs must agree so lookups match inserts.
	job := testfixtures.WithGangJobDetails(
		[]*jobdb.Job{testfixtures.Test1Cpu4GiJob("queue-a", testfixtures.PriorityClass0)},
		"gang-1", 2, "",
	)[0]
	_, objVal, _ := sut.FromObject(evictedCtxForJob(job))
	assert.Equal(t, objVal, val)
}

func TestGangKeyIndex_FromArgs_WrongArgCount(t *testing.T) {
	sut := createGangKeyIndex()
	_, err := sut.FromArgs("only-one")
	assert.NotNil(t, err)
}

func TestGangKeyIndex_FromArgs_WrongArgType(t *testing.T) {
	sut := createGangKeyIndex()
	_, err := sut.FromArgs("queue-a", 123)
	assert.NotNil(t, err)
}

func TestGangKeyIndex_NoCollisionBetweenPairs(t *testing.T) {
	sut := createGangKeyIndex()
	a, err := sut.FromArgs("a", "bc")
	assert.Nil(t, err)
	b, err := sut.FromArgs("ab", "c")
	assert.Nil(t, err)
	assert.NotEqual(t, a, b)
}

func TestGangKeyIndex_ReturnsSiblingsAndSkipsNonGang(t *testing.T) {
	nodeDb, err := newNodeDbWithNodes(nil)
	require.NoError(t, err)

	node := testfixtures.Test32CpuNode(testfixtures.TestPriorities)
	gangJobs := testfixtures.WithGangJobDetails(
		testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 2), "gang-1", 2, "",
	)
	nonGangJobs := testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1)
	allJobs := append(append([]*jobdb.Job{}, gangJobs...), nonGangJobs...)

	txn := nodeDb.Txn(true)
	require.NoError(t, nodeDb.CreateAndInsertWithJobDbJobsWithTxn(txn, allJobs, node))
	txn.Commit()

	evictJobs(t, nodeDb, allJobs, node.GetId(), 0)

	txn = nodeDb.Txn(false)
	defer txn.Abort()

	// Gang key returns both gang members.
	it, err := txn.Get(EvictedJobsTable, GangKeyIndex, "A", "gang-1")
	require.NoError(t, err)
	found := map[string]bool{}
	for obj := it.Next(); obj != nil; obj = it.Next() {
		found[obj.(*EvictedJobSchedulingContext).JobId] = true
	}
	assert.Equal(t, map[string]bool{gangJobs[0].Id(): true, gangJobs[1].Id(): true}, found)

	// A non-gang job is not indexed under any gang key: querying its (queue, "")
	// returns nothing.
	it, err = txn.Get(EvictedJobsTable, GangKeyIndex, "A", "")
	require.NoError(t, err)
	assert.Nil(t, it.Next())
}
