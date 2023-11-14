package scheduler

import (
	"github.com/armadaproject/armada/internal/common/util"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
)

func TestAggregateJobs(t *testing.T) {
	testJobs := []*jobdb.Job{
		testfixtures.Test1Cpu4GiJob("queue_a", testfixtures.PriorityClass0),
		testfixtures.Test1Cpu4GiJob("queue_b", testfixtures.PriorityClass0),
		testfixtures.Test1Cpu4GiJob("queue_a", testfixtures.PriorityClass0),
		testfixtures.Test1Cpu4GiJob("queue_a", testfixtures.PriorityClass1),
		testfixtures.Test1Cpu4GiJob("queue_a", testfixtures.PriorityClass0),
		testfixtures.Test1Cpu4GiJob("queue_b", testfixtures.PriorityClass1),
		testfixtures.Test1Cpu4GiJob("queue_a", testfixtures.PriorityClass0),
	}

	actual := aggregateJobsByAggregator(testJobs, util.Map(testJobs, func(j *jobdb.Job) string { return j.GetQueue() }))

	expected := map[aggregatorCollectionKey]int{
		{aggregator: "queue_a", priorityClass: testfixtures.PriorityClass0}: 4,
		{aggregator: "queue_a", priorityClass: testfixtures.PriorityClass1}: 1,
		{aggregator: "queue_b", priorityClass: testfixtures.PriorityClass0}: 1,
		{aggregator: "queue_b", priorityClass: testfixtures.PriorityClass1}: 1,
	}

	assert.Equal(t, expected, actual)
}
