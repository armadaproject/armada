package scheduler

import (
	"testing"

	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
	"github.com/stretchr/testify/assert"
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

	actual := aggregateJobs(testJobs)

	expected := map[collectionKey]int{
		{Queue: "queue_a", PriorityClass: testfixtures.PriorityClass0}: 4,
		{Queue: "queue_a", PriorityClass: testfixtures.PriorityClass1}: 1,
		{Queue: "queue_b", PriorityClass: testfixtures.PriorityClass0}: 1,
		{Queue: "queue_b", PriorityClass: testfixtures.PriorityClass1}: 1,
	}

	assert.Equal(t, expected, actual)
}
