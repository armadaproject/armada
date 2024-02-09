package scheduler

import (
	"testing"

	"github.com/stretchr/testify/assert"

	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
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

	actual := aggregateJobContexts(map[queuePriorityClassKey]int{}, schedulercontext.JobSchedulingContextsFromJobs(testfixtures.TestPriorityClasses, testJobs))

	expected := map[queuePriorityClassKey]int{
		{queue: "queue_a", priorityClass: testfixtures.PriorityClass0}: 4,
		{queue: "queue_a", priorityClass: testfixtures.PriorityClass1}: 1,
		{queue: "queue_b", priorityClass: testfixtures.PriorityClass0}: 1,
		{queue: "queue_b", priorityClass: testfixtures.PriorityClass1}: 1,
	}

	assert.Equal(t, expected, actual)
}
