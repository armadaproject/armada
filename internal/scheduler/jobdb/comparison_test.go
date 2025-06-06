package jobdb

import (
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/util"
	util2 "github.com/armadaproject/armada/internal/executor/util"
	"github.com/benbjohnson/immutable"
	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/slices"

	"github.com/armadaproject/armada/internal/common/stringinterner"
	"github.com/armadaproject/armada/internal/common/types"
)

func TestJobPriorityComparer(t *testing.T) {
	tests := map[string]struct {
		a        *Job
		b        *Job
		expected int
	}{
		"Jobs with equal id are considered equal": {
			a:        &Job{id: "a"},
			b:        &Job{id: "a"},
			expected: 0,
		},
		"Jobs with equal id are considered equal even if priority differs": {
			a:        &Job{id: "a", priority: 1},
			b:        &Job{id: "a", priority: 2},
			expected: 0,
		},
		"Queued jobs are ordered first by increasing priority class priority": {
			a:        &Job{id: "a", priority: 1, priorityClass: types.PriorityClass{Priority: 1}},
			b:        &Job{id: "b", priority: 2, priorityClass: types.PriorityClass{Priority: 2}},
			expected: 1,
		},
		"Queued jobs are ordered second by decreasing priority": {
			a:        &Job{id: "a", priority: 2, priorityClass: types.PriorityClass{Priority: 1}},
			b:        &Job{id: "b", priority: 1, priorityClass: types.PriorityClass{Priority: 1}},
			expected: 1,
		},
		"Queued jobs are ordered third by decreasing submit time": {
			a:        &Job{id: "a", priority: 1, priorityClass: types.PriorityClass{Priority: 1}, submittedTime: 2},
			b:        &Job{id: "b", priority: 1, priorityClass: types.PriorityClass{Priority: 1}, submittedTime: 1},
			expected: 1,
		},
		"Queued jobs are not ordered by runtime": {
			a:        &Job{id: "a", priority: 1, priorityClass: types.PriorityClass{Priority: 1}, activeRunTimestamp: 1, submittedTime: 2},
			b:        &Job{id: "b", priority: 1, priorityClass: types.PriorityClass{Priority: 1}, activeRunTimestamp: 2, submittedTime: 1},
			expected: 1,
		},
		"Queued jobs are ordered fourth by increasing id": {
			a:        &Job{id: "a", priority: 1, priorityClass: types.PriorityClass{Priority: 1}, submittedTime: 1},
			b:        &Job{id: "b", priority: 1, priorityClass: types.PriorityClass{Priority: 1}, submittedTime: 1},
			expected: -1,
		},
		"Running jobs come before queued jobs": {
			a:        &Job{id: "a", priority: 1},
			b:        (&Job{id: "b", priority: 2, jobDb: NewJobDb(map[string]types.PriorityClass{"foo": {}}, "foo", stringinterner.New(1), testResourceListFactory)}).WithNewRun("", "", "", "", 0),
			expected: 1,
		},
		"Running jobs are ordered third by runtime": {
			a: (&Job{id: "a", priority: 1, priorityClass: types.PriorityClass{Priority: 1}, submittedTime: 1}).WithUpdatedRun(
				&JobRun{created: 1},
			),
			b: (&Job{id: "b", priority: 1, priorityClass: types.PriorityClass{Priority: 1}, submittedTime: 2}).WithUpdatedRun(
				&JobRun{created: 0},
			),
			expected: 1,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, JobPriorityComparer{}.Compare(tc.a, tc.b))
		})
	}
}

func TestJobPriorityComparer_Scale(t *testing.T) {
	numberOfJobs := 1000000
	jobs := make([]*Job, 0, numberOfJobs)

	for i := 0; i < numberOfJobs; i++ {
		jobs = append(jobs, createRandomJob())
	}

	start := time.Now()
	//
	set := immutable.NewSortedSet[*Job](JobPriorityComparer{}, jobs...)
	//
	//for _, job := range jobs {
	//	set.Set(job)
	//}
	fmt.Println(set.Len())

	//result := make(map[uint32][]*Job, 1000)
	//for _, job := range jobs {
	//	_, present := result[job.priority]
	//	if !present {
	//		result[job.priority] = []*Job{}
	//	}
	//
	//	result[job.priority] = append(result[job.priority], job)
	//}
	//slices.SortFunc(jobs, func(a, b *Job) int {
	//	return JobPriorityComparer{}.Compare(a, b)
	//})
	end := time.Now()
	fmt.Println(fmt.Sprintf("total time taken %s", end.Sub(start)))

	//jobs = make([]*Job, 0, 500000)
	//
	//for i := 0; i < 500000; i++ {
	//	jobs = append(jobs, createRandomJob())
	//}
	//
	//start = time.Now()
	//
	//for _, job := range jobs {
	//	set = set.Add(job)
	//}
	//
	//end = time.Now()
	//fmt.Println(fmt.Sprintf("total time taken %s", end.Sub(start)))
	//for i := 0; i < 10; i++ {
	//	jobs = append(jobs, createRandomJob())
	//}
	//
	//start2 := time.Now()
	//slices.SortFunc(jobs, func(a, b *Job) int {
	//	return JobPriorityComparer{}.Compare(a, b)
	//})
	//end2 := time.Now()
	//fmt.Println(fmt.Sprintf("total time taken %s", end2.Sub(start2)))
}

func TestJobPriorityComparer_Scale_2(t *testing.T) {
	queues := 1
	numberOfJobs := 1000000
	jobs := make([][]*Job, queues)

	for i, _ := range jobs {
		jobNumbers := numberOfJobs
		//if i < 3 {
		//	jobNumbers = 3000000
		//}
		j1 := make([]*Job, 0, jobNumbers)
		for i := 0; i < jobNumbers; i++ {
			j1 = append(j1, createRandomJob())
		}
		jobs[i] = j1
	}

	start := time.Now()

	//for _, j := range jobs {
	//	slices.SortFunc(j, func(a, b *Job) int {
	//		return JobPriorityComparer{}.Compare(a, b)
	//	})
	//}
	util2.ProcessItemsWithThreadPool(armadacontext.Background(), 5, jobs, func(jobs []*Job) {
		slices.SortFunc(jobs, func(a, b *Job) int {
			return JobPriorityComparer{}.Compare(a, b)
		})
	})

	//wg := sync.WaitGroup{}
	//wg.Add(queues)
	//for _, j := range jobs {
	//	go func() {
	//		defer wg.Done()
	//		slices.SortFunc(j, func(a, b *Job) int {
	//			return JobPriorityComparer{}.Compare(a, b)
	//		})
	//	}()
	//}
	//wg.Wait()
	end := time.Now()
	fmt.Println(fmt.Sprintf("total time taken %s", end.Sub(start)))
}

func createRandomJob() *Job {
	id := util.NewULID()
	priority := rand.Uint32N(100)
	priorityClass := rand.Int32N(1) + 1
	submittedTime := rand.Int64N(1000)

	return &Job{id: id, priority: priority, priorityClass: types.PriorityClass{Priority: priorityClass}, submittedTime: submittedTime}
}
