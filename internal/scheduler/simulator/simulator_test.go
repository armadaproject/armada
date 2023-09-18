package simulator

import (
	fmt "fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/armada/configuration"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

func TestSimulator(t *testing.T) {
	tests := map[string]struct {
		testCase               *TestCase
		schedulingConfig       configuration.SchedulingConfig
		expectedEventSequences []*armadaevents.EventSequence
	}{
		"Two jobs in parallel": {
			testCase: &TestCase{
				Name:  "basic",
				Pools: []*Pool{Pool32Cpu("Pool", 1, 1, 2)},
				Queues: []Queue{
					WithJobTemplatesQueue(
						Queue{Name: "A", Weight: 1},
						JobTemplate32Cpu(2, "foo", testfixtures.TestDefaultPriorityClass),
					),
				},
			},
			schedulingConfig: testfixtures.TestSchedulingConfig(),
			expectedEventSequences: []*armadaevents.EventSequence{
				{Queue: "A", JobSetName: "foo", Events: testfixtures.Repeat(SubmitJob(), 2)},
				{Queue: "A", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobRunLeased()}},
				{Queue: "A", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobRunLeased()}},
				{Queue: "A", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobSucceeded()}},
				{Queue: "A", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobSucceeded()}},
			},
		},
		"Two jobs in sequence": {
			testCase: &TestCase{
				Name:  "basic",
				Pools: []*Pool{Pool32Cpu("Pool", 1, 1, 1)},
				Queues: []Queue{
					WithJobTemplatesQueue(
						Queue{Name: "A", Weight: 1},
						JobTemplate32Cpu(2, "foo", testfixtures.TestDefaultPriorityClass),
					),
				},
			},
			schedulingConfig: testfixtures.TestSchedulingConfig(),
			expectedEventSequences: []*armadaevents.EventSequence{
				{Queue: "A", JobSetName: "foo", Events: testfixtures.Repeat(SubmitJob(), 2)},
				{Queue: "A", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobRunLeased()}},
				{Queue: "A", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobSucceeded()}},
				{Queue: "A", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobRunLeased()}},
				{Queue: "A", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobSucceeded()}},
			},
		},
		"10 jobs in sequence": {
			testCase: &TestCase{
				Name:  "basic",
				Pools: []*Pool{Pool32Cpu("Pool", 1, 1, 1)},
				Queues: []Queue{
					WithJobTemplatesQueue(
						Queue{Name: "A", Weight: 1},
						JobTemplate32Cpu(10, "foo", testfixtures.TestDefaultPriorityClass),
					),
				},
			},
			schedulingConfig: testfixtures.TestSchedulingConfig(),
			expectedEventSequences: append(
				[]*armadaevents.EventSequence{
					{Queue: "A", JobSetName: "foo", Events: testfixtures.Repeat(SubmitJob(), 10)},
				},
				armadaslices.Repeat(
					10,
					&armadaevents.EventSequence{Queue: "A", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobRunLeased()}},
					&armadaevents.EventSequence{Queue: "A", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobSucceeded()}},
				)...,
			),
		},
		"JobTemplate dependencies": {
			testCase: &TestCase{
				Name:  "basic",
				Pools: []*Pool{Pool32Cpu("Pool", 1, 1, 3)},
				Queues: []Queue{
					WithJobTemplatesQueue(
						Queue{Name: "A", Weight: 1},
						WithIdJobTemplate(
							JobTemplate32Cpu(2, "foo", testfixtures.TestDefaultPriorityClass),
							"jobTemplate",
						),
						WithDependenciesJobTemplate(
							JobTemplate32Cpu(1, "foo", testfixtures.TestDefaultPriorityClass),
							"jobTemplate",
						),
					),
				},
			},
			schedulingConfig: testfixtures.TestSchedulingConfig(),
			expectedEventSequences: []*armadaevents.EventSequence{
				{Queue: "A", JobSetName: "foo", Events: testfixtures.Repeat(SubmitJob(), 2)},
				{Queue: "A", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobRunLeased()}},
				{Queue: "A", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobRunLeased()}},
				{Queue: "A", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobSucceeded()}},
				{Queue: "A", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobSucceeded()}},
				{Queue: "A", JobSetName: "foo", Events: testfixtures.Repeat(SubmitJob(), 1)},
				{Queue: "A", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobRunLeased()}},
				{Queue: "A", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobSucceeded()}},
			},
		},
		"Preemption": {
			testCase: &TestCase{
				Name:  "basic",
				Pools: []*Pool{Pool32Cpu("Pool", 1, 1, 2)},
				Queues: []Queue{
					WithJobTemplatesQueue(
						Queue{Name: "A", Weight: 1},
						JobTemplate32Cpu(2, "foo", testfixtures.PriorityClass0),
					),
					WithJobTemplatesQueue(
						Queue{Name: "B", Weight: 1},
						WithMinSubmitTimeJobTemplate(
							JobTemplate32Cpu(1, "bar", testfixtures.PriorityClass0),
							time.Time{}.Add(30*time.Second),
						),
					),
				},
			},
			schedulingConfig: testfixtures.TestSchedulingConfig(),
			expectedEventSequences: []*armadaevents.EventSequence{
				{Queue: "A", JobSetName: "foo", Events: armadaslices.Repeat(2, SubmitJob())},
				{Queue: "A", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobRunLeased()}},
				{Queue: "A", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobRunLeased()}},
				{Queue: "B", JobSetName: "bar", Events: []*armadaevents.EventSequence_Event{SubmitJob()}},
				{Queue: "A", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobRunPreempted()}},
				{Queue: "B", JobSetName: "bar", Events: []*armadaevents.EventSequence_Event{JobRunLeased()}},
				{Queue: "A", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{SubmitJob()}},
				{Queue: "A", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobSucceeded()}},
				{Queue: "A", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobRunLeased()}},
				{Queue: "B", JobSetName: "bar", Events: []*armadaevents.EventSequence_Event{JobSucceeded()}},
				{Queue: "A", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobSucceeded()}},
			},
		},
		"Preemption cascade": {
			testCase: &TestCase{
				Name: "test",
				Pools: []*Pool{
					WithExecutorGroupsPool(
						&Pool{Name: "Pool"},
						ExecutorGroup32Cpu(1, 1),
						ExecutorGroup32Cpu(1, 1),
						ExecutorGroup32Cpu(1, 1),
					),
				},
				Queues: []Queue{
					WithJobTemplatesQueue(
						Queue{Name: "B", Weight: 1},
						JobTemplate32Cpu(1, "foo", testfixtures.PriorityClass0),
					),
					WithJobTemplatesQueue(
						Queue{Name: "C", Weight: 1},
						JobTemplate32Cpu(2, "foo", testfixtures.PriorityClass0),
					),
					WithJobTemplatesQueue(
						Queue{Name: "A", Weight: 1},
						WithMinSubmitTimeJobTemplate(
							JobTemplate32Cpu(1, "foo", testfixtures.PriorityClass0),
							time.Time{}.Add(30*time.Second),
						),
					),
				},
			},
			schedulingConfig: testfixtures.TestSchedulingConfig(),
			expectedEventSequences: []*armadaevents.EventSequence{
				{Queue: "B", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{SubmitJob()}},
				{Queue: "C", JobSetName: "foo", Events: armadaslices.Repeat(2, SubmitJob())},
				{Queue: "B", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobRunLeased()}},
				{Queue: "C", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobRunLeased()}},
				{Queue: "C", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobRunLeased()}},
				{Queue: "A", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{SubmitJob()}},
				{Queue: "B", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobRunPreempted()}},
				{Queue: "A", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobRunLeased()}},
				{Queue: "B", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{SubmitJob()}},
				{Queue: "C", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobRunPreempted()}},
				{Queue: "B", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobRunLeased()}},
				{Queue: "C", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{SubmitJob()}},
				{Queue: "C", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobSucceeded()}},
				{Queue: "C", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobRunLeased()}},
				{Queue: "A", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobSucceeded()}},
				{Queue: "B", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobSucceeded()}},
				{Queue: "C", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobSucceeded()}},
			},
		},
		"No preemption cascade with unified scheduling": {
			testCase: &TestCase{
				Name: "test",
				Pools: []*Pool{
					WithExecutorGroupsPool(
						&Pool{Name: "Pool"},
						ExecutorGroup32Cpu(3, 1),
					),
				},
				Queues: []Queue{
					WithJobTemplatesQueue(
						Queue{Name: "B", Weight: 1},
						JobTemplate32Cpu(1, "foo", testfixtures.PriorityClass0),
					),
					WithJobTemplatesQueue(
						Queue{Name: "C", Weight: 1},
						JobTemplate32Cpu(2, "foo", testfixtures.PriorityClass0),
					),
					WithJobTemplatesQueue(
						Queue{Name: "A", Weight: 1},
						WithMinSubmitTimeJobTemplate(
							JobTemplate32Cpu(1, "foo", testfixtures.PriorityClass0),
							time.Time{}.Add(30*time.Second),
						),
					),
				},
			},
			schedulingConfig: testfixtures.TestSchedulingConfig(),
			expectedEventSequences: []*armadaevents.EventSequence{
				{Queue: "B", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{SubmitJob()}},
				{Queue: "C", JobSetName: "foo", Events: armadaslices.Repeat(2, SubmitJob())},
				{Queue: "B", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobRunLeased()}},
				{Queue: "C", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobRunLeased()}},
				{Queue: "C", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobRunLeased()}},
				{Queue: "A", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{SubmitJob()}},
				{Queue: "C", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobRunPreempted()}},
				{Queue: "A", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobRunLeased()}},
				{Queue: "C", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{SubmitJob()}},
				{Queue: "B", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobSucceeded()}},
				{Queue: "C", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobSucceeded()}},
				{Queue: "C", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobRunLeased()}},
				{Queue: "A", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobSucceeded()}},
				{Queue: "C", JobSetName: "foo", Events: []*armadaevents.EventSequence_Event{JobSucceeded()}},
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			s, err := NewSimulator(tc.testCase, tc.schedulingConfig)
			require.NoError(t, err)
			go func() { err = s.Run() }()
			actualEventSequences := make([]*armadaevents.EventSequence, 0, len(tc.expectedEventSequences))
			for eventSequence := range s.C() {
				t.Log(*eventSequence.Events[0].Created, eventSequenceSummary(eventSequence))
				actualEventSequences = append(actualEventSequences, eventSequence)
			}
			require.NoError(t, err)
			t.Logf("Simulated time: %s", s.time.Sub(time.Time{}))
			if tc.expectedEventSequences != nil {
				require.Equal(
					t,
					util.Map(tc.expectedEventSequences, func(eventSequence *armadaevents.EventSequence) string { return eventSequenceSummary(eventSequence) }),
					util.Map(actualEventSequences, func(eventSequence *armadaevents.EventSequence) string { return eventSequenceSummary(eventSequence) }),
					"Expected:\n%s\nReceived:\n%s",
					eventSequencesSummary(tc.expectedEventSequences),
					eventSequencesSummary(actualEventSequences),
				)
			}
		})
	}
}

func WithExecutorGroupsPool(pool *Pool, executorGroups ...*ExecutorGroup) *Pool {
	pool.ExecutorGroups = append(pool.ExecutorGroups, executorGroups...)
	return pool
}

func WithExecutorsExecutorGroup(executorGroup *ExecutorGroup, executors ...*Executor) *ExecutorGroup {
	executorGroup.Executors = append(executorGroup.Executors, executors...)
	return executorGroup
}

func WithNodeTemplatesExecutor(executor *Executor, nodeTemplates ...*NodeTemplate) *Executor {
	executor.NodeTemplates = append(executor.NodeTemplates, nodeTemplates...)
	return executor
}

func Pool32Cpu(name string, numExecutorGroups, numExecutorsPerGroup, numNodesPerExecutor int64) *Pool {
	executorGroups := make([]*ExecutorGroup, numExecutorGroups)
	for i := 0; i < int(numExecutorGroups); i++ {
		executorGroups[i] = ExecutorGroup32Cpu(numExecutorsPerGroup, numNodesPerExecutor)
	}
	return &Pool{
		Name:           name,
		ExecutorGroups: executorGroups,
	}
}

func ExecutorGroup32Cpu(numExecutors, numNodesPerExecutor int64) *ExecutorGroup {
	executors := make([]*Executor, numExecutors)
	for i := 0; i < int(numExecutors); i++ {
		executors[i] = Executor32Cpu(numNodesPerExecutor)
	}
	return &ExecutorGroup{
		Executors: executors,
	}
}

func Executor32Cpu(numNodes int64) *Executor {
	return &Executor{
		NodeTemplates: []*NodeTemplate{
			NodeTemplate32Cpu(numNodes),
		},
	}
}

func NodeTemplate32Cpu(n int64) *NodeTemplate {
	return &NodeTemplate{
		Number: n,
		TotalResources: schedulerobjects.ResourceList{
			Resources: map[string]resource.Quantity{
				"cpu":    resource.MustParse("32"),
				"memory": resource.MustParse("256Gi"),
			},
		},
	}
}

func WithJobTemplatesQueue(queue Queue, jobTemplate ...*JobTemplate) Queue {
	queue.JobTemplates = append(queue.JobTemplates, jobTemplate...)
	return queue
}

func WithIdJobTemplate(jobTemplate *JobTemplate, id string) *JobTemplate {
	jobTemplate.Id = id
	return jobTemplate
}

func WithDependenciesJobTemplate(jobTemplate *JobTemplate, dependencyIds ...string) *JobTemplate {
	jobTemplate.Dependencies = append(jobTemplate.Dependencies, dependencyIds...)
	return jobTemplate
}

func WithMinSubmitTimeJobTemplate(jobTemplate *JobTemplate, minSubmitTime time.Time) *JobTemplate {
	jobTemplate.MinSubmitTime = minSubmitTime
	return jobTemplate
}

func JobTemplate32Cpu(n int64, jobSet, priorityClassName string) *JobTemplate {
	return &JobTemplate{
		Number:            n,
		JobSet:            jobSet,
		PriorityClassName: priorityClassName,
		Requirements: schedulerobjects.PodRequirements{
			ResourceRequirements: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					"cpu":    resource.MustParse("32"),
					"memory": resource.MustParse("256Gi"),
				},
			},
		},
		RuntimeMean: 60,
	}
}

func JobTemplate1Cpu(n int64, jobSet, priorityClassName string) *JobTemplate {
	return &JobTemplate{
		Number:            n,
		JobSet:            jobSet,
		PriorityClassName: priorityClassName,
		Requirements: schedulerobjects.PodRequirements{
			ResourceRequirements: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					"cpu":    resource.MustParse("1"),
					"memory": resource.MustParse("8Gi"),
				},
			},
		},
		RuntimeMean: 60,
	}
}

func SubmitJob() *armadaevents.EventSequence_Event {
	return &armadaevents.EventSequence_Event{
		Event: &armadaevents.EventSequence_Event_SubmitJob{
			SubmitJob: &armadaevents.SubmitJob{},
		},
	}
}

func JobRunLeased() *armadaevents.EventSequence_Event {
	return &armadaevents.EventSequence_Event{
		Event: &armadaevents.EventSequence_Event_JobRunLeased{
			JobRunLeased: &armadaevents.JobRunLeased{},
		},
	}
}

func JobRunPreempted() *armadaevents.EventSequence_Event {
	return &armadaevents.EventSequence_Event{
		Event: &armadaevents.EventSequence_Event_JobRunPreempted{
			JobRunPreempted: &armadaevents.JobRunPreempted{},
		},
	}
}

func JobSucceeded() *armadaevents.EventSequence_Event {
	return &armadaevents.EventSequence_Event{
		Event: &armadaevents.EventSequence_Event_JobSucceeded{
			JobSucceeded: &armadaevents.JobSucceeded{},
		},
	}
}

func eventSequencesSummary(eventSequences []*armadaevents.EventSequence) string {
	var sb strings.Builder
	for i, eventSequence := range eventSequences {
		sb.WriteString(eventSequenceSummary(eventSequence))
		if i != len(eventSequences)-1 {
			sb.WriteString("\n")
		}
	}
	return sb.String()
}

func eventSequenceSummary(eventSequence *armadaevents.EventSequence) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("EventSequence{Queue: %s, JobSetName: %s, Events: [", eventSequence.Queue, eventSequence.JobSetName))
	for i, event := range eventSequence.Events {
		sb.WriteString(eventSummary(event))
		if i != len(eventSequence.Events)-1 {
			sb.WriteString(", ")
		}
	}
	sb.WriteString("]}")
	return sb.String()
}

func eventSummary(event *armadaevents.EventSequence_Event) string {
	return strings.ReplaceAll(fmt.Sprintf("%T", event.Event), "*armadaevents.EventSequence_Event_", "")
}
