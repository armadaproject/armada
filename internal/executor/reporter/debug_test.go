package reporter

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	k8s_errors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clocktesting "k8s.io/utils/clock/testing"
	"k8s.io/utils/pointer"

	"github.com/armadaproject/armada/internal/executor/configuration"
)

const testMaxEvents = 10

type fakeNodeSource struct {
	nodes  map[string]*v1.Node
	events map[string][]*v1.Event
	err    error
}

func (f *fakeNodeSource) GetNode(nodeName string) (*v1.Node, error) {
	if f.err != nil {
		return nil, f.err
	}
	node, exists := f.nodes[nodeName]
	if !exists {
		return nil, k8s_errors.NewNotFound(schema.GroupResource{Resource: "nodes"}, nodeName)
	}
	return node, nil
}

func (f *fakeNodeSource) GetNodeEvents(nodeName string) ([]*v1.Event, error) {
	return f.events[nodeName], nil
}

var renderTime = time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)

func rendererWithNodes(nodes ...*v1.Node) *DebugMessageRenderer {
	return rendererWithNodeEvents(nil, nodes...)
}

func rendererWithNodeEvents(nodeEvents map[string][]*v1.Event, nodes ...*v1.Node) *DebugMessageRenderer {
	byName := map[string]*v1.Node{}
	for _, node := range nodes {
		byName[node.Name] = node
	}
	return &DebugMessageRenderer{
		nodes: &fakeNodeSource{nodes: byName, events: nodeEvents},
		clock: clocktesting.NewFakeClock(renderTime),
		config: configuration.DebugEventsConfig{
			Enabled: true,
			Pod:     configuration.PodDebugConfig{MaxEvents: testMaxEvents},
			Node: configuration.NodeDebugConfig{
				MaxEvents:   testMaxEvents,
				Labels:      []string{"kubernetes.io/hostname", "armadaproject.io/pool"},
				Annotations: []string{"armadaproject.io/drain-reason"},
			},
		},
	}
}

// render parses the payload back, so the tests assert on fields rather than on formatting.
func render(t *testing.T, renderer *DebugMessageRenderer, pod *v1.Pod, podEvents []*v1.Event) *PodDebugInfo {
	t.Helper()
	payload := renderer.Render(pod, podEvents, TriggerStuckTerminating)
	require.NotEmpty(t, payload)

	info := &PodDebugInfo{}
	require.NoError(t, json.Unmarshal([]byte(payload), info), "payload must be valid JSON")
	return info
}

func event(message string, options ...func(*v1.Event)) *v1.Event {
	e := &v1.Event{
		ObjectMeta:     metav1.ObjectMeta{Name: "event", Namespace: "default"},
		InvolvedObject: v1.ObjectReference{Kind: "Pod", Name: "test-pod", Namespace: "default"},
		Reason:         "FailedScheduling",
		Message:        message,
		Type:           "Warning",
		Source:         v1.EventSource{Component: "default-scheduler"},
	}
	for _, option := range options {
		option(e)
	}
	return e
}

func TestRender_IsValidJsonWithSchemaVersionAndTrigger(t *testing.T) {
	pod := &v1.Pod{Status: v1.PodStatus{Phase: v1.PodFailed}}
	payload := rendererWithNodes().Render(pod, nil, TriggerPodFailed)

	info := &PodDebugInfo{}
	require.NoError(t, json.Unmarshal([]byte(payload), info))
	assert.Equal(t, debugSchemaVersion, info.SchemaVersion)
	assert.Equal(t, TriggerPodFailed, info.Trigger)
}

// The kill switch lives in the renderer, so every call site is disabled at once and each attaches an
// empty debug field - which is how these events looked before pod diagnostics existed.
func TestRender_ReturnsNothingWhenDisabled(t *testing.T) {
	node := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}}
	pod := &v1.Pod{
		Spec:   v1.PodSpec{NodeName: "node-1"},
		Status: v1.PodStatus{Phase: v1.PodFailed},
	}
	renderer := rendererWithNodes(node)
	renderer.config.Enabled = false

	assert.Empty(t, renderer.Render(pod, []*v1.Event{event("something happened")}, TriggerPodFailed))
}

// A pod with no reported state that was never scheduled leaves nothing to diagnose.
func TestRender_ReturnsNothingWhenThereIsNothingToDescribe(t *testing.T) {
	assert.Empty(t, rendererWithNodes().Render(&v1.Pod{}, nil, TriggerPodFailed))
}

// Init containers are reported separately from app containers, and a per-container restartPolicy is
// what distinguishes a native sidecar (Always) from an ordinary init container that runs to
// completion. App containers inherit the pod's policy, so that is reported once on the pod.
func TestRender_DescribesPodAndContainers(t *testing.T) {
	startedAt := renderTime.Add(-time.Hour)
	alwaysRestart := v1.ContainerRestartPolicyAlways
	pod := &v1.Pod{
		Spec: v1.PodSpec{
			RestartPolicy: v1.RestartPolicyNever,
			InitContainers: []v1.Container{
				{Name: "init"},
				{Name: "sidecar", RestartPolicy: &alwaysRestart},
			},
		},
		Status: v1.PodStatus{
			Phase:  v1.PodFailed,
			Reason: "Evicted",
			ContainerStatuses: []v1.ContainerStatus{
				{Name: "main", State: v1.ContainerState{Terminated: &v1.ContainerStateTerminated{
					ExitCode:   137,
					Reason:     "OOMKilled",
					StartedAt:  metav1.NewTime(startedAt),
					FinishedAt: metav1.NewTime(renderTime),
				}}},
			},
			InitContainerStatuses: []v1.ContainerStatus{
				{Name: "init", State: v1.ContainerState{Terminated: &v1.ContainerStateTerminated{
					Reason:     "Completed",
					StartedAt:  metav1.NewTime(startedAt),
					FinishedAt: metav1.NewTime(startedAt.Add(10 * time.Second)),
				}}},
				{Name: "sidecar", State: v1.ContainerState{Running: &v1.ContainerStateRunning{}}},
			},
		},
	}

	info := render(t, rendererWithNodes(), pod, nil)

	assert.Equal(t, "Failed", info.Pod.Phase)
	assert.Equal(t, "Evicted", info.Pod.Reason)
	assert.Equal(t, "Never", info.Pod.RestartPolicy)

	require.Len(t, info.Pod.InitContainers, 2)
	assert.Equal(t, ContainerInfo{
		Name:       "init",
		State:      "terminated",
		ExitCode:   pointer.Int32(0),
		Reason:     "Completed",
		RunSeconds: pointer.Int64(10),
	}, info.Pod.InitContainers[0])
	assert.Equal(t, ContainerInfo{Name: "sidecar", RestartPolicy: "Always", State: "running"}, info.Pod.InitContainers[1])

	require.Len(t, info.Pod.Containers, 1)
	assert.Equal(t, ContainerInfo{
		Name:       "main",
		State:      "terminated",
		ExitCode:   pointer.Int32(137),
		Reason:     "OOMKilled",
		RunSeconds: pointer.Int64(3600),
	}, info.Pod.Containers[0])
}

func TestRender_OmitsTerminationFieldsWhenPodIsNotBeingDeleted(t *testing.T) {
	info := render(t, rendererWithNodes(), &v1.Pod{Status: v1.PodStatus{Phase: v1.PodRunning}}, nil)

	assert.Nil(t, info.Pod.ForceTerminated)
	assert.Empty(t, info.Pod.DeletionTimestamp)
	assert.Empty(t, info.Pod.Finalizers)
}

func TestRender_DescribesTermination(t *testing.T) {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			DeletionTimestamp:          &metav1.Time{Time: renderTime.Add(-90 * time.Second)},
			DeletionGracePeriodSeconds: pointer.Int64(30),
			Finalizers:                 []string{"example.com/cleanup"},
		},
		Spec: v1.PodSpec{TerminationGracePeriodSeconds: pointer.Int64(30)},
	}

	info := render(t, rendererWithNodes(), pod, nil)

	// The deadline the API server computed, not the time the delete was requested.
	assert.Equal(t, "2026-08-11T11:58:30Z", info.Pod.DeletionTimestamp)
	assert.False(t, *info.Pod.ForceTerminated)
	assert.Equal(t, []string{"example.com/cleanup"}, info.Pod.Finalizers)
}

// DeletionGracePeriodSeconds may only be shortened, so zero is the API's record of a force delete.
func TestRender_ReportsForceTerminated(t *testing.T) {
	pod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{
		DeletionTimestamp:          &metav1.Time{Time: renderTime.Add(-time.Minute)},
		DeletionGracePeriodSeconds: pointer.Int64(0),
	}}

	info := render(t, rendererWithNodes(), pod, nil)

	assert.True(t, *info.Pod.ForceTerminated)
}

func TestRender_DescribesNode(t *testing.T) {
	node := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-1"},
		Spec:       v1.NodeSpec{Unschedulable: true},
		Status: v1.NodeStatus{Conditions: []v1.NodeCondition{
			{Type: v1.NodeDiskPressure, Status: v1.ConditionFalse},
			{Type: v1.NodeReady, Status: v1.ConditionUnknown, Reason: "NodeStatusUnknown", Message: "Kubelet stopped posting node status."},
		}},
	}
	pod := &v1.Pod{Spec: v1.PodSpec{NodeName: "node-1"}}

	info := render(t, rendererWithNodes(node), pod, nil)

	assert.Equal(t, "node-1", info.Node.Name)
	assert.True(t, *info.Node.Exists)
	assert.True(t, *info.Node.Unschedulable)
	// Hoisted out of the conditions array so it is a column rather than a scan.
	assert.Equal(t, "Unknown", info.Node.Ready)
	assert.Equal(t, "NodeStatusUnknown", info.Node.ReadyReason)
	require.Len(t, info.Node.Conditions, 2)
	assert.Equal(t, "DiskPressure", info.Node.Conditions[0].Type)
	assert.Equal(t, ConditionInfo{
		Type:    "Ready",
		Status:  "Unknown",
		Reason:  "NodeStatusUnknown",
		Message: "Kubelet stopped posting node status.",
	}, info.Node.Conditions[1])
}

// A production node carries 25+ conditions, most of them node-problem-detector checks sitting False,
// with the kubelet's own pressure conditions last. All of them are reported: which one matters is
// exactly what is not known in advance, and the set is bounded by cluster configuration.
func TestRender_ReportsEveryNodeCondition(t *testing.T) {
	conditions := []v1.NodeCondition{}
	for i := 0; i < 20; i++ {
		conditions = append(conditions, v1.NodeCondition{
			Type:   v1.NodeConditionType(fmt.Sprintf("HealthyNodeProblemDetectorCheck%d", i)),
			Status: v1.ConditionFalse,
			Reason: "AllGood",
		})
	}
	conditions = append(conditions,
		v1.NodeCondition{Type: v1.NodeMemoryPressure, Status: v1.ConditionTrue, Reason: "KubeletHasInsufficientMemory"},
		v1.NodeCondition{Type: v1.NodeDiskPressure, Status: v1.ConditionFalse, Reason: "KubeletHasNoDiskPressure"},
		v1.NodeCondition{Type: v1.NodeReady, Status: v1.ConditionUnknown, Reason: "NodeStatusUnknown"},
	)
	node := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}, Status: v1.NodeStatus{Conditions: conditions}}
	pod := &v1.Pod{Spec: v1.PodSpec{NodeName: "node-1"}}

	info := render(t, rendererWithNodes(node), pod, nil)

	require.Len(t, info.Node.Conditions, len(conditions))
	assert.Equal(t, "MemoryPressure", info.Node.Conditions[20].Type)
	assert.Equal(t, "Ready", info.Node.Conditions[22].Type)
	assert.Equal(t, "Unknown", info.Node.Ready)
}

// Nodes carry a lot of labels and annotations, most of it irrelevant here, so only the configured
// names are captured.
func TestRender_CapturesOnlyConfiguredNodeLabelsAndAnnotations(t *testing.T) {
	node := &v1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: "node-1",
		Labels: map[string]string{
			"kubernetes.io/hostname":  "node-1.example.com",
			"armadaproject.io/pool":   "gpu",
			"some.vendor.io/internal": "not captured",
		},
		Annotations: map[string]string{
			"armadaproject.io/drain-reason": "hardware maintenance",
			"some.vendor.io/checksum":       "not captured",
		},
	}}
	pod := &v1.Pod{Spec: v1.PodSpec{NodeName: "node-1"}}

	info := render(t, rendererWithNodes(node), pod, nil)

	assert.Equal(t, map[string]string{
		"kubernetes.io/hostname": "node-1.example.com",
		"armadaproject.io/pool":  "gpu",
	}, info.Node.Labels)
	assert.Equal(t, map[string]string{"armadaproject.io/drain-reason": "hardware maintenance"}, info.Node.Annotations)
}

func TestRender_OmitsNodeLabelsAndAnnotationsWhenNoneConfigured(t *testing.T) {
	node := &v1.Node{ObjectMeta: metav1.ObjectMeta{
		Name:        "node-1",
		Labels:      map[string]string{"kubernetes.io/hostname": "node-1.example.com"},
		Annotations: map[string]string{"armadaproject.io/drain-reason": "maintenance"},
	}}
	pod := &v1.Pod{Spec: v1.PodSpec{NodeName: "node-1"}}
	renderer := rendererWithNodes(node)
	renderer.config.Node.Labels = nil
	renderer.config.Node.Annotations = nil

	payload := renderer.Render(pod, nil, TriggerStuckTerminating)

	assert.NotContains(t, payload, "kubernetes.io/hostname")
	assert.NotContains(t, payload, `"labels"`)
	assert.NotContains(t, payload, `"annotations"`)
}

// A configured name that the node does not carry must not appear as an empty value.
func TestRender_OmitsConfiguredNodeLabelsThatAreAbsent(t *testing.T) {
	node := &v1.Node{ObjectMeta: metav1.ObjectMeta{
		Name:   "node-1",
		Labels: map[string]string{"armadaproject.io/pool": "gpu"},
	}}
	pod := &v1.Pod{Spec: v1.PodSpec{NodeName: "node-1"}}

	info := render(t, rendererWithNodes(node), pod, nil)

	assert.Equal(t, map[string]string{"armadaproject.io/pool": "gpu"}, info.Node.Labels)
	assert.Empty(t, info.Node.Annotations)
}

func TestRender_ReportsMissingNode(t *testing.T) {
	pod := &v1.Pod{Spec: v1.PodSpec{NodeName: "gone"}}

	info := render(t, rendererWithNodes(), pod, nil)

	assert.Equal(t, "gone", info.Node.Name)
	assert.False(t, *info.Node.Exists)
	assert.Empty(t, info.Node.Conditions)
}

func TestRender_OmitsNodeObjectWhenPodNeverScheduled(t *testing.T) {
	pod := &v1.Pod{Status: v1.PodStatus{Phase: v1.PodPending}}
	payload := rendererWithNodes().Render(pod, nil, TriggerPodFailed)

	assert.NotContains(t, payload, `"node"`)

	info := &PodDebugInfo{}
	require.NoError(t, json.Unmarshal([]byte(payload), info))
	assert.Nil(t, info.Node)
}

func TestRender_DescribesEvents(t *testing.T) {
	podEvent := event("0/8 nodes are available", func(e *v1.Event) {
		e.LastTimestamp = metav1.Time{Time: renderTime.Add(-time.Minute)}
	})
	nodeEvent := event("Node node-1 status is now: NodeNotReady", func(e *v1.Event) {
		e.Reason = "NodeNotReady"
		e.Type = "Normal"
		e.Source = v1.EventSource{Component: "node-controller"}
		e.LastTimestamp = metav1.Time{Time: renderTime.Add(-2 * time.Minute)}
	})
	node := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}}
	pod := &v1.Pod{Spec: v1.PodSpec{NodeName: "node-1"}}

	info := render(t, rendererWithNodeEvents(map[string][]*v1.Event{"node-1": {nodeEvent}}, node), pod, []*v1.Event{podEvent})

	require.Len(t, info.Pod.Events, 1)
	assert.Equal(t, EventInfo{
		Type:      "Warning",
		Reason:    "FailedScheduling",
		From:      "default-scheduler",
		Message:   "0/8 nodes are available",
		Timestamp: "2026-08-11T11:59:00Z",
	}, info.Pod.Events[0])
	require.Len(t, info.Node.Events, 1)
	assert.Equal(t, "NodeNotReady", info.Node.Events[0].Reason)
	assert.Equal(t, "node-controller", info.Node.Events[0].From)
}

// The node's events are where a drain or kubelet failure is described, so they are still reported for
// a node that has gone away - which is exactly when they matter most.
func TestRender_ReportsNodeEventsForMissingNode(t *testing.T) {
	pod := &v1.Pod{Spec: v1.PodSpec{NodeName: "gone"}}
	nodeEvents := map[string][]*v1.Event{"gone": {event("node was deleted", func(e *v1.Event) { e.Reason = "RemovingNode" })}}

	info := render(t, rendererWithNodeEvents(nodeEvents), pod, nil)

	assert.False(t, *info.Node.Exists)
	require.Len(t, info.Node.Events, 1)
	assert.Equal(t, "RemovingNode", info.Node.Events[0].Reason)
}

// Modern Kubernetes populates EventTime and Series rather than LastTimestamp. Sorting on
// LastTimestamp alone pins those events at the zero time, so keeping the most recent events would
// silently keep the oldest ones instead.
func TestRender_KeepsMostRecentEventsAcrossMixedTimestampFields(t *testing.T) {
	base := renderTime.Add(-time.Hour)
	events := []*v1.Event{
		event("oldest by LastTimestamp", func(e *v1.Event) {
			e.LastTimestamp = metav1.Time{Time: base}
		}),
		event("middle by EventTime", func(e *v1.Event) {
			e.EventTime = metav1.MicroTime{Time: base.Add(10 * time.Minute)}
		}),
		event("newest by Series", func(e *v1.Event) {
			e.Series = &v1.EventSeries{Count: 3, LastObservedTime: metav1.MicroTime{Time: base.Add(20 * time.Minute)}}
		}),
	}
	renderer := rendererWithNodes()
	renderer.config.Pod.MaxEvents = 2

	info := render(t, renderer, &v1.Pod{}, events)

	require.Len(t, info.Pod.Events, 2)
	assert.Equal(t, "middle by EventTime", info.Pod.Events[0].Message)
	assert.Equal(t, "newest by Series", info.Pod.Events[1].Message)
}

func TestRender_CapsEventCountsSeparatelyForPodAndNode(t *testing.T) {
	podEvents := make([]*v1.Event, 0, testMaxEvents+5)
	nodeEvents := make([]*v1.Event, 0, testMaxEvents+5)
	for i := 0; i < testMaxEvents+5; i++ {
		at := renderTime.Add(time.Duration(i) * time.Minute)
		podEvents = append(podEvents, event(fmt.Sprintf("pod event %d", i), func(e *v1.Event) {
			e.LastTimestamp = metav1.Time{Time: at}
		}))
		nodeEvents = append(nodeEvents, event(fmt.Sprintf("node event %d", i), func(e *v1.Event) {
			e.LastTimestamp = metav1.Time{Time: at}
		}))
	}
	node := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}}
	pod := &v1.Pod{Spec: v1.PodSpec{NodeName: "node-1"}}

	info := render(t, rendererWithNodeEvents(map[string][]*v1.Event{"node-1": nodeEvents}, node), pod, podEvents)

	require.Len(t, info.Pod.Events, testMaxEvents)
	require.Len(t, info.Node.Events, testMaxEvents)
	assert.Equal(t, "pod event 5", info.Pod.Events[0].Message, "the oldest events are dropped")
	assert.Equal(t, "pod event 14", info.Pod.Events[testMaxEvents-1].Message)
}

// Every list and message comes from Kubernetes, so nothing but these caps bounds the payload size.
func TestRender_CapsListsAndFieldsToBoundPayloadSize(t *testing.T) {
	containers := make([]v1.ContainerStatus, 50)
	finalizers := make([]string, 50)
	conditions := make([]v1.NodeCondition, 50)
	for i := range containers {
		containers[i] = v1.ContainerStatus{Name: strings.Repeat("c", 500), State: v1.ContainerState{Running: &v1.ContainerStateRunning{}}}
		finalizers[i] = strings.Repeat("f", 500)
		conditions[i] = v1.NodeCondition{Type: v1.NodeReady, Message: strings.Repeat("m", 500)}
	}
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			DeletionTimestamp: &metav1.Time{Time: renderTime.Add(-time.Minute)},
			Finalizers:        finalizers,
		},
		Spec:   v1.PodSpec{NodeName: "node-1"},
		Status: v1.PodStatus{ContainerStatuses: containers},
	}
	node := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}, Status: v1.NodeStatus{Conditions: conditions}}
	events := make([]*v1.Event, 30)
	for i := range events {
		events[i] = event(strings.Repeat("e", 2000))
	}

	renderer := rendererWithNodeEvents(map[string][]*v1.Event{"node-1": events}, node)
	payload := renderer.Render(pod, events, TriggerStuckTerminating)
	info := &PodDebugInfo{}
	require.NoError(t, json.Unmarshal([]byte(payload), info))

	assert.Len(t, info.Pod.Containers, maxRenderedListEntries)
	assert.Len(t, info.Pod.Finalizers, maxRenderedListEntries)
	assert.Len(t, info.Pod.Events, testMaxEvents)
	assert.Len(t, info.Node.Events, testMaxEvents)
	// Node conditions are deliberately uncapped, but each message is still trimmed.
	assert.Len(t, info.Node.Conditions, len(conditions))
	for _, condition := range info.Node.Conditions {
		assert.LessOrEqual(t, len(condition.Message), maxRenderedFieldSize+len("...[truncated]"))
	}
	for _, container := range info.Pod.Containers {
		assert.LessOrEqual(t, len(container.Name), maxRenderedFieldSize+len("...[truncated]"))
	}
	for _, podEvent := range info.Pod.Events {
		assert.LessOrEqual(t, len(podEvent.Message), maxRenderedFieldSize+len("...[truncated]"))
	}
	assert.Less(t, len(payload), 64*1024, "payload must stay bounded")
}

// Truncating by byte offset can split a rune, and invalid UTF-8 is rejected by json.Marshal, which
// would lose the whole payload.
func TestRender_TruncationProducesValidUtf8(t *testing.T) {
	for pad := 0; pad < 6; pad++ {
		message := strings.Repeat("€", maxRenderedFieldSize) + strings.Repeat("a", pad)
		pod := &v1.Pod{Status: v1.PodStatus{Reason: message}}

		info := render(t, rendererWithNodes(), pod, []*v1.Event{event(message)})

		require.Contains(t, info.Pod.Reason, "[truncated]", "pad=%d", pad)
		assert.True(t, utf8ValidRoundTrip(info.Pod.Reason), "truncated field must be valid UTF-8 (pad=%d)", pad)
		assert.True(t, utf8ValidRoundTrip(info.Pod.Events[0].Message), "truncated message must be valid UTF-8 (pad=%d)", pad)
	}
}

func utf8ValidRoundTrip(value string) bool {
	encoded, err := json.Marshal(value)
	return err == nil && len(encoded) > 0
}
