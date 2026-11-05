package reporter

import (
	"encoding/json"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	v1 "k8s.io/api/core/v1"
	k8s_errors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/utils/clock"
	"k8s.io/utils/pointer"

	log "github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/executor/configuration"
)

// debugSchemaVersion is carried in every payload so readers can tell which fields to expect.
// Increment it when a field changes meaning or is removed.
const debugSchemaVersion = 1

// Messages come from Kubernetes and containers, so nothing bounds their length. The payload travels
// in a Pulsar message and lands in a Postgres column, so each is capped and so is every list.
const (
	maxRenderedFieldSize   = 256
	maxRenderedListEntries = 10
)

// DebugTrigger records why the debug data was captured, so a reader can select comparable records.
type DebugTrigger string

const (
	TriggerPodFailed              DebugTrigger = "podFailed"
	TriggerCancelledNotStarted    DebugTrigger = "cancelledNotStarted"
	TriggerStuckTerminating       DebugTrigger = "stuckTerminating"
	TriggerUndeletable            DebugTrigger = "undeletable"
	TriggerPreempted              DebugTrigger = "preempted"
	TriggerExternallyDeleted      DebugTrigger = "externallyDeleted"
	TriggerActiveDeadlineExceeded DebugTrigger = "activeDeadlineExceeded"
)

// Implemented by executor context.ClusterContext.
type nodeSource interface {
	GetNode(nodeName string) (*v1.Node, error)
	GetNodeEvents(nodeName string) ([]*v1.Event, error)
}

// PodDebugInfo is the debug field of PodError and JobRunTerminatedDebugInfo events, serialized as
// JSON. Scalars are intended to become columns when these payloads are loaded for ad-hoc analysis,
// so durations are seconds and timestamps are RFC3339.
type PodDebugInfo struct {
	SchemaVersion int          `json:"schemaVersion"`
	Trigger       DebugTrigger `json:"trigger"`
	Pod           PodInfo      `json:"pod"`
	Node          *NodeInfo    `json:"node,omitempty"`
}

type PodInfo struct {
	Phase  string `json:"phase,omitempty"`
	Reason string `json:"reason,omitempty"`
	// RestartPolicy is the pod's, which app containers inherit. Init containers can override it, so
	// theirs is reported per container.
	RestartPolicy string `json:"restartPolicy,omitempty"`

	// Set only for a pod Kubernetes has been asked to delete.
	DeletionTimestamp string   `json:"deletionTimestamp,omitempty"`
	ForceTerminated   *bool    `json:"forceTerminated,omitempty"`
	Finalizers        []string `json:"finalizers,omitempty"`

	InitContainers []ContainerInfo `json:"initContainers,omitempty"`
	Containers     []ContainerInfo `json:"containers,omitempty"`
	Events         []EventInfo     `json:"events,omitempty"`
}

func (p PodInfo) isEmpty() bool {
	return p.Phase == "" && p.Reason == "" && p.RestartPolicy == "" && p.DeletionTimestamp == "" &&
		len(p.InitContainers) == 0 && len(p.Containers) == 0 && len(p.Events) == 0
}

type NodeInfo struct {
	Name string `json:"name"`
	// Exists is false when the pod outlived the node it was assigned to.
	Exists        *bool  `json:"exists,omitempty"`
	Unschedulable *bool  `json:"unschedulable,omitempty"`
	Ready         string `json:"ready,omitempty"`
	ReadyReason   string `json:"readyReason,omitempty"`
	// Only the labels and annotations named in config are captured.
	Labels      map[string]string `json:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
	Conditions  []ConditionInfo   `json:"conditions,omitempty"`
	Events      []EventInfo       `json:"events,omitempty"`
}

type ContainerInfo struct {
	Name string `json:"name"`
	// RestartPolicy is set only where a container overrides the pod's - an init container with
	// Always is a native sidecar, running alongside the app containers rather than before them.
	RestartPolicy string `json:"restartPolicy,omitempty"`
	State         string `json:"state"`
	ExitCode      *int32 `json:"exitCode,omitempty"`
	Reason        string `json:"reason,omitempty"`
	RunSeconds    *int64 `json:"runSeconds,omitempty"`
}

type ConditionInfo struct {
	Type    string `json:"type"`
	Status  string `json:"status"`
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`
}

type EventInfo struct {
	Type      string `json:"type,omitempty"`
	Reason    string `json:"reason,omitempty"`
	From      string `json:"from,omitempty"`
	Message   string `json:"message,omitempty"`
	Timestamp string `json:"timestamp,omitempty"`
}

// DebugMessageRenderer builds the JSON debug payload for a pod.
type DebugMessageRenderer struct {
	nodes  nodeSource
	clock  clock.Clock
	config configuration.DebugEventsConfig
}

func NewDebugMessageRenderer(nodes nodeSource, config configuration.DebugEventsConfig) *DebugMessageRenderer {
	return &DebugMessageRenderer{nodes: nodes, clock: clock.RealClock{}, config: config}
}

// Render returns the JSON payload, or an empty string when capture is disabled or the payload could
// not be serialized. Callers attach the result unconditionally: an empty debug field is how every
// event looked before this existed, so the kill switch lives here rather than at each call site.
func (r *DebugMessageRenderer) Render(pod *v1.Pod, podEvents []*v1.Event, trigger DebugTrigger) string {
	if !r.config.Enabled {
		return ""
	}
	info := r.describe(pod, podEvents, trigger)
	// A pod that reported no state at all and was never scheduled leaves nothing to diagnose, so say
	// nothing rather than record the trigger alone.
	if info.Node == nil && info.Pod.isEmpty() {
		return ""
	}
	payload, err := json.Marshal(info)
	if err != nil {
		log.Errorf("Failed to serialize debug info for pod %s: %v", pod.Name, err)
		return ""
	}
	return string(payload)
}

func (r *DebugMessageRenderer) describe(pod *v1.Pod, podEvents []*v1.Event, trigger DebugTrigger) *PodDebugInfo {
	return &PodDebugInfo{
		SchemaVersion: debugSchemaVersion,
		Trigger:       trigger,
		Pod:           r.describePod(pod, podEvents),
		Node:          r.describeNode(pod.Spec.NodeName),
	}
}

// DeletionTimestamp is the deadline the API server computed as request time + grace period, not the
// request time, so the time past it is how long the pod survived its own SIGKILL deadline.
// DeletionGracePeriodSeconds is the in-API force-delete marker: it may only be shortened, so a force
// delete of an already-terminating pod drops it to zero.
func (r *DebugMessageRenderer) describePod(pod *v1.Pod, podEvents []*v1.Event) PodInfo {
	info := PodInfo{
		Phase:          string(pod.Status.Phase),
		Reason:         keepHeadWithinSize(pod.Status.Reason, maxRenderedFieldSize),
		RestartPolicy:  string(pod.Spec.RestartPolicy),
		InitContainers: describeContainers(pod.Status.InitContainerStatuses, initContainerRestartPolicies(pod)),
		Containers:     describeContainers(pod.Status.ContainerStatuses, nil),
		Events:         describeEvents(podEvents, r.config.Pod.MaxEvents),
	}
	if pod.DeletionTimestamp == nil {
		return info
	}

	info.DeletionTimestamp = pod.DeletionTimestamp.UTC().Format(time.RFC3339)
	info.ForceTerminated = pointer.Bool(pod.DeletionGracePeriodSeconds != nil && *pod.DeletionGracePeriodSeconds == 0)
	info.Finalizers = limitEntries(pod.Finalizers)
	return info
}

// A node that no longer resolves is itself the diagnosis for a pod that will not terminate, so it is
// reported rather than omitted - and its events are still worth having, since that is where a drain,
// eviction or kubelet failure is described.
func (r *DebugMessageRenderer) describeNode(nodeName string) *NodeInfo {
	if nodeName == "" {
		return nil
	}
	info := &NodeInfo{Name: nodeName}

	nodeEvents, err := r.nodes.GetNodeEvents(nodeName)
	if err != nil {
		log.Warnf("Failed retrieving node events for node %s: %v", nodeName, err)
	} else {
		info.Events = describeEvents(nodeEvents, r.config.Node.MaxEvents)
	}

	node, err := r.nodes.GetNode(nodeName)
	if k8s_errors.IsNotFound(err) {
		info.Exists = pointer.Bool(false)
		return info
	}
	if err != nil {
		log.Warnf("Failed retrieving node %s: %v", nodeName, err)
		return info
	}

	info.Exists = pointer.Bool(true)
	info.Unschedulable = pointer.Bool(node.Spec.Unschedulable)
	info.Labels = selectEntries(node.Labels, r.config.Node.Labels)
	info.Annotations = selectEntries(node.Annotations, r.config.Node.Annotations)
	for _, condition := range node.Status.Conditions {
		// Hoisted out of the conditions list so "did this run die on a NotReady node" is a single
		// field rather than a search through an array.
		if condition.Type == v1.NodeReady {
			info.Ready = string(condition.Status)
			info.ReadyReason = keepHeadWithinSize(condition.Reason, maxRenderedFieldSize)
		}
	}
	// Not capped: a node's conditions are a fixed set that node-problem-detector and the kubelet
	// define, so the count is bounded by the cluster's configuration rather than by its workload. Any
	// cap would have to drop some, and which ones matter is exactly what is not known in advance.
	for _, condition := range node.Status.Conditions {
		info.Conditions = append(info.Conditions, ConditionInfo{
			Type:    string(condition.Type),
			Status:  string(condition.Status),
			Reason:  keepHeadWithinSize(condition.Reason, maxRenderedFieldSize),
			Message: keepHeadWithinSize(strings.TrimSpace(condition.Message), maxRenderedFieldSize),
		})
	}
	return info
}

// restartPolicies is keyed by container name and holds only per-container overrides of the pod's
// policy, which PodInfo.RestartPolicy carries.
func describeContainers(statuses []v1.ContainerStatus, restartPolicies map[string]string) []ContainerInfo {
	containers := make([]ContainerInfo, 0, len(statuses))
	for _, status := range limitEntries(statuses) {
		container := ContainerInfo{
			Name:          keepHeadWithinSize(status.Name, maxRenderedFieldSize),
			RestartPolicy: restartPolicies[status.Name],
			State:         containerState(status.State),
		}
		if terminated := status.State.Terminated; terminated != nil {
			container.ExitCode = pointer.Int32(terminated.ExitCode)
			container.Reason = keepHeadWithinSize(terminated.Reason, maxRenderedFieldSize)
			if !terminated.StartedAt.IsZero() && !terminated.FinishedAt.IsZero() {
				container.RunSeconds = pointer.Int64(int64(terminated.FinishedAt.Sub(terminated.StartedAt.Time).Seconds()))
			}
		}
		if waiting := status.State.Waiting; waiting != nil {
			container.Reason = keepHeadWithinSize(waiting.Reason, maxRenderedFieldSize)
		}
		containers = append(containers, container)
	}
	return containers
}

func initContainerRestartPolicies(pod *v1.Pod) map[string]string {
	policies := make(map[string]string, len(pod.Spec.InitContainers))
	for _, container := range pod.Spec.InitContainers {
		if container.RestartPolicy != nil {
			policies[container.Name] = string(*container.RestartPolicy)
		}
	}
	return policies
}

func containerState(state v1.ContainerState) string {
	switch {
	case state.Running != nil:
		return "running"
	case state.Terminated != nil:
		return "terminated"
	case state.Waiting != nil:
		return "waiting"
	default:
		return "unknown"
	}
}

// Keeps the most recent events, since a teardown is explained by what happened last.
func describeEvents(podEvents []*v1.Event, limit int) []EventInfo {
	events := make([]v1.Event, 0, len(podEvents))
	for _, event := range podEvents {
		events = append(events, *event)
	}
	sort.SliceStable(events, func(i, j int) bool {
		return latestEventTime(events[i]).Before(latestEventTime(events[j]))
	})
	if len(events) > limit {
		events = events[len(events)-limit:]
	}

	described := make([]EventInfo, 0, len(events))
	for _, event := range events {
		described = append(described, EventInfo{
			Type:      event.Type,
			Reason:    keepHeadWithinSize(event.Reason, maxRenderedFieldSize),
			From:      keepHeadWithinSize(eventSource(event), maxRenderedFieldSize),
			Message:   keepHeadWithinSize(strings.TrimSpace(event.Message), maxRenderedFieldSize),
			Timestamp: eventTimestamp(event),
		})
	}
	return described
}

func eventSource(event v1.Event) string {
	if event.Source.Component != "" {
		return event.Source.Component
	}
	return event.ReportingController
}

func eventTimestamp(event v1.Event) string {
	latest := latestEventTime(event)
	if latest.IsZero() {
		return ""
	}
	return latest.UTC().Format(time.RFC3339)
}

// Modern Kubernetes populates EventTime and Series rather than LastTimestamp, so sorting on
// LastTimestamp alone pins those events at the zero time and "most recent" selects the oldest.
func latestEventTime(event v1.Event) time.Time {
	latest := event.LastTimestamp.Time
	if event.EventTime.Time.After(latest) {
		latest = event.EventTime.Time
	}
	if event.Series != nil && event.Series.LastObservedTime.Time.After(latest) {
		latest = event.Series.LastObservedTime.Time
	}
	return latest
}

// selectEntries picks the named keys that are present, so the payload only ever carries what an
// operator asked for. Values are capped; keys are not, since they come from config.
func selectEntries(entries map[string]string, names []string) map[string]string {
	selected := make(map[string]string, len(names))
	for _, name := range names {
		if value, present := entries[name]; present {
			selected[name] = keepHeadWithinSize(value, maxRenderedFieldSize)
		}
	}
	if len(selected) == 0 {
		return nil
	}
	return selected
}

func limitEntries[T any](entries []T) []T {
	if len(entries) <= maxRenderedListEntries {
		return entries
	}
	return entries[:maxRenderedListEntries]
}

func keepHeadWithinSize(message string, size int) string {
	if len(message) <= size {
		return message
	}
	// Drop the trailing partial rune the byte slice may have left. Decoding only the last rune
	// keeps a pre-existing invalid byte earlier in the string from discarding the whole prefix.
	trimmed := message[:size]
	if last, width := utf8.DecodeLastRuneInString(trimmed); last == utf8.RuneError && width <= 1 {
		trimmed = trimmed[:len(trimmed)-width]
	}
	return trimmed + "...[truncated]"
}
