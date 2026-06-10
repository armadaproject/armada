package podchecks

import (
	"fmt"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/utils/clock"

	log "github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/common/slices"
	config "github.com/armadaproject/armada/internal/executor/configuration/podchecks"
	"github.com/armadaproject/armada/internal/executor/util"
)

type PodChecker interface {
	GetAction(pod *v1.Pod, podEvents []*v1.Event) (Action, Cause, string)
}

type PodChecks struct {
	clock                     clock.Clock
	eventChecks               eventChecker
	containerStateChecks      containerStateChecker
	deadlineForUpdates        time.Duration
	deadlineForNodeAssignment time.Duration
	deadlineForInitContainers time.Duration
}

func NewPodChecks(cfg config.Checks, clk clock.Clock) (*PodChecks, error) {
	log.Info("Creating pod checks...")
	ec, err := newEventChecks(cfg.Events)
	if err != nil {
		return nil, err
	}

	csc, err := newContainerStateChecks(cfg.ContainerStatuses)
	if err != nil {
		return nil, err
	}

	return &PodChecks{
		clock:                     clk,
		eventChecks:               ec,
		containerStateChecks:      csc,
		deadlineForUpdates:        cfg.DeadlineForUpdates,
		deadlineForNodeAssignment: cfg.DeadlineForNodeAssignment,
		deadlineForInitContainers: cfg.DeadlineForInitContainers,
	}, nil
}

func (pc *PodChecks) GetAction(pod *v1.Pod, podEvents []*v1.Event) (Action, Cause, string) {
	lastStateChange, err := util.LastStatusChange(pod)
	if err != nil {
		log.Errorf("Unable to get lastStateChange for pod %s: %v", pod.Name, err)
		return ActionWait, None, ""
	}

	currentTime := pc.clock.Now()
	messages := []string{}

	if pc.deadlineForInitContainers > 0 {
		initContainerState := getInitContainerState(pod)
		if initContainerState.startedAt != nil && initContainerState.completedAt == nil && currentTime.Sub(*initContainerState.startedAt) > pc.deadlineForInitContainers {
			return ActionFail, PodStartupIssue, fmt.Sprintf("init containers did not complete within deadline of %s", pc.deadlineForInitContainers)
		}
	}

	timeInState := currentTime.Sub(lastStateChange)

	isAssignedToNode := pod.Spec.NodeName != ""
	if timeInState > pc.deadlineForNodeAssignment && !isAssignedToNode {
		return ActionRetry, NoNodeAssigned, fmt.Sprintf("Pod could not been scheduled in within %s deadline. Retrying", pc.deadlineForNodeAssignment)
	}

	isNodeBad := pc.isBadNode(pod, podEvents)
	if timeInState > pc.deadlineForUpdates && isNodeBad {
		return ActionRetry, NoStatusUpdates, fmt.Sprintf("Pod has received no updates within %s deadline - likely the node is bad. Retrying", pc.deadlineForUpdates)
	} else if isNodeBad {
		return ActionWait, NoStatusUpdates, "Pod status and pod events are both empty but we are under time limit. Waiting"
	}

	eventAction, message := pc.eventChecks.getAction(pod.Name, podEvents, timeInState)
	if eventAction != ActionWait {
		messages = append(messages, message)
	}

	containerStateAction, message := pc.containerStateChecks.getAction(pod, timeInState)
	if containerStateAction != ActionWait {
		messages = append(messages, message)
	}

	resultAction := maxAction(eventAction, containerStateAction)
	resultMessage := strings.Join(messages, "\n")
	var cause Cause
	if resultAction != ActionWait {
		cause = PodStartupIssue
	}
	log.Infof("Pod checks for pod %s returned %s %s\n", pod.Name, resultAction, resultMessage)
	return resultAction, cause, resultMessage
}

type initContainerState struct {
	startedAt   *time.Time
	completedAt *time.Time
}

func getInitContainerState(pod *v1.Pod) initContainerState {
	state := initContainerState{}
	statusByName := make(map[string]v1.ContainerStatus)
	for _, initStatus := range pod.Status.InitContainerStatuses {
		statusByName[initStatus.Name] = initStatus
	}

	nonSidecarInitContainerCount := 0
	successfulInitContainerCount := 0
	for _, container := range pod.Spec.InitContainers {
		if policy := container.RestartPolicy; policy != nil && *policy == v1.ContainerRestartPolicyAlways {
			continue
		}
		nonSidecarInitContainerCount++

		initStatus, ok := statusByName[container.Name]
		if !ok {
			continue
		}
		if running := initStatus.State.Running; running != nil {
			state.startedAt = earliestTime(state.startedAt, running.StartedAt.Time)
		}
		if terminated := initStatus.State.Terminated; terminated != nil {
			state.startedAt = earliestTime(state.startedAt, terminated.StartedAt.Time)
			if terminated.ExitCode != 0 {
				continue
			}
			successfulInitContainerCount++
			state.completedAt = latestTime(state.completedAt, terminated.FinishedAt.Time)
		}
	}

	if nonSidecarInitContainerCount == 0 || successfulInitContainerCount != nonSidecarInitContainerCount {
		state.completedAt = nil
	}

	return state
}

func earliestTime(current *time.Time, candidate time.Time) *time.Time {
	if candidate.IsZero() {
		return current
	}
	if current == nil || candidate.Before(*current) {
		return &candidate
	}
	return current
}

func latestTime(current *time.Time, candidate time.Time) *time.Time {
	if candidate.IsZero() {
		return current
	}
	if current == nil || candidate.After(*current) {
		return &candidate
	}
	return current
}

// This func is trying to determine if the node is bad based on the kubelet not updating the pod at all
func (pc *PodChecks) isBadNode(pod *v1.Pod, podEvents []*v1.Event) bool {
	// Ignore the scheduling events as these come from the kube-scheduler
	podEvents = slices.Filter(podEvents, func(e *v1.Event) bool {
		return e.Reason != EventReasonScheduled && e.Reason != EvenReasonFailedScheduling
	})

	containerStatus := util.GetPodContainerStatuses(pod)
	return len(containerStatus) == 0 && len(podEvents) == 0
}
