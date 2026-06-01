package podchecks

import (
	"fmt"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"

	log "github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/common/slices"
	config "github.com/armadaproject/armada/internal/executor/configuration/podchecks"
	"github.com/armadaproject/armada/internal/executor/util"
)

type PodChecker interface {
	GetAction(pod *v1.Pod, podEvents []*v1.Event, timeInState time.Duration) (Action, Cause, string)
}

type PodChecks struct {
	eventChecks               eventChecker
	containerStateChecks      containerStateChecker
	deadlineForUpdates        time.Duration
	deadlineForNodeAssignment time.Duration
	deadlineForInitContainers time.Duration
}

func NewPodChecks(cfg config.Checks) (*PodChecks, error) {
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
		eventChecks:               ec,
		containerStateChecks:      csc,
		deadlineForUpdates:        cfg.DeadlineForUpdates,
		deadlineForNodeAssignment: cfg.DeadlineForNodeAssignment,
		deadlineForInitContainers: cfg.DeadlineForInitContainers,
	}, nil
}

func (pc *PodChecks) GetAction(pod *v1.Pod, podEvents []*v1.Event, timeInState time.Duration) (Action, Cause, string) {
	messages := []string{}

	elapsedForChecks := timeInState
	initComplete, initCompletionTime := getInitCompletionInfo(pod)

	if pc.deadlineForInitContainers > 0 && len(pod.Status.InitContainerStatuses) > 0 {
		if !initComplete && timeInState > pc.deadlineForInitContainers {
			return ActionFail, PodStartupIssue, fmt.Sprintf("init containers did not complete within deadline of %s", pc.deadlineForInitContainers)
		}
	}

	if initComplete && !initCompletionTime.IsZero() {
		elapsedForChecks = time.Since(initCompletionTime)
	}

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

	eventAction, message := pc.eventChecks.getAction(pod.Name, podEvents, elapsedForChecks)
	if eventAction != ActionWait {
		messages = append(messages, message)
	}

	containerStateAction, message := pc.containerStateChecks.getAction(pod, elapsedForChecks)
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

func getInitCompletionInfo(pod *v1.Pod) (bool, time.Time) {
	if len(pod.Status.InitContainerStatuses) == 0 {
		return false, time.Time{}
	}

	var maxFinishedAt time.Time

	for _, initStatus := range pod.Status.InitContainerStatuses {
		if initStatus.State.Terminated == nil {
			return false, time.Time{}
		}
		if initStatus.State.Terminated.ExitCode != 0 {
			return false, time.Time{}
		}

		finishedAt := initStatus.State.Terminated.FinishedAt.Time
		if finishedAt.After(maxFinishedAt) {
			maxFinishedAt = finishedAt
		}
	}

	return true, maxFinishedAt
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
