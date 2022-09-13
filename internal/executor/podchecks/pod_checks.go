package podchecks

import (
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"

	config "github.com/G-Research/armada/internal/executor/configuration/podchecks"
	"github.com/G-Research/armada/internal/executor/util"
)

type PodChecker interface {
	GetAction(pod *v1.Pod, podEvents []*v1.Event, timeInState time.Duration) (Action, string)
}

type PodChecks struct {
	eventChecks          eventChecker
	containerStateChecks containerStateChecker
	deadlineForUpdates   time.Duration
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

	return &PodChecks{eventChecks: ec, containerStateChecks: csc, deadlineForUpdates: cfg.DeadlineForUpdates}, nil
}

func (pc *PodChecks) GetAction(pod *v1.Pod, podEvents []*v1.Event, timeInState time.Duration) (Action, string) {
	messages := []string{}

	isNodeBad := pc.hasNoEventsOrStatus(pod, podEvents)
	if timeInState > pc.deadlineForUpdates && isNodeBad {
		return ActionRetry, "Pod status and pod events are both empty. Retrying"
	} else if isNodeBad {
		return ActionWait, "Pod status and pod events are both empty but we are under timelimit. Waiting"
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
	log.Infof("Pod checks for pod %s returned %s %s\n", pod.Name, resultAction, resultMessage)
	return resultAction, resultMessage
}

// If a node is bad, we can have no pod status and no pod events.
// We should retry the pod rather than wait
func (pc *PodChecks) hasNoEventsOrStatus(pod *v1.Pod, podEvents []*v1.Event) bool {
	containerStatus := util.GetPodContainerStatuses(pod)
	return len(containerStatus) == 0 && len(podEvents) == 0
}
