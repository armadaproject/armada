package podchecks

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"time"

	config "github.com/armadaproject/armada/internal/executor/configuration/podchecks"
	"github.com/armadaproject/armada/internal/executor/util"
	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/kubectl/pkg/describe"
)

type PodChecker interface {
	GetAction(pod *v1.Pod, podEvents []*v1.Event, timeInState time.Duration) (Action, Cause, string)
}

type PodChecks struct {
	eventChecks               eventCheckera
	containerStateChecks      containerStateChecker
	deadlineForUpdates        time.Duration
	deadlineForNodeAssignment time.Duration
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
	}, nil
}

func (pc *PodChecks) GetAction(pod *v1.Pod, podEvents []*v1.Event, timeInState time.Duration) (Action, Cause, string) {
	messages := []string{}

	isAssignedToNode := pod.Spec.NodeName != ""
	if timeInState > pc.deadlineForNodeAssignment && !isAssignedToNode {
		return ActionRetry, NoNodeAssigned, fmt.Sprintf("Pod could not been scheduled in within %s deadline. Retrying", pc.deadlineForNodeAssignment)
	}

	isNodeBad := pc.hasNoEventsOrStatus(pod, podEvents)
	if timeInState > pc.deadlineForUpdates && isNodeBad {
		return ActionRetry, NoStatusUpdates, "Pod status and pod events are both empty. Retrying"
	} else if isNodeBad {
		return ActionWait, NoStatusUpdates, "Pod status and pod events are both empty but we are under timelimit. Waiting"
	}

	eventAction, message := pc.eventChecks.getAction(pod.Name, podEvents, timeInState)
	if eventAction != ActionWait {
		messages = append(messages, message)
	}
	buf := new(bytes.Buffer)
	writer := NewPrefixWriter(buf)
	noPointerEvents := make([]v1.Event, len(podEvents))
	for i := range podEvents {
		noPointerEvents = append(noPointerEvents, *podEvents[i])
	}
	eventsList := &v1.EventList{
		Items: noPointerEvents,
	}
	describe.DescribeEvents(eventsList, writer)
	fmt.Println(buf.String())

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

type prefixWriter struct {
	out io.Writer
}

// NewPrefixWriter creates a new PrefixWriter.
func NewPrefixWriter(out io.Writer) describe.PrefixWriter {
	return &prefixWriter{out: out}
}

func (pw *prefixWriter) Write(level int, format string, a ...interface{}) {
	levelSpace := "  "
	prefix := ""
	for i := 0; i < level; i++ {
		prefix += levelSpace
	}
	fmt.Fprintf(pw.out, prefix+format, a...)
}

func (pw *prefixWriter) WriteLine(a ...interface{}) {
	fmt.Fprintln(pw.out, a...)
}

func (pw *prefixWriter) Flush() {
}

// If a node is bad, we can have no pod status and no pod events.
// We should retry the pod rather than wait
func (pc *PodChecks) hasNoEventsOrStatus(pod *v1.Pod, podEvents []*v1.Event) bool {
	containerStatus := util.GetPodContainerStatuses(pod)
	return len(containerStatus) == 0 && len(podEvents) == 0
}
