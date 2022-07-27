package joblogger

import (
	"bytes"

	v1 "k8s.io/api/core/v1"
)

type podInfo struct {
	Kubectx   string
	Name      string
	Namespace string
	Phase     v1.PodPhase
	Scraped   bool
	Logs      *bytes.Buffer
}

func newPodInfo(kubectx string, pod *v1.Pod) *podInfo {
	return &podInfo{Kubectx: kubectx, Name: pod.Name, Namespace: pod.Namespace, Phase: pod.Status.Phase, Scraped: false}
}

func (p *podInfo) appendLog(logs *bytes.Buffer) {
	// if we get a stream of logs for the first time
	if p.Logs == nil || p.Logs.Cap() == 0 {
		p.Logs = logs
		return
	}
	// return if no new logs
	if p.Logs.Len() == logs.Len() {
		return
	}
	// otherwise append logs
	last := p.Logs.Len() - 1
	p.Logs.Write(logs.Bytes()[last:])
}

func (p *podInfo) hasStarted() bool {
	return p.Phase == v1.PodRunning || p.Phase == v1.PodSucceeded || p.Phase == v1.PodFailed
}

func (p *podInfo) hasCompleted() bool {
	return p.Phase == v1.PodSucceeded || p.Phase == v1.PodFailed
}
