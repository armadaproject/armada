package reporter

import v1 "k8s.io/api/core/v1"

type EventReporter interface {
	ReportEvent(pod *v1.Pod)
}

type JobEventReporter struct {
	podEvents []*v1.Pod
	//TODO API CLIENT
}

func (jobEventReporter JobEventReporter) ReportEvent(pod v1.Pod) {

}
