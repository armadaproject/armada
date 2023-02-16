package job

import (
	"time"

	v1 "k8s.io/api/core/v1"
	networking "k8s.io/api/networking/v1"
)

type SubmitJobMeta struct {
	RunMeta         *RunMeta
	Owner           string
	OwnershipGroups []string
}

type SubmitJob struct {
	Meta      SubmitJobMeta
	Pod       *v1.Pod
	Ingresses []*networking.Ingress
	Services  []*v1.Service
}

type RunMeta struct {
	JobId  string
	RunId  string
	JobSet string
	Queue  string
}

func (r *RunMeta) DeepCopy() *RunMeta {
	return &RunMeta{
		JobId:  r.JobId,
		RunId:  r.RunId,
		JobSet: r.JobSet,
		Queue:  r.Queue,
	}
}

type RunPhase int

const (
	// Leased is the initial state and occurs before we submit the run to kubernetes
	Leased RunPhase = iota
	// Active is any run present in Kubernetes
	Active
	// FailedSubmission is when a failed submission has been reported
	FailedSubmission
	// Missing is when we have lost track of the run
	// This may happen if we submit a pod to kubernetes but the pod never becomes present in kubernetes
	Missing
)

type RunState struct {
	Meta               *RunMeta
	KubernetesId       string
	Phase              RunPhase
	LastTransitionTime time.Time
}

func (r *RunState) DeepCopy() *RunState {
	return &RunState{
		Meta:               r.Meta.DeepCopy(),
		KubernetesId:       r.KubernetesId,
		Phase:              r.Phase,
		LastTransitionTime: r.LastTransitionTime,
	}
}
