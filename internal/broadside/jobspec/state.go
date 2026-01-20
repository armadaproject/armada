package jobspec

import "time"

type JobState int

const (
	StateLeased JobState = iota
	StatePending
	StateRunning
	StateSucceeded
	StateErrored
	StateCancelled
	StatePreempted
)

const (
	QueryStateQueued    = "QUEUED"
	QueryStateLeased    = "LEASED"
	QueryStatePending   = "PENDING"
	QueryStateRunning   = "RUNNING"
	QueryStateSucceeded = "SUCCEEDED"
	QueryStateFailed    = "FAILED"
	QueryStateCancelled = "CANCELLED"
	QueryStatePreempted = "PREEMPTED"
)

type ScheduledTransition struct {
	time    time.Time
	jobID   string
	runID   string
	toState JobState
}

func NewScheduledTransition(t time.Time, jobID, runID string, toState JobState) ScheduledTransition {
	return ScheduledTransition{
		time:    t,
		jobID:   jobID,
		runID:   runID,
		toState: toState,
	}
}

func (s ScheduledTransition) Time() time.Time   { return s.time }
func (s ScheduledTransition) JobID() string     { return s.jobID }
func (s ScheduledTransition) RunID() string     { return s.runID }
func (s ScheduledTransition) ToState() JobState { return s.toState }

type TransitionHeap []ScheduledTransition

func (h TransitionHeap) Len() int           { return len(h) }
func (h TransitionHeap) Less(i, j int) bool { return h[i].time.Before(h[j].time) }
func (h TransitionHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *TransitionHeap) Push(x any) {
	*h = append(*h, x.(ScheduledTransition))
}

func (h *TransitionHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
