package podchecks

type Cause int

const (
	None Cause = iota
	PodStartupIssue
	NoStatusUpdates
	NoNodeAssigned
)
