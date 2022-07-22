package eventbenchmark

import "time"

type EventDurationsByJobId struct {
	JobId  string           `json:"jobId"`
	Events []*EventDuration `json:"events"`
}

type EventDuration struct {
	Received time.Time     `json:"received"`
	Duration time.Duration `json:"duration"`
	Event    string        `json:"event"`
}

type TestBenchmarkReport struct {
	Name       string                   `json:"name"`
	Statistics map[string]*Statistics   `json:"statistics"`
	Summary    []*EventDurationsByJobId `json:"summary"`
	Subreports []*TestBenchmarkReport   `json:"subreports"`
}
