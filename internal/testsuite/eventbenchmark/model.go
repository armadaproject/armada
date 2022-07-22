package eventbenchmark

import "time"

type GlobalBenchmarkReport struct {
	Statistics map[string]*Statistics     `json:"statistics"`
	Subreports []*TestCaseBenchmarkReport `json:"subreports"`
}

type TestCaseBenchmarkReport struct {
	Name       string                   `json:"name"`
	Statistics map[string]*Statistics   `json:"statistics"`
	Summary    []*EventDurationsByJobId `json:"summary"`
}

type EventDurationsByJobId struct {
	JobId  string           `json:"jobId"`
	Events []*EventDuration `json:"events"`
}

type EventDuration struct {
	Received time.Time     `json:"received"`
	Duration time.Duration `json:"duration"`
	Event    string        `json:"event"`
}

type Statistics struct {
	Min               int64   `json:"min"`
	Max               int64   `json:"max"`
	Average           float64 `json:"average"`
	Variance          float64 `json:"variance"`
	StandardDeviation float64 `json:"standardDeviation"`
}
