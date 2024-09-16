package sink

import (
	"github.com/armadaproject/armada/internal/scheduler/schedulerresult"
	"github.com/armadaproject/armada/internal/scheduler/simulator"
)

type Sink struct {
	jobWriter   *JobWriter
	StatsWriter *StatsWriter
}

func NewSink(outputDir string) (*Sink, error) {
	jobWriter, err := NewJobWriter(outputDir)
	if err != nil {
		return nil, err
	}
	statsWriter, err := NewStatsWriter(outputDir)
	if err != nil {
		return nil, err
	}
	return &Sink{
		jobWriter:   jobWriter,
		StatsWriter: statsWriter,
	}, nil
}

func (s *Sink) OnNewStateTransitions(transitions []*simulator.StateTransition) {
	for _, t := range transitions {
		s.jobWriter.Update(t)
	}
}

func (s *Sink) OnCycleEnd(result schedulerresult.SchedulerResult) {

}
