package sink

import (
	"github.com/armadaproject/armada/internal/scheduler/schedulerresult"
	"github.com/armadaproject/armada/internal/scheduler/simulator"
)

type Sink struct {
	jobWriter       *JobWriter
	fairShareWriter *FairShareWriter
}

func NewSink(outputDir string) (*Sink, error) {
	jobWriter, err := NewJobWriter(outputDir)
	if err != nil {
		return nil, err
	}
	fairShareWriter, err := NewFairShareWriter(outputDir)
	if err != nil {
		return nil, err
	}
	return &Sink{
		jobWriter:       jobWriter,
		fairShareWriter: fairShareWriter,
	}, nil
}

func (s *Sink) OnNewStateTransitions(transitions []*simulator.StateTransition) error {
	for _, t := range transitions {
		err := s.jobWriter.Update(t)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Sink) OnCycleEnd(result schedulerresult.SchedulerResult) error {
	err := s.fairShareWriter.Update(result)
	if err != nil {
		return err
	}
	return nil
}

func (s *Sink) close() {
	s.fairShareWriter.Close()
	s.jobWriter.Close()
}
