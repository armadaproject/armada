package sink

import (
	"time"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/scheduler/scheduling"
	"github.com/armadaproject/armada/internal/scheduler/simulator/model"
)

type Sink interface {
	OnNewStateTransitions(transitions []*model.StateTransition) error
	OnCycleEnd(time time.Time, result *scheduling.SchedulerResult) error
	Close(ctx *armadacontext.Context)
}

type ParquetSink struct {
	jobWriter       *JobWriter
	fairShareWriter *QueueStatsWriter
}

func NewParquetSink(outputDir string) (*ParquetSink, error) {
	jobWriter, err := NewJobWriter(outputDir)
	if err != nil {
		return nil, err
	}
	fairShareWriter, err := NewQueueStatsWriter(outputDir)
	if err != nil {
		return nil, err
	}
	return &ParquetSink{
		jobWriter:       jobWriter,
		fairShareWriter: fairShareWriter,
	}, nil
}

func (s *ParquetSink) OnNewStateTransitions(transitions []*model.StateTransition) error {
	for _, t := range transitions {
		err := s.jobWriter.Update(t)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *ParquetSink) OnCycleEnd(time time.Time, result *scheduling.SchedulerResult) error {
	err := s.fairShareWriter.Update(time, result)
	if err != nil {
		return err
	}
	return nil
}

func (s *ParquetSink) Close(ctx *armadacontext.Context) {
	s.fairShareWriter.Close(ctx)
	s.jobWriter.Close(ctx)
}

type NullSink struct{}

func (s NullSink) OnNewStateTransitions(_ []*model.StateTransition) error {
	return nil
}

func (s NullSink) OnCycleEnd(_ time.Time, _ *scheduling.SchedulerResult) error {
	return nil
}

func (s NullSink) Close(ctx *armadacontext.Context) {}
