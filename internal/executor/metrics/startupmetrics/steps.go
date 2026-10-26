package startupmetrics

import (
	"time"

	"github.com/gogo/protobuf/types"

	protoutil "github.com/armadaproject/armada/internal/common/proto"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

// Steps that are currently measured
// Steps represent a section of the pod start up
// We record these separately to allow for deeper analysis of which section is taking a long time
const (
	schedulingStep      = "scheduling"
	sandboxCreationStep = "sandbox_creation"
)

type measuredStep struct {
	name     string
	duration time.Duration
}

func deriveSteps(info *armadaevents.PodStartUpInfo) []measuredStep {
	if info == nil {
		return nil
	}

	steps := addStep(nil, schedulingStep, info.CreatedAt, info.ScheduledAt)
	steps = addStep(steps, sandboxCreationStep, info.ScheduledAt, info.SandboxReadyAt)
	for _, step := range info.ExtraSteps {
		if step == nil {
			continue
		}
		steps = addStep(steps, step.Name, step.StartedAt, step.FinishedAt)
	}
	return steps
}

func addStep(steps []measuredStep, name string, from, to *types.Timestamp) []measuredStep {
	duration := interval(from, to)
	if duration == nil {
		return steps
	}
	return append(steps, measuredStep{name: name, duration: *duration})
}

func interval(from, to *types.Timestamp) *time.Duration {
	if from == nil || to == nil {
		return nil
	}
	duration := protoutil.ToStdTime(to).Sub(protoutil.ToStdTime(from))
	if duration < 0 {
		return nil
	}
	return &duration
}
