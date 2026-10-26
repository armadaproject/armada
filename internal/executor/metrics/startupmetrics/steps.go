package startupmetrics

import (
	"time"

	"github.com/gogo/protobuf/types"

	protoutil "github.com/armadaproject/armada/internal/common/proto"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

// Step names. A deliberately small set to start with - the two intervals between pod instants
// worth watching on their own - plus one step per configured extra step, under that step's own
// name, which is why these are steps rather than phases: the configuration calls them steps, the
// proto calls them steps, and a pod phase is a different thing entirely.
//
// A configured step named after one of these would merge with it, so the built-in names are kept
// distinctive.
const (
	schedulingStep      = "scheduling"
	sandboxCreationStep = "sandbox_creation"
)

type measuredStep struct {
	name     string
	duration time.Duration
}

// deriveSteps turns the instants the breakdown recorded into the intervals between them.
func deriveSteps(info *armadaevents.PodStartUpInfo) []measuredStep {
	if info == nil {
		return nil
	}

	steps := appendStep(nil, schedulingStep, info.CreatedAt, info.ScheduledAt)
	steps = appendStep(steps, sandboxCreationStep, info.ScheduledAt, info.SandboxReadyAt)
	for _, step := range info.ExtraSteps {
		if step == nil {
			continue
		}
		steps = appendStep(steps, step.Name, step.StartedAt, step.FinishedAt)
	}
	return steps
}

func appendStep(steps []measuredStep, name string, from, to *types.Timestamp) []measuredStep {
	duration := interval(from, to)
	if duration == nil {
		return steps
	}
	return append(steps, measuredStep{name: name, duration: *duration})
}

// interval is the time between two instants, or nil when either was not determined - in which case
// the gap is unknown rather than zero, and is already counted as a missing field. A negative
// interval is treated the same way: both instants in a pair come from one source, so one can only
// precede the other if that source is reporting something we do not understand.
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
