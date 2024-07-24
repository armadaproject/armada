package metrics

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

func TestUpdate(t *testing.T) {
	ctx := armadacontext.Background()

	metrics, err := New(configuration.MetricsConfig{
		TrackedErrorRegexes:  nil,
		TrackedResourceNames: []v1.ResourceName{"cpu"},
		ResetInterval:        24 * time.Hour,
	})
	require.NoError(t, err)

	now := time.Now()

	queuedJob := testfixtures.NewJob(uuid.NewString(),
		"test-jobset",
		"test-queue",
		1,
		&schedulerobjects.JobSchedulingInfo{},
		true,
		0,
		false,
		false,
		false,
		time.Now().UnixNano(),
		true)

	jobRunErrorsByRunId := map[uuid.UUID]*armadaevents.Error{
		uuid.MustParse(queuedJob.Id()): {
			Terminal: true,
			Reason: &armadaevents.Error_PodError{
				PodError: &armadaevents.PodError{
					Message: "my error",
				},
			},
		},
	}

	leasedJob := queuedJob.WithNewRun("test-executor", "node1", "test-node", "test-pool", 1)
	pendingJob := leasedJob.WithUpdatedRun(leasedJob.LatestRun().WithPendingTime(addSeconds(now, 1)))
	runningJob := pendingJob.WithUpdatedRun(pendingJob.LatestRun().WithRunningTime(addSeconds(now, 2)))
	finishedJob := runningJob.WithUpdatedRun(runningJob.LatestRun().WithTerminatedTime(addSeconds(now, 3)))
	preemptedJob := finishedJob.WithUpdatedRun(runningJob.LatestRun().WithPreemptedTime(addSeconds(now, 4)))

	require.NoError(t, metrics.UpdateQueued(queuedJob))
	require.NoError(t, metrics.UpdateLeased(context.JobSchedulingContextFromJob(leasedJob)))
	require.NoError(t, metrics.UpdatePending(pendingJob))
	require.NoError(t, metrics.UpdateRunning(runningJob))
	require.NoError(t, metrics.UpdateSucceeded(finishedJob))
	require.NoError(t, metrics.UpdateCancelled(finishedJob))
	require.NoError(t, metrics.UpdateFailed(ctx, finishedJob, jobRunErrorsByRunId))
	require.NoError(t, metrics.UpdatePreempted(preemptedJob))
}

func addSeconds(t time.Time, seconds int) *time.Time {
	t = t.Add(time.Duration(seconds) * time.Second)
	return &t
}
