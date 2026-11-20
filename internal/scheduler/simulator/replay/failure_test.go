package replay

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/scheduler/scheduling"
)

func TestIndependentlyTimedOutPoolFailsRound(t *testing.T) {
	round := &scheduling.SchedulerResult{PoolResults: []*scheduling.PoolSchedulingResult{
		{Name: "completed", Outcome: *scheduling.NewPoolSchedulingOutcome(scheduling.PoolSchedulingTerminationReasonCompleted, nil)},
		{Name: "timed-out", Outcome: *scheduling.NewPoolSchedulingOutcome(scheduling.PoolSchedulingTerminationReasonTimeout, context.DeadlineExceeded)},
	}}
	require.ErrorIs(t, checkPoolOutcomes(t.Context(), round), context.DeadlineExceeded)
}

func TestParquetOutputPropagatesWriteStopAndCloseErrors(t *testing.T) {
	writer, err := createParquet(filepath.Join(t.TempDir(), "output.parquet"), new(attemptRow))
	require.NoError(t, err)
	require.NoError(t, writer.w.Write(attemptRow{JobID: "job", Ordinal: 1}))
	require.NoError(t, writer.file.Close())
	require.ErrorContains(t, writer.close(), "file already closed")
}
