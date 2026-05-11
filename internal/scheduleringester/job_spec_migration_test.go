package scheduleringester

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/compress"
	f "github.com/armadaproject/armada/internal/common/ingest/testfixtures"
	protoutil "github.com/armadaproject/armada/internal/common/proto"
	schedulerdb "github.com/armadaproject/armada/internal/scheduler/database"
)

func TestHandleSubmitJob_MigrationPhases(t *testing.T) {
	cases := []struct {
		name                string
		phase               schedulerdb.JobSpecMigrationPhase
		wantJobHasBlobs     bool
		wantJobSpecsEmitted bool
	}{
		{"legacy", schedulerdb.JobSpecMigrationPhaseLegacy, true, false},
		{"cutover", schedulerdb.JobSpecMigrationPhaseCutover, false, true},
		{"dualWrite", schedulerdb.JobSpecMigrationPhaseDualWrite, true, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			converter, err := NewJobSetEventsInstructionConverter(m, tc.phase)
			require.NoError(t, err)

			es := f.NewEventSequence(f.Submit)
			ops := converter.dbOperationsFromEventSequence(es)

			var insertJobs InsertJobs
			var insertSpecs InsertJobSpecs
			for _, op := range ops {
				switch o := op.(type) {
				case InsertJobs:
					insertJobs = o
				case InsertJobSpecs:
					insertSpecs = o
				}
			}

			require.NotNil(t, insertJobs, "every submit emits an InsertJobs row")
			job := insertJobs[f.JobId]
			require.NotNil(t, job)

			expectedGroups := compress.MustCompressStringArray(f.Groups, compressor)

			if tc.wantJobHasBlobs {
				require.NotNil(t, job.SubmitMessage)
				assertSubmitMessagesEqual(t, protoutil.MustMarshallAndCompress(f.Submit.GetSubmitJob(), compressor), job.SubmitMessage)
				assert.Equal(t, expectedGroups, job.Groups)
			} else {
				assert.Nil(t, job.SubmitMessage)
				assert.Nil(t, job.Groups)
			}

			if tc.wantJobSpecsEmitted {
				require.NotNil(t, insertSpecs, "dualWrite and cutover phases emit an InsertJobSpecs row")
				spec := insertSpecs[f.JobId]
				require.NotNil(t, spec)
				require.NotNil(t, spec.SubmitMessage)
				assertSubmitMessagesEqual(t, protoutil.MustMarshallAndCompress(f.Submit.GetSubmitJob(), compressor), spec.SubmitMessage)
				assert.Equal(t, expectedGroups, spec.Groups)
			} else {
				assert.Nil(t, insertSpecs, "legacy phase must not emit InsertJobSpecs")
			}
		})
	}
}
