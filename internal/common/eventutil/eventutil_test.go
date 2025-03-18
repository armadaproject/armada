package eventutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/armadaproject/armada/internal/common/testutil"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

func TestCompactSequences_Basic(t *testing.T) {
	sequences := []*armadaevents.EventSequence{
		{
			Queue:      "queue1",
			UserId:     "userId1",
			JobSetName: "jobSetName1",
			Groups:     []string{"group1", "group2"},
			Events: []*armadaevents.EventSequence_Event{
				{Event: &armadaevents.EventSequence_Event_SubmitJob{}},
			},
		},
		{
			Queue:      "queue2",
			UserId:     "userId2",
			JobSetName: "jobSetName2",
			Groups:     []string{"group1", "group2"},
			Events: []*armadaevents.EventSequence_Event{
				{Event: &armadaevents.EventSequence_Event_SubmitJob{}},
			},
		},
		{
			Queue:      "queue1",
			UserId:     "userId1",
			JobSetName: "jobSetName1",
			Groups:     []string{"group1", "group2"},
			Events: []*armadaevents.EventSequence_Event{
				{Event: &armadaevents.EventSequence_Event_CancelJob{}},
			},
		},
	}

	expected := []*armadaevents.EventSequence{
		{
			Queue:      "queue1",
			UserId:     "userId1",
			JobSetName: "jobSetName1",
			Groups:     []string{"group1", "group2"},
			Events: []*armadaevents.EventSequence_Event{
				{Event: &armadaevents.EventSequence_Event_SubmitJob{}},
				{Event: &armadaevents.EventSequence_Event_CancelJob{}},
			},
		},
		{
			Queue:      "queue2",
			UserId:     "userId2",
			JobSetName: "jobSetName2",
			Groups:     []string{"group1", "group2"},
			Events: []*armadaevents.EventSequence_Event{
				{Event: &armadaevents.EventSequence_Event_SubmitJob{}},
			},
		},
	}

	actual := CompactEventSequences(sequences)
	assert.Equal(t, expected, actual)
}

func TestCompactSequences_JobSetOrder(t *testing.T) {
	sequences := []*armadaevents.EventSequence{
		{
			Queue:      "queue1",
			UserId:     "userId1",
			JobSetName: "jobSetName1",
			Groups:     []string{"group1", "group2"},
			Events: []*armadaevents.EventSequence_Event{
				{Event: &armadaevents.EventSequence_Event_SubmitJob{}},
			},
		},
		{
			Queue:      "queue1",
			UserId:     "userId1",
			JobSetName: "jobSetName1",
			Groups:     []string{"group1", "group2", "group3"},
			Events: []*armadaevents.EventSequence_Event{
				{Event: &armadaevents.EventSequence_Event_SubmitJob{}},
			},
		},
		{
			Queue:      "queue1",
			UserId:     "userId1",
			JobSetName: "jobSetName2",
			Groups:     []string{"group1", "group2"},
			Events: []*armadaevents.EventSequence_Event{
				{Event: &armadaevents.EventSequence_Event_SubmitJob{}},
			},
		},
		{
			Queue:      "queue1",
			UserId:     "userId1",
			JobSetName: "jobSetName1",
			Groups:     []string{"group1", "group2"},
			Events: []*armadaevents.EventSequence_Event{
				{Event: &armadaevents.EventSequence_Event_ReprioritiseJob{}},
			},
		},
		{
			Queue:      "queue1",
			UserId:     "userId1",
			JobSetName: "jobSetName2",
			Groups:     []string{"group1", "group2"},
			Events: []*armadaevents.EventSequence_Event{
				{Event: &armadaevents.EventSequence_Event_CancelJob{}},
			},
		},
		{
			Queue:      "queue1",
			UserId:     "userId1",
			JobSetName: "jobSetName1",
			Groups:     []string{"group1", "group2"},
			Events: []*armadaevents.EventSequence_Event{
				{Event: &armadaevents.EventSequence_Event_CancelJob{}},
			},
		},
	}

	expected := []*armadaevents.EventSequence{
		{
			Queue:      "queue1",
			UserId:     "userId1",
			JobSetName: "jobSetName1",
			Groups:     []string{"group1", "group2"},
			Events: []*armadaevents.EventSequence_Event{
				{Event: &armadaevents.EventSequence_Event_SubmitJob{}},
			},
		},
		{
			Queue:      "queue1",
			UserId:     "userId1",
			JobSetName: "jobSetName1",
			Groups:     []string{"group1", "group2", "group3"},
			Events: []*armadaevents.EventSequence_Event{
				{Event: &armadaevents.EventSequence_Event_SubmitJob{}},
			},
		},
		{
			Queue:      "queue1",
			UserId:     "userId1",
			JobSetName: "jobSetName1",
			Groups:     []string{"group1", "group2"},
			Events: []*armadaevents.EventSequence_Event{
				{Event: &armadaevents.EventSequence_Event_ReprioritiseJob{}},
				{Event: &armadaevents.EventSequence_Event_CancelJob{}},
			},
		},
		{
			Queue:      "queue1",
			UserId:     "userId1",
			JobSetName: "jobSetName2",
			Groups:     []string{"group1", "group2"},
			Events: []*armadaevents.EventSequence_Event{
				{Event: &armadaevents.EventSequence_Event_SubmitJob{}},
				{Event: &armadaevents.EventSequence_Event_CancelJob{}},
			},
		},
	}

	actual := CompactEventSequences(sequences)
	assert.Equal(t, expected, actual)
}

func TestCompactSequences_Groups(t *testing.T) {
	sequences := []*armadaevents.EventSequence{
		{
			Queue:      "queue1",
			UserId:     "userId1",
			JobSetName: "jobSetName1",
			Groups:     []string{"group1"},
			Events: []*armadaevents.EventSequence_Event{
				{Event: &armadaevents.EventSequence_Event_SubmitJob{}},
			},
		},
		{
			Queue:      "queue1",
			UserId:     "userId1",
			JobSetName: "jobSetName1",
			Groups:     []string{"group1"},
			Events: []*armadaevents.EventSequence_Event{
				{Event: &armadaevents.EventSequence_Event_CancelJob{}},
			},
		},
		{
			Queue:      "queue1",
			UserId:     "userId1",
			JobSetName: "jobSetName1",
			Groups:     []string{},
			Events: []*armadaevents.EventSequence_Event{
				{Event: &armadaevents.EventSequence_Event_SubmitJob{}},
			},
		},
		{
			Queue:      "queue1",
			UserId:     "userId1",
			JobSetName: "jobSetName1",
			Groups:     []string{},
			Events: []*armadaevents.EventSequence_Event{
				{Event: &armadaevents.EventSequence_Event_CancelJob{}},
			},
		},
		{
			Queue:      "queue1",
			UserId:     "userId1",
			JobSetName: "jobSetName1",
			Groups:     nil,
			Events: []*armadaevents.EventSequence_Event{
				{Event: &armadaevents.EventSequence_Event_SubmitJob{}},
			},
		},
		{
			Queue:      "queue1",
			UserId:     "userId1",
			JobSetName: "jobSetName1",
			Groups:     nil,
			Events: []*armadaevents.EventSequence_Event{
				{Event: &armadaevents.EventSequence_Event_CancelJob{}},
			},
		},
	}

	expected := []*armadaevents.EventSequence{
		{
			Queue:      "queue1",
			UserId:     "userId1",
			JobSetName: "jobSetName1",
			Groups:     []string{"group1"},
			Events: []*armadaevents.EventSequence_Event{
				{Event: &armadaevents.EventSequence_Event_SubmitJob{}},
				{Event: &armadaevents.EventSequence_Event_CancelJob{}},
			},
		},
		{
			Queue:      "queue1",
			UserId:     "userId1",
			JobSetName: "jobSetName1",
			Groups:     []string{},
			Events: []*armadaevents.EventSequence_Event{
				{Event: &armadaevents.EventSequence_Event_SubmitJob{}},
				{Event: &armadaevents.EventSequence_Event_CancelJob{}},
				{Event: &armadaevents.EventSequence_Event_SubmitJob{}},
				{Event: &armadaevents.EventSequence_Event_CancelJob{}},
			},
		},
	}

	actual := CompactEventSequences(sequences)
	assert.Equal(t, expected, actual)
}

func TestSequenceEventListSizeBytes(t *testing.T) {
	jobId := util.NewULID()

	sequence := &armadaevents.EventSequence{
		Queue:      "",
		UserId:     "",
		JobSetName: "",
		Groups:     []string{},
		Events: []*armadaevents.EventSequence_Event{
			{
				Event: &armadaevents.EventSequence_Event_CancelledJob{
					CancelledJob: &armadaevents.CancelledJob{
						JobId: jobId,
					},
				},
			},
		},
	}

	sequenceSizeBytes := uint(proto.Size(sequence))
	// If this fails, it means that the sequenceEventListOverheadSizeBytes constant is possibly too small
	// We are showing our safe estimate of the byte overhead added by the event list in proto is definitely large enough
	//  by showing it is larger than a sequence with a single event (as that sequence contains the overhead added by the event list)
	assert.True(t, sequenceSizeBytes < sequenceEventListOverheadSizeBytes)
}

func TestLimitSequencesEventMessageCount(t *testing.T) {
	input := []*armadaevents.EventSequence{
		{
			Queue:      "queue1",
			UserId:     "userId1",
			JobSetName: "jobSetName1",
			Groups:     []string{"group1", "group2"},
			Events: []*armadaevents.EventSequence_Event{
				{Event: &armadaevents.EventSequence_Event_SubmitJob{SubmitJob: &armadaevents.SubmitJob{JobId: "a"}}},
				{Event: &armadaevents.EventSequence_Event_SubmitJob{SubmitJob: &armadaevents.SubmitJob{JobId: "b"}}},
				{Event: &armadaevents.EventSequence_Event_SubmitJob{SubmitJob: &armadaevents.SubmitJob{JobId: "c"}}},
			},
		},
		{
			Queue:      "queue2",
			UserId:     "userId1",
			JobSetName: "jobSetName1",
			Groups:     []string{"group1", "group2"},
			Events: []*armadaevents.EventSequence_Event{
				{Event: &armadaevents.EventSequence_Event_SubmitJob{SubmitJob: &armadaevents.SubmitJob{JobId: "d"}}},
				{Event: &armadaevents.EventSequence_Event_SubmitJob{SubmitJob: &armadaevents.SubmitJob{JobId: "e"}}},
			},
		},
	}

	expected := []*armadaevents.EventSequence{
		{
			Queue:      "queue1",
			UserId:     "userId1",
			JobSetName: "jobSetName1",
			Groups:     []string{"group1", "group2"},
			Events: []*armadaevents.EventSequence_Event{
				{Event: &armadaevents.EventSequence_Event_SubmitJob{SubmitJob: &armadaevents.SubmitJob{JobId: "a"}}},
				{Event: &armadaevents.EventSequence_Event_SubmitJob{SubmitJob: &armadaevents.SubmitJob{JobId: "b"}}},
			},
		},
		{
			Queue:      "queue1",
			UserId:     "userId1",
			JobSetName: "jobSetName1",
			Groups:     []string{"group1", "group2"},
			Events: []*armadaevents.EventSequence_Event{
				{Event: &armadaevents.EventSequence_Event_SubmitJob{SubmitJob: &armadaevents.SubmitJob{JobId: "c"}}},
			},
		},
		{
			Queue:      "queue2",
			UserId:     "userId1",
			JobSetName: "jobSetName1",
			Groups:     []string{"group1", "group2"},
			Events: []*armadaevents.EventSequence_Event{
				{Event: &armadaevents.EventSequence_Event_SubmitJob{SubmitJob: &armadaevents.SubmitJob{JobId: "d"}}},
				{Event: &armadaevents.EventSequence_Event_SubmitJob{SubmitJob: &armadaevents.SubmitJob{JobId: "e"}}},
			},
		},
	}

	result := LimitSequencesEventMessageCount(input, 2)
	assert.Len(t, result, 3)
	assert.Equal(t, expected, result)
}

func TestLimitSequenceByteSize(t *testing.T) {
	sequence := &armadaevents.EventSequence{
		Queue:      "queue1",
		UserId:     "userId1",
		JobSetName: "jobSetName1",
		Groups:     []string{"group1", "group2"},
		Events:     nil,
	}

	// 10 events, each of size 10 bytes + a little more.
	// At the time of writing, each event is 14 bytes and the sequence with no event is of size 46 bytes.
	numEvents := 3
	for i := 0; i < numEvents; i++ {
		sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
			Event: &armadaevents.EventSequence_Event_SubmitJob{
				SubmitJob: &armadaevents.SubmitJob{
					DeduplicationId: "1234567890",
				},
			},
		})
	}

	actual, err := LimitSequenceByteSize(sequence, 1000, true)
	require.NoError(t, err)
	testutil.AssertProtoEqual(t, []*armadaevents.EventSequence{sequence}, actual)

	_, err = LimitSequenceByteSize(sequence, 1, true)
	assert.Error(t, err)

	_, err = LimitSequenceByteSize(sequence, 1, false)
	assert.NoError(t, err)

	expected := make([]*armadaevents.EventSequence, numEvents)
	for i := 0; i < numEvents; i++ {
		expected[i] = &armadaevents.EventSequence{
			Queue:      "queue1",
			UserId:     "userId1",
			JobSetName: "jobSetName1",
			Groups:     []string{"group1", "group2"},
			Events: []*armadaevents.EventSequence_Event{
				{
					Event: &armadaevents.EventSequence_Event_SubmitJob{
						SubmitJob: &armadaevents.SubmitJob{
							DeduplicationId: "1234567890",
						},
					},
				},
			},
		}
	}
	actual, err = LimitSequenceByteSize(sequence, 65+sequenceEventListOverheadSizeBytes, true)
	if !assert.NoError(t, err) {
		return
	}
	testutil.AssertProtoEqual(t, expected, actual)
}

func TestLimitSequencesByteSize(t *testing.T) {
	numSequences := 3
	numEvents := 3
	sequences := make([]*armadaevents.EventSequence, 0)
	for i := 0; i < numEvents; i++ {
		sequence := &armadaevents.EventSequence{
			Queue:      "queue1",
			UserId:     "userId1",
			JobSetName: "jobSetName1",
			Groups:     []string{"group1", "group2"},
			Events:     nil,
		}

		// 10 events, each of size 10 bytes + a little more.
		// At the time of writing, each event is 14 bytes and the sequence with no event is of size 46 bytes.
		for i := 0; i < numEvents; i++ {
			sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
				Event: &armadaevents.EventSequence_Event_SubmitJob{
					SubmitJob: &armadaevents.SubmitJob{
						DeduplicationId: "1234567890",
					},
				},
			})
		}

		sequences = append(sequences, sequence)
	}

	actual, err := LimitSequencesByteSize(sequences, 65+sequenceEventListOverheadSizeBytes, true)
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, numSequences*numEvents, len(actual))
}
