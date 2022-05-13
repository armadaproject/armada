package serving

import (
	"context"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gogo/protobuf/proto"
	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/eventapi/eventdb"
	"github.com/G-Research/armada/pkg/armadaevents"
)

// SequenceManager is responsible for storing the latest available Sequence number for each jobset
type SequenceManager interface {
	Get(jobsetId int64) (int64, bool)
	Update(newOffsets map[int64]int64)
}

type DefaultSequenceManager struct {
	sequences map[int64]int64
}

// NewUpdatingSequenceManager returns a SequenceManager that is initialised from the eventDb and then receives
// updates from pulsar
func NewUpdatingSequenceManager(ctx context.Context, eventDb *eventdb.EventDb, pulsarClient pulsar.Client, updateTopic string) (*DefaultSequenceManager, error) {
	// Snapshot the time before we fetch from the db.  This allows us to later subscribe to puslarmessages from before
	// our db fetch time
	startTime := time.Now()

	// Load the latest sequence from the DB
	// TODO: this could become expensive if we have a very large number of jobsets
	// Could move to a caching model
	initialSequenceRows, err := eventDb.LoadSeqNos(ctx)
	if err != nil {
		return nil, err
	}
	initialSequences := make(map[int64]int64, len(initialSequenceRows))
	for _, seq := range initialSequenceRows {
		initialSequences[seq.JobSetId] = seq.JobSetId
	}
	sm := &DefaultSequenceManager{
		sequences: initialSequences,
	}

	// subscribe to pulsar updates from a time before we did a database fetch
	reader, err := pulsarClient.CreateReader(pulsar.ReaderOptions{
		Topic: updateTopic,
		Name:  "armada-sequence-manager",
	})
	if err != nil {
		return nil, err
	}
	// Note that this means tha the updates topic cannot be partitioned
	err = reader.SeekByTime(startTime.Add(-10 * time.Minute))
	if err != nil {
		return nil, err
	}

	go func() {
		for reader.HasNext() {
			msg, err := reader.Next(context.Background())
			if err != nil {
				log.Fatal(err)
			}
			seqUpdate := &armadaevents.SeqUpdates{}
			err = proto.Unmarshal(msg.Payload(), seqUpdate)
			if err != nil {
				log.Error(err)
			}
			seqByJobset := make(map[int64]int64, len(seqUpdate.Updates))
			for _, seq := range seqUpdate.Updates {
				seqByJobset[seq.JobsetId] = seq.SeqNo
				sm.Update(seqByJobset)
			}
		}
	}()

	// TODO: are there some edge cases where we might not get an update on pulsar?
	// We could add a belt-and-braces poll ont he database here.
	return sm, nil
}

// NewStaticSequenceManager returns a SequenceManager that only updates if `Update` is called.
// This is mainly usefuly for test purposes
func NewStaticSequenceManager(initialSequences map[int64]int64) *DefaultSequenceManager {
	om := &DefaultSequenceManager{
		sequences: initialSequences,
	}
	return om
}

// Get Retrieves the latets sequence for the given jobset.  The boolean returned will
// be true if an offset exists and false otherwise
func (sm *DefaultSequenceManager) Get(jobsetId int64) (int64, bool) {
	existingOffset, ok := sm.sequences[jobsetId]
	return existingOffset, ok
}

// Update updates the sequences for the supplied jobsets.
// Any sequences in the update which are lower than the sequences we already store will be ignored.
func (sm *DefaultSequenceManager) Update(newSequences map[int64]int64) {
	for k, v := range newSequences {
		existingSequence, ok := sm.sequences[k]
		if !ok || v > existingSequence {
			sm.sequences[k] = v
		}
	}
}
