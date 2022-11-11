package convert

import (
	"context"

	"github.com/pkg/errors"

	"github.com/G-Research/armada/internal/common/ingest/metrics"

	"github.com/G-Research/armada/internal/common/ingest"

	"github.com/gogo/protobuf/proto"
	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/common/compress"
	"github.com/G-Research/armada/internal/common/eventutil"
	"github.com/G-Research/armada/internal/eventingester/model"
)

// EventConverter converts event sequences into events that we can store in Redis
type EventConverter struct {
	Compressor          compress.Compressor
	MaxMessageBatchSize int
	metrics             *metrics.Metrics
}

func NewEventConverter(compressor compress.Compressor, maxMessageBatchSize int, metrics *metrics.Metrics) ingest.InstructionConverter[*model.BatchUpdate] {
	return &EventConverter{
		Compressor:          compressor,
		MaxMessageBatchSize: maxMessageBatchSize,
		metrics:             metrics,
	}
}

func (ec *EventConverter) Convert(ctx context.Context, sequencesWithIds *ingest.EventSequencesWithIds) *model.BatchUpdate {
	// Remove all groups as they are potentially quite large
	for _, es := range sequencesWithIds.EventSequences {
		es.Groups = nil
	}

	sequences := eventutil.CompactEventSequences(sequencesWithIds.EventSequences)
	sequences, err := eventutil.LimitSequencesByteSize(sequences, ec.MaxMessageBatchSize, false)
	if err != nil {
		// This should never happen. We pass strict=false to theabove sequence
		panic(errors.WithMessage(err, "Failed to limit sequence by size"))
	}
	events := make([]*model.Event, 0)
	for _, es := range sequences {
		// Remove the jobset Name and the queue from the proto as this will be stored as the key
		queue := es.Queue
		jobset := es.JobSetName
		es.JobSetName = ""
		es.Queue = ""

		bytes, err := proto.Marshal(es)
		if err != nil {
			ec.metrics.RecordPulsarMessageError(metrics.PulsarMessageErrorProcessing)
			log.WithError(err).Warnf("Could not marshall proto for msg")
			continue
		}
		compressedBytes, err := ec.Compressor.Compress(bytes)
		if err != nil {
			ec.metrics.RecordPulsarMessageError(metrics.PulsarMessageErrorProcessing)
			log.WithError(err).Warnf("Could not compress event")
			continue
		}

		events = append(events, &model.Event{
			Queue:  queue,
			Jobset: jobset,
			Event:  compressedBytes,
		})
	}

	return &model.BatchUpdate{
		MessageIds: sequencesWithIds.MessageIds,
		Events:     events,
	}
}
