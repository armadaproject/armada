package convert

import (
	"fmt"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/common/eventutil"
	"github.com/armadaproject/armada/internal/common/ingest"
	"github.com/armadaproject/armada/internal/common/ingest/metrics"
	"github.com/armadaproject/armada/internal/common/ingest/utils"
	log "github.com/armadaproject/armada/internal/common/logging"
	eventingestermetrics "github.com/armadaproject/armada/internal/eventingester/metrics"
	"github.com/armadaproject/armada/internal/eventingester/model"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

// EventConverter converts event sequences into events that we can store in Redis
type EventConverter struct {
	Compressor          compress.Compressor
	MaxMessageBatchSize uint
	metrics             *metrics.Metrics
}

func NewEventConverter(compressor compress.Compressor, maxMessageBatchSize uint, metrics *metrics.Metrics) ingest.InstructionConverter[*model.BatchUpdate, *armadaevents.EventSequence] {
	return &EventConverter{
		Compressor:          compressor,
		MaxMessageBatchSize: maxMessageBatchSize,
		metrics:             metrics,
	}
}

func (ec *EventConverter) Convert(ctx *armadacontext.Context, eventsWithIds *utils.EventsWithIds[*armadaevents.EventSequence]) *model.BatchUpdate {
	// Remove all groups as they are potentially quite large
	for _, es := range eventsWithIds.Events {
		es.Groups = nil
	}

	sequences := eventutil.CompactEventSequences(eventsWithIds.Events)
	sequences, err := eventutil.LimitSequencesByteSize(sequences, ec.MaxMessageBatchSize, false)
	if err != nil {
		// This should never happen. We pass strict=false to theabove sequence
		panic(errors.WithMessage(err, "Failed to limit sequence by size"))
	}

	// Batch-level aggregators for compression ratio calculation
	var batchUncompressedTotal uint64
	var batchCompressedTotal uint64

	// Queue-scoped aggregators for batch metrics
	uncompressedByQueue := make(map[string]uint64)
	compressedByQueue := make(map[string]uint64)

	// Store per-event metadata for deferred metric recording
	type eventMetadata struct {
		eventType        string
		queue            string
		uncompressedSize uint64
	}
	eventMetadataList := make([]eventMetadata, 0)

	events := make([]*model.Event, 0)
	for _, es := range sequences {
		queue := es.Queue
		jobset := es.JobSetName
		es.JobSetName = ""
		es.Queue = ""

		clearCancellationReason(es)

		sequenceUncompressedTotal := uint64(0)
		for _, event := range es.Events {
			uncompressedSize := uint64(proto.Size(event))
			batchUncompressedTotal += uncompressedSize
			sequenceUncompressedTotal += uncompressedSize

			eventType := fmt.Sprintf("%T", event.GetEvent())
			if eventType == "" {
				eventType = "unknown"
			}

			eventingestermetrics.RecordUncompressedEventSize(eventType, queue, uncompressedSize)

			eventMetadataList = append(eventMetadataList, eventMetadata{
				eventType:        eventType,
				queue:            queue,
				uncompressedSize: uncompressedSize,
			})
		}

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

		sequenceCompressedSize := uint64(len(compressedBytes))
		batchCompressedTotal += sequenceCompressedSize

		uncompressedByQueue[queue] += sequenceUncompressedTotal
		compressedByQueue[queue] += sequenceCompressedSize

		events = append(events, &model.Event{
			Queue:  queue,
			Jobset: jobset,
			Event:  compressedBytes,
		})
	}

	// Compute batch compression ratio with zero guard
	var batchRatio float64
	if batchUncompressedTotal == 0 {
		batchRatio = 0
	} else {
		batchRatio = float64(batchCompressedTotal) / float64(batchUncompressedTotal)
	}

	// Record estimated compressed size for each event using batch ratio
	for _, metadata := range eventMetadataList {
		estimatedCompressed := float64(metadata.uncompressedSize) * batchRatio
		eventingestermetrics.RecordEstimatedCompressedEventSize(metadata.eventType, metadata.queue, estimatedCompressed)
	}

	return &model.BatchUpdate{
		MessageIds:               eventsWithIds.MessageIds,
		Events:                   events,
		UncompressedTotalByQueue: uncompressedByQueue,
		CompressedTotalByQueue:   compressedByQueue,
	}
}

// For each cancel event, remove the cancellation reason
func clearCancellationReason(es *armadaevents.EventSequence) {
	for _, e := range es.Events {
		switch event := e.GetEvent().(type) {
		case *armadaevents.EventSequence_Event_CancelJob:
			event.CancelJob.Reason = ""
		case *armadaevents.EventSequence_Event_CancelJobSet:
			event.CancelJobSet.Reason = ""
		case *armadaevents.EventSequence_Event_CancelledJob:
			event.CancelledJob.Reason = ""
		default:
		}
	}
}
