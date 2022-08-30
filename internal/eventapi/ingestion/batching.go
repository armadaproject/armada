package ingestion

import (
	"time"

	"k8s.io/apimachinery/pkg/util/clock"

	"github.com/G-Research/armada/internal/eventapi/model"
)

// Batch batches up events from a channel.  Batches are created whenever maxItems InstructionSets have been
// received or maxTimeout has elapsed since the last batch was created (whichever occurs first).
// This function has a lot in common with lookoutingester.batch.  Hopefully when generics become available we can
// factor out most of the common code
func Batch(values <-chan *model.PulsarEventRow, maxItems int, maxTimeout time.Duration, bufferSize int, clock clock.Clock) chan []*model.PulsarEventRow {
	out := make(chan []*model.PulsarEventRow, bufferSize)

	go func() {
		defer close(out)

		for keepGoing := true; keepGoing; {
			var batch []*model.PulsarEventRow
			expire := clock.After(maxTimeout)
			for {
				select {
				case value, ok := <-values:
					if !ok {
						keepGoing = false
						goto done
					}

					batch = append(batch, value)
					if len(batch) == maxItems {
						goto done
					}

				case <-expire:
					goto done
				}
			}

		done:
			if len(batch) > 0 {
				out <- batch
			}
		}
	}()
	return out
}
