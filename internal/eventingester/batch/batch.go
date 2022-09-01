package batch

import (
	"time"

	"github.com/G-Research/armada/internal/pulsarutils"

	"k8s.io/apimachinery/pkg/util/clock"
)

// Batch batches up events from a channel.  Batches are created whenever maxItems InstructionSets have been
// received or maxTimeout has elapsed since the last batch was created (whichever occurs first).
// This function has a lot in common with lookoutingester.batch.  Hopefully when generics become available we can
// factor out most of the common code
func Batch(values <-chan *pulsarutils.ConsumerMessage, maxItems int,
	maxTimeout time.Duration, bufferSize int, clock clock.Clock,
) chan []*pulsarutils.ConsumerMessage {
	out := make(chan []*pulsarutils.ConsumerMessage, bufferSize)

	go func() {
		defer close(out)

		for keepGoing := true; keepGoing; {
			var batch []*pulsarutils.ConsumerMessage
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
