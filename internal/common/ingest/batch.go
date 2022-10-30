package ingest

import (
	"time"

	"k8s.io/apimachinery/pkg/util/clock"
)

// Batch batches up events from a channel.  Batches are created whenever maxItems have been
// received or maxTimeout has elapsed since the last batch was created (whichever occurs first).
func Batch[T any](values <-chan T, maxItems int,
	maxTimeout time.Duration, clock clock.Clock,
) chan []T {
	out := make(chan []T)

	go func() {
		defer close(out)
		for keepGoing := true; keepGoing; {
			var batch []T
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
