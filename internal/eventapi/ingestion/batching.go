package ingestion

import (
	"time"

	"k8s.io/apimachinery/pkg/util/clock"

	"github.com/G-Research/armada/internal/eventapi/model"
)

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
