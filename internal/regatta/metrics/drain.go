package metrics

import (
	"context"
	"time"

	log "github.com/armadaproject/armada/internal/common/logging"
)

// DrainPollTimeout bounds WaitForQueueDrain's polling in case the drain signal never arrives (e.g.
// Prometheus isn't scraping the relevant target) - a safety net, not the main configurable knob.
// See config.MetricsConfig.Delay for the fixed post-drain wait before the report is collected.
const DrainPollTimeout = 10 * time.Minute

// drainPollInterval is how often WaitForQueueDrain re-queries Prometheus while waiting.
const drainPollInterval = 5 * time.Second

// DefaultSettleDelay is how long Collect's caller should wait after the queue drains before
// querying Prometheus for the report, so the scrape interval(s) covering the drain moment itself
// actually land before the query time. See config.MetricsConfig.Delay.
const DefaultSettleDelay = 90 * time.Second

// WaitForQueueDrain polls Prometheus until queue's armada_queue_size (validated state) and
// armada_queue_leased_pod_count both read zero, i.e. nothing left waiting and nothing left
// leased - the closest available proxy for "every submitted job has finished" without regatta
// itself polling individual job state. Gives up after DrainPollTimeout and returns regardless (a
// stuck/absent signal should not block metrics collection or fail an otherwise-successful run).
// The returned time.Time is when drain was actually observed (or the timeout moment, if it never
// was) - callers use this as the report window's end, since it reflects when the real jobs
// finished rather than when submission merely returned.
//
// isZeroOrAbsent treats "absent" as drained, which is correct once a queue has genuinely finished
// and its series has expired - but right after a large submission returns, the scheduler may not
// have run its next cycle yet and Prometheus may not have scraped it, so the series can look
// "absent" before the real jobs are ever counted. To avoid mistaking that startup gap for a real
// drain, a zero/absent reading only counts once armada_queue_size has been observed populated
// (i.e. actually > 0) at least once in this poll loop.
func WaitForQueueDrain(ctx context.Context, promURL, queue string) time.Time {
	deadline := time.Now().Add(DrainPollTimeout)
	sizeExpr := queueSizeQuery(queue)
	leasedExpr := leasedPodCountQuery(queue)
	seenPopulated := false

	for {
		now := time.Now()
		size, sizeErr := query(ctx, promURL, sizeExpr, now)
		leased, leasedErr := query(ctx, promURL, leasedExpr, now)

		if sizeErr == nil && size != nil && *size > 0 {
			seenPopulated = true
		}

		drained := seenPopulated && sizeErr == nil && leasedErr == nil &&
			isZeroOrAbsent(size) && isZeroOrAbsent(leased)
		if drained {
			log.Infof("queue %q drained (queue_size and leased_pod_count both 0)", queue)
			return now
		}

		if now.After(deadline) {
			log.Warnf("timed out after %s waiting for queue %q to drain, proceeding anyway", DrainPollTimeout, queue)
			return now
		}

		select {
		case <-ctx.Done():
			return time.Now()
		case <-time.After(drainPollInterval):
		}
	}
}

// isZeroOrAbsent treats a missing series (nil - no jobs ever recorded for this queue/state) the
// same as an explicit zero, since armada_queue_size/leased_pod_count only exist as time series
// once a queue has had at least one job.
func isZeroOrAbsent(v *float64) bool {
	return v == nil || *v == 0
}
