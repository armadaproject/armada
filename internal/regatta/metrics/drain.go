package metrics

import (
	"context"
	"time"

	log "github.com/armadaproject/armada/internal/common/logging"
)

// DrainPollTimeout bounds WaitForQueueDrain in case the drain signal never arrives (e.g. Prometheus is not scraping).
const DrainPollTimeout = 10 * time.Minute

// drainPollInterval is how often WaitForQueueDrain re-queries Prometheus while waiting.
const drainPollInterval = 5 * time.Second

// DefaultSettleDelay is how long to wait after the drain before querying, so the scrape covering the drain lands.
const DefaultSettleDelay = 90 * time.Second

// WaitForQueueDrain polls until the queues' combined armada_queue_size (validated) and armada_queue_leased_pod_count
// both read zero: nothing waiting or leased, the nearest proxy for "all jobs finished" without polling job state. It
// gives up after DrainPollTimeout. It returns when the drain was observed (or the timeout), which the caller uses as the
// report window's end.
//
// A missing series counts as zero, but a zero reading only counts once either metric has been seen above zero, else the
// gap before the scheduler's first cycle after a large submission would pass for a drain. Both are checked because jobs
// can move from validated to leased between two 5s polls. Activity since `since` (the run's start) counts, for jobs that
// all finished before this wait began.
func WaitForQueueDrain(ctx context.Context, promURL string, queues []string, since time.Time) time.Time {
	deadline := time.Now().Add(DrainPollTimeout)
	sizeExpr := queueSizeQuery(queues)
	leasedExpr := leasedPodCountQuery(queues)
	// Seeded from the history: if every job finished before the wait began, the polls alone never see activity.
	seenPopulated := activitySince(ctx, promURL, queues, since)
	if seenPopulated {
		log.Infof("queue activity was recorded since the run started; waiting for the queues to read zero")
	}

	for {
		now := time.Now()
		size, sizeErr := query(ctx, promURL, sizeExpr, now)
		leased, leasedErr := query(ctx, promURL, leasedExpr, now)

		if (sizeErr == nil && size != nil && *size > 0) || (leasedErr == nil && leased != nil && *leased > 0) {
			seenPopulated = true
		}

		drained := seenPopulated && sizeErr == nil && leasedErr == nil &&
			isZeroOrAbsent(size) && isZeroOrAbsent(leased)
		if drained {
			log.Infof("%d queue(s) drained (queue_size and leased_pod_count both 0)", len(queues))
			return now
		}

		if now.After(deadline) {
			log.Warnf("timed out after %s waiting for %d queue(s) to drain, proceeding anyway", DrainPollTimeout, len(queues))
			return now
		}

		select {
		case <-ctx.Done():
			return time.Now()
		case <-time.After(drainPollInterval):
		}
	}
}

// activitySince reports whether any queue had jobs waiting or leased since the given time.
func activitySince(ctx context.Context, promURL string, queues []string, since time.Time) bool {
	seconds := time.Since(since).Seconds()
	if seconds < 1 {
		seconds = 1
	}
	window := windowStr(seconds)
	now := time.Now()
	for _, expr := range []string{peakQueueSizeQuery(queues, window, seconds), peakLeasedPodCountQuery(queues, window, seconds)} {
		if peak, err := query(ctx, promURL, expr, now); err == nil && peak != nil && *peak > 0 {
			return true
		}
	}
	return false
}

// isZeroOrAbsent treats a missing series as zero: the series exist only once a queue has had a job.
func isZeroOrAbsent(v *float64) bool {
	return v == nil || *v == 0
}
