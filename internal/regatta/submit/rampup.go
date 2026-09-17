package submit

import (
	"context"
	"fmt"
	"time"

	log "github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/pkg/client"
)

// runRampUp spreads spec's total job count across spec.RampUp.RampDuration, submitting a batch
// every spec.RampUp.StepInterval (the final step absorbs any remainder), exhausting each
// JobItem's quota in list order before moving to the next. It then blocks until every submitted
// job reaches a terminal state, or ctx is cancelled - unlike one-shot's "poll only the last job",
// ramp-up must poll every submitted ID, since an early step's job can easily outlive a
// later step's.
func runRampUp(ctx context.Context, apiConnectionDetails *client.ApiConnectionDetails, spec *Spec) error {
	total := 0
	for _, job := range spec.Jobs {
		total += job.Count
	}
	if total == 0 {
		return nil
	}

	steps := int(spec.RampUp.RampDuration / spec.RampUp.StepInterval)
	if steps < 1 {
		steps = 1
	}

	remaining := flattenJobItems(spec.Jobs)
	perStep := total / steps

	var allJobIds []string
	submitted := 0
	for step := 1; step <= steps; step++ {
		count := perStep
		if step == steps {
			count = total - submitted
		}

		batch := take(remaining, count)
		remaining = remaining[len(batch):]

		jobIds, err := submitItems(apiConnectionDetails, spec.Queue, spec.JobSetId, spec.Namespace, batch)
		if err != nil {
			return fmt.Errorf("ramp-up step %d/%d: submitting jobs: %w", step, steps, err)
		}
		allJobIds = append(allJobIds, jobIds...)
		submitted += len(jobIds)
		log.Infof("ramp-up step %d/%d: submitted %d jobs (%d total so far)", step, steps, len(jobIds), submitted)

		if step == steps {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(spec.RampUp.StepInterval):
		}
	}

	if len(allJobIds) == 0 {
		return nil
	}
	return waitForTerminal(ctx, apiConnectionDetails, allJobIds)
}

// flattenJobItems expands each JobItem's Count into that many single-count JobItems, so take can
// slice across job-spec boundaries one job at a time.
func flattenJobItems(jobs []JobItem) []JobItem {
	var flat []JobItem
	for _, job := range jobs {
		for i := 0; i < job.Count; i++ {
			flat = append(flat, JobItem{Spec: job.Spec, Count: 1})
		}
	}
	return flat
}

// take returns up to n items from the front of flat, coalesced back into JobItems with Count>1
// where the same Spec repeats consecutively (keeps chunking/logging readable without changing
// submission order).
func take(flat []JobItem, n int) []JobItem {
	if n > len(flat) {
		n = len(flat)
	}
	var out []JobItem
	for _, item := range flat[:n] {
		if len(out) > 0 && out[len(out)-1].Spec == item.Spec {
			out[len(out)-1].Count++
			continue
		}
		out = append(out, JobItem{Spec: item.Spec, Count: 1})
	}
	return out
}
