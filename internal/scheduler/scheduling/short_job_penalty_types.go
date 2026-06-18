package scheduling

import (
	"container/heap"
	"sync"
	"time"

	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
)

// penaltyEntry is one reported terminal short job
type penaltyEntry struct {
	jobId     string
	pool      string
	queue     string
	resources internaltypes.ResourceList
	// deadline is runStart + cutoff[pool], fixed at insert time
	deadline time.Time
}

// ShortJobPenalty owns job penalty state keyed by (pool, queue).
type ShortJobPenalty struct {
	mu      sync.Mutex
	cutoffs map[string]time.Duration
	now     time.Time

	byId   map[string]*penaltyEntry
	expiry *entryHeap
	// Derived cache of the per-(pool,queue) running total
	sums map[string]map[string]internaltypes.ResourceList
}

// ShortJobPenaltySnapshot is an immutable, point-in-time view of the
// per-(pool,queue) penalties
type ShortJobPenaltySnapshot struct {
	sums map[string]map[string]internaltypes.ResourceList
}

func (s *ShortJobPenaltySnapshot) GetPenaltiesForPool(pool string) map[string]internaltypes.ResourceList {
	if s == nil {
		return nil
	}
	return s.sums[pool]
}

// entryHeap is a min-heap of penaltyEntry ordered by deadline.
type entryHeap []*penaltyEntry

func (h entryHeap) Len() int           { return len(h) }
func (h entryHeap) Less(i, j int) bool { return h[i].deadline.Before(h[j].deadline) }
func (h entryHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *entryHeap) Push(x any) {
	*h = append(*h, x.(*penaltyEntry))
}

func (h *entryHeap) Pop() any {
	old := *h
	n := len(old)
	e := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return e
}

func (h entryHeap) peek() *penaltyEntry {
	return h[0]
}

var _ heap.Interface = (*entryHeap)(nil)
