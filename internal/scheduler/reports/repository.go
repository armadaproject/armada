package reports

import (
	"sync"
	"sync/atomic"

	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"

	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
)

type CtxPoolPair[T any] struct {
	pool          string
	schedulingCtx T
}

type SchedulingContextRepository struct {
	mostRecentByPool atomic.Pointer[map[string]*schedulercontext.SchedulingContext]
	mu               sync.Mutex
}

func NewSchedulingContextRepository() *SchedulingContextRepository {
	mostRecentByExecutor := make(map[string]*schedulercontext.SchedulingContext)
	rv := &SchedulingContextRepository{}
	rv.mostRecentByPool.Store(&mostRecentByExecutor)
	return rv
}

func (r *SchedulingContextRepository) StoreSchedulingContext(sctx *schedulercontext.SchedulingContext) {
	r.mu.Lock()
	defer r.mu.Unlock()
	byPool := r.mostRecentByPool.Load()
	byPoolCopy := maps.Clone(*byPool)
	byPoolCopy[sctx.Pool] = sctx
	r.mostRecentByPool.Store(&byPoolCopy)
}

func (r *SchedulingContextRepository) QueueSchedulingContext(queue string) []CtxPoolPair[*schedulercontext.QueueSchedulingContext] {
	contextsByPool := *(r.mostRecentByPool.Load())
	ctxs := make([]CtxPoolPair[*schedulercontext.QueueSchedulingContext], 0, len(contextsByPool))
	for _, pool := range sortedKeys(contextsByPool) {
		ctx := CtxPoolPair[*schedulercontext.QueueSchedulingContext]{pool: pool}
		schedulingCtx, ok := contextsByPool[pool].QueueSchedulingContexts[queue]
		if ok {
			ctx.schedulingCtx = schedulingCtx
		}
		ctxs = append(ctxs, ctx)
	}
	return ctxs
}

func (r *SchedulingContextRepository) JobSchedulingContext(jobId string) []CtxPoolPair[*schedulercontext.JobSchedulingContext] {
	contextsByPool := *(r.mostRecentByPool.Load())
	ctxs := make([]CtxPoolPair[*schedulercontext.JobSchedulingContext], 0, len(contextsByPool))
	for _, pool := range sortedKeys(contextsByPool) {
		ctx := CtxPoolPair[*schedulercontext.JobSchedulingContext]{
			pool:          pool,
			schedulingCtx: getSchedulingReportForJob(contextsByPool[pool], jobId),
		}
		ctxs = append(ctxs, ctx)
	}
	return ctxs
}

func (r *SchedulingContextRepository) RoundSchedulingContext() []CtxPoolPair[*schedulercontext.SchedulingContext] {
	contextsByPool := *(r.mostRecentByPool.Load())
	ctxs := make([]CtxPoolPair[*schedulercontext.SchedulingContext], 0, len(contextsByPool))
	for _, pool := range sortedKeys(contextsByPool) {
		ctx := CtxPoolPair[*schedulercontext.SchedulingContext]{
			pool:          pool,
			schedulingCtx: contextsByPool[pool],
		}
		ctxs = append(ctxs, ctx)
	}
	return ctxs
}

func getSchedulingReportForJob(sctx *schedulercontext.SchedulingContext, jobId string) *schedulercontext.JobSchedulingContext {
	for _, qctx := range sctx.QueueSchedulingContexts {
		for _, jctx := range qctx.SuccessfulJobSchedulingContexts {
			if jctx.JobId == jobId {
				return jctx
			}
		}
		for _, jctx := range qctx.UnsuccessfulJobSchedulingContexts {
			if jctx.JobId == jobId {
				return jctx
			}
		}
	}
	return nil
}

func sortedKeys(s map[string]*schedulercontext.SchedulingContext) []string {
	keys := maps.Keys(s)
	slices.Sort(keys)
	return keys
}
