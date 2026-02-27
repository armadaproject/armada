package metrics

import (
	"fmt"
	"time"

	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/bidstore"
)

type QueueMetricProvider interface {
	GetQueuedJobMetrics(queueName string) []*QueueMetrics
	GetRunningJobMetrics(queueName string) []*QueueMetrics
	GetAllQueues() []*api.Queue
}

type QueueMetrics struct {
	Pool           string
	PriorityClass  string
	AccountingRole string
	Resources      ResourceMetrics
	Durations      *FloatMetrics
	BidPrices      *FloatMetrics
}

type QueueMetricsRecorder struct {
	Pool             string
	PriorityClass    string
	AccountingRole   string
	resourceRecorder *ResourceMetricsRecorder
	durationRecorder *FloatMetricsRecorder
	bidPriceRecorder *FloatMetricsRecorder
}

type JobMetricsRecorder struct {
	recordersByRecorderKey map[string]*QueueMetricsRecorder
}

func NewJobMetricsRecorder() *JobMetricsRecorder {
	return &JobMetricsRecorder{make(map[string]*QueueMetricsRecorder)}
}

func (r *JobMetricsRecorder) RecordBidPrice(pool string, priorityClass string, accountingRole string, price float64) {
	recorder := r.getOrCreateRecorder(pool, priorityClass, accountingRole)
	recorder.bidPriceRecorder.Record(price)
}

func (r *JobMetricsRecorder) RecordJobRuntime(pool string, priorityClass string, accountingRole string, jobRuntime time.Duration) {
	recorder := r.getOrCreateRecorder(pool, priorityClass, accountingRole)
	recorder.durationRecorder.Record(jobRuntime.Seconds())
}

func (r *JobMetricsRecorder) RecordResources(pool string, priorityClass string, accountingRole string, priceBand bidstore.PriceBand, resources armadaresource.ComputeResourcesFloat) {
	recorder := r.getOrCreateRecorder(pool, priorityClass, accountingRole)
	recorder.resourceRecorder.Record(priceBand, resources)
}

func (r *JobMetricsRecorder) Metrics() []*QueueMetrics {
	result := make([]*QueueMetrics, 0, len(r.recordersByRecorderKey))
	for _, v := range r.recordersByRecorderKey {
		result = append(result, &QueueMetrics{
			Pool:           v.Pool,
			PriorityClass:  v.PriorityClass,
			AccountingRole: v.AccountingRole,
			Resources:      v.resourceRecorder.GetMetrics(),
			Durations:      v.durationRecorder.GetMetrics(),
			BidPrices:      v.bidPriceRecorder.GetMetrics(),
		})
	}
	return result
}

func (r *JobMetricsRecorder) getOrCreateRecorder(pool string, priorityClass string, accountingRole string) *QueueMetricsRecorder {
	recorderKey := key(pool, priorityClass, accountingRole)
	qmr, ok := r.recordersByRecorderKey[recorderKey]
	if !ok {
		qmr = &QueueMetricsRecorder{
			Pool:             pool,
			PriorityClass:    priorityClass,
			AccountingRole:   accountingRole,
			resourceRecorder: NewResourceMetricsRecorder(),
			durationRecorder: NewDefaultJobDurationMetricsRecorder(),
			bidPriceRecorder: NewFloatMetricsRecorder(),
		}
		r.recordersByRecorderKey[recorderKey] = qmr
	}
	return qmr
}

func key(pool string, priorityClass string, accountingRole string) string {
	return fmt.Sprintf("%s:%s:%s", pool, priorityClass, accountingRole)
}
