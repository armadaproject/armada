package metrics

import (
	"fmt"
	"time"

	armadaresource "github.com/armadaproject/armada/internal/common/resource"
)

type QueueMetrics struct {
	Pool          string
	PriorityClass string
	Resources     ResourceMetrics
	Durations     *FloatMetrics
}

type QueueMetricsRecorder struct {
	Pool             string
	PriorityClass    string
	resourceRecorder *ResourceMetricsRecorder
	durationRecorder *FloatMetricsRecorder
}

type JobMetricsRecorder struct {
	recordersByPoolAndPriorityClass map[string]*QueueMetricsRecorder
}

func NewJobMetricsRecorder() *JobMetricsRecorder {
	return &JobMetricsRecorder{map[string]*QueueMetricsRecorder{}}
}

func (r *JobMetricsRecorder) RecordJobRuntime(pool string, priorityClass string, jobRuntime time.Duration) {
	recorder := r.getOrCreateRecorder(pool, priorityClass)
	recorder.durationRecorder.Record(jobRuntime.Seconds())
}

func (r *JobMetricsRecorder) RecordResources(pool string, priorityClass string, resources armadaresource.ComputeResourcesFloat) {
	recorder := r.getOrCreateRecorder(pool, priorityClass)
	recorder.resourceRecorder.Record(resources)
}

func (r *JobMetricsRecorder) Metrics() []*QueueMetrics {
	result := make([]*QueueMetrics, 0, len(r.recordersByPoolAndPriorityClass))
	for _, v := range r.recordersByPoolAndPriorityClass {
		result = append(result, &QueueMetrics{
			Pool:          v.Pool,
			PriorityClass: v.PriorityClass,
			Resources:     v.resourceRecorder.GetMetrics(),
			Durations:     v.durationRecorder.GetMetrics(),
		})
	}
	return result
}

func (r *JobMetricsRecorder) getOrCreateRecorder(pool string, pritorityClass string) *QueueMetricsRecorder {
	recorderKey := key(pool, pritorityClass)
	qmr, exists := r.recordersByPoolAndPriorityClass[recorderKey]
	if !exists {
		qmr = &QueueMetricsRecorder{
			Pool:             pool,
			PriorityClass:    pritorityClass,
			resourceRecorder: NewResourceMetricsRecorder(),
			durationRecorder: NewFloatMetricsRecorder(),
		}
		r.recordersByPoolAndPriorityClass[recorderKey] = qmr
	}
	return qmr
}

func key(pool string, priorityClass string) string {
	return fmt.Sprintf("%s:%s", pool, priorityClass)
}
