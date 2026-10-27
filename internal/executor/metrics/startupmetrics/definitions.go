package startupmetrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/armadaproject/armada/internal/executor/metrics"
)

const (
	queueLabel     = "queue"
	poolLabel      = "pool"
	nodeLabel      = "node"
	stepLabel      = "step"
	containerLabel = "container"
	cachedLabel    = "cached"
	successLabel   = "success"
	fieldLabel     = "field"
)

// Pod metrics
var (
	podStartUpDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    metrics.ArmadaExecutorMetricsPrefix + "pod_start_up_duration_seconds",
			Help:    "Time from the pod being created to its app containers being started",
			Buckets: []float64{1, 5, 10, 20, 30, 60, 120, 180, 300, 600, 900, 1800},
		},
		[]string{queueLabel, poolLabel},
	)

	podStartUpStepDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    metrics.ArmadaExecutorMetricsPrefix + "pod_start_up_step_duration_seconds",
			Help:    "Time spent in one step of pod start up",
			Buckets: []float64{0.5, 1, 5, 10, 20, 30, 60, 120, 180, 300, 600},
		},
		[]string{queueLabel, poolLabel, stepLabel},
	)

	// Real pulls only
	podImagePullDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    metrics.ArmadaExecutorMetricsPrefix + "pod_image_pull_duration_seconds",
			Help:    "Time the kubelet spent pulling one container's image, observed once per container",
			Buckets: []float64{0.5, 1, 5, 10, 20, 30, 60, 120, 180, 300, 600, 900},
		},
		[]string{queueLabel, poolLabel},
	)

	// Increments once per container request
	podImagePullInfoTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: metrics.ArmadaExecutorMetricsPrefix + "pod_image_pull_info_total",
			Help: "Total container image pull requests.",
		},
		[]string{queueLabel, poolLabel, cachedLabel, successLabel},
	)

	// Real pulls only
	podImagePullWaitingDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: metrics.ArmadaExecutorMetricsPrefix + "pod_image_pull_waiting_duration_seconds",
			Help: "Time one container's image pull spent queued behind other pulls before it " +
				"began, observed once per container.",
			Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300},
		},
		[]string{queueLabel, poolLabel},
	)

	// One observation per init container
	podInitContainerRunDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    metrics.ArmadaExecutorMetricsPrefix + "pod_init_container_run_duration_seconds",
			Help:    "Time one init container ran for",
			Buckets: []float64{0.5, 1, 5, 10, 30, 60, 120, 180, 240, 300},
		},
		[]string{queueLabel, poolLabel, containerLabel},
	)

	// Only counts images we could determine size of
	podImagePullSizeBytes = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: metrics.ArmadaExecutorMetricsPrefix + "pod_image_pull_size_bytes",
			Help: "Size of one container's image, observed once per container",
			Buckets: []float64{
				10 << 20,  // 10MiB
				100 << 20, // 100MiB
				500 << 20, // 500MiB
				1 << 30,   // 1GiB
				5 << 30,   // 5GiB
				10 << 30,  // 10GiB
				20 << 30,  // 20GiB
				30 << 30,  // 30GiB
				40 << 30,  // 40GiB
				60 << 30,  // 60GiB
				100 << 30, // 100GiB
			},
		},
		[]string{queueLabel, poolLabel, cachedLabel},
	)
)

// Node metrics
var (
	nodeStartUpDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: metrics.ArmadaExecutorMetricsPrefix + "node_start_up_duration_seconds",
			Help: "Time from a pod being created to its app containers being started, by the node " +
				"it ran on.",
			Buckets: []float64{1, 5, 10, 20, 30, 60, 120, 180, 300, 600, 900, 1800},
		},
		[]string{nodeLabel, poolLabel},
	)

	nodeStartUpStepDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    metrics.ArmadaExecutorMetricsPrefix + "node_start_up_step_duration_seconds",
			Help:    "Time spent in one step of pod start up, by the node the pod ran on.",
			Buckets: []float64{0.5, 1, 5, 10, 20, 30, 60, 120, 180, 300, 600},
		},
		[]string{nodeLabel, poolLabel, stepLabel},
	)

	nodeInitContainerRunDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    metrics.ArmadaExecutorMetricsPrefix + "node_init_container_run_duration_seconds",
			Help:    "Time one init container ran for, by the node it ran on.",
			Buckets: []float64{0.5, 1, 5, 10, 30, 60, 120, 180, 240, 300},
		},
		[]string{nodeLabel, poolLabel, containerLabel},
	)

	nodeImagePullWaitingDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: metrics.ArmadaExecutorMetricsPrefix + "node_image_pull_waiting_duration_seconds",
			Help: "Time one container's image pull spent queued before it began, by the node it " +
				"was pulled on.",
			Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300},
		},
		[]string{nodeLabel, poolLabel},
	)
)

func nodeMetrics() []*prometheus.HistogramVec {
	return []*prometheus.HistogramVec{
		nodeStartUpDurationSeconds,
		nodeStartUpStepDurationSeconds,
		nodeInitContainerRunDurationSeconds,
		nodeImagePullWaitingDurationSeconds,
	}
}

// Meta metrics
var (
	podStartUpInfoCollectedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: metrics.ArmadaExecutorMetricsPrefix + "pod_start_up_info_collected_total",
			Help: "Total start up breakdowns collected, incremented once per pod",
		},
		[]string{poolLabel, successLabel},
	)

	podStartUpInfoCollectionErrorTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: metrics.ArmadaExecutorMetricsPrefix + "pod_start_up_info_field_collection_error_total",
			Help: "Total start up breakdowns missing a given field, incremented at most once per " +
				"pod per field. Container and image pull fields are combined across all of a " +
				"pod's containers.",
		},
		[]string{poolLabel, fieldLabel},
	)
)
