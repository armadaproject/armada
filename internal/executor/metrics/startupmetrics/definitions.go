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

	podStartUpWithoutUntrackedInitContainersDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: metrics.ArmadaExecutorMetricsPrefix + "pod_start_up_without_untracked_init_containers_duration_seconds",
			Help: "Time from the pod being created to its app containers being started, without the " +
				"time spent running init containers that are not tracked",
			Buckets: []float64{1, 5, 10, 20, 30, 60, 120, 180, 300, 600, 900, 1800},
		},
		[]string{queueLabel, poolLabel},
	)

	podStartUpWithoutInitContainersDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: metrics.ArmadaExecutorMetricsPrefix + "pod_start_up_without_init_containers_duration_seconds",
			Help: "Time from the pod being created to its app containers being started, without the " +
				"time spent running any init container",
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

	podTotalImagePullDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    metrics.ArmadaExecutorMetricsPrefix + "pod_total_image_pull_duration_seconds",
			Help:    "Time the kubelet spent pulling images for across all containers of a pod. Observed once per pod.",
			Buckets: []float64{0.5, 1, 5, 10, 20, 30, 60, 120, 180, 300, 600, 900, 1800},
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

	nodeStartUpWithoutUntrackedInitContainersDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: metrics.ArmadaExecutorMetricsPrefix + "node_start_up_without_untracked_init_containers_duration_seconds",
			Help: "Time from a pod being created to its app containers being started, without the " +
				"time spent running init containers that are not tracked, by the node it ran on.",
			Buckets: []float64{1, 5, 10, 20, 30, 60, 120, 180, 300, 600, 900, 1800},
		},
		[]string{nodeLabel, poolLabel},
	)

	nodeStartUpWithoutInitContainersDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: metrics.ArmadaExecutorMetricsPrefix + "node_start_up_without_init_containers_duration_seconds",
			Help: "Time from a pod being created to its app containers being started, without the " +
				"time spent running any init container, by the node it ran on.",
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

// Pool metrics
var (
	poolStartUpDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: metrics.ArmadaExecutorMetricsPrefix + "pool_start_up_duration_seconds",
			Help: "Time from a pod being created to its app containers being started, by pool",
			Buckets: []float64{
				0.25, 0.5, 0.75, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 17.5, 20, 22.5,
				25, 27.5, 30, 35, 40, 45, 50, 60, 75, 90, 105, 120, 150, 180, 240, 300, 360, 420,
				480, 600, 900, 1200, 1800, 2700, 3600,
			},
		},
		[]string{poolLabel},
	)

	poolStartUpWithoutUntrackedInitContainersDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: metrics.ArmadaExecutorMetricsPrefix + "pool_start_up_without_untracked_init_containers_duration_seconds",
			Help: "Time from a pod being created to its app containers being started, without the " +
				"time spent running init containers that are not tracked, by pool",
			Buckets: []float64{
				0.25, 0.5, 0.75, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 17.5, 20, 22.5,
				25, 27.5, 30, 35, 40, 45, 50, 60, 75, 90, 105, 120, 150, 180, 240, 300, 360, 420,
				480, 600, 900, 1200, 1800, 2700, 3600,
			},
		},
		[]string{poolLabel},
	)

	poolStartUpWithoutInitContainersDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: metrics.ArmadaExecutorMetricsPrefix + "pool_start_up_without_init_containers_duration_seconds",
			Help: "Time from a pod being created to its app containers being started, without the " +
				"time spent running any init container, by pool",
			Buckets: []float64{
				0.25, 0.5, 0.75, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 17.5, 20, 22.5,
				25, 27.5, 30, 35, 40, 45, 50, 60, 75, 90, 105, 120, 150, 180, 240, 300, 360, 420,
				480, 600, 900, 1200, 1800, 2700, 3600,
			},
		},
		[]string{poolLabel},
	)

	poolStartUpStepDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: metrics.ArmadaExecutorMetricsPrefix + "pool_start_up_step_duration_seconds",
			Help: "Time spent in one step of pod start up, by pool",
			Buckets: []float64{
				0.01, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1, 1.25, 1.5, 1.75, 2,
				2.5, 3, 3.5, 4, 4.5, 5, 6, 7, 8, 9, 10, 12, 14, 16, 18, 20, 22.5, 25, 27.5, 30, 40,
				50, 60, 75, 90, 120, 180, 240, 300, 360, 420, 480, 600, 900, 1800,
			},
		},
		[]string{poolLabel, stepLabel},
	)

	// Real pulls only
	poolImagePullDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: metrics.ArmadaExecutorMetricsPrefix + "pool_image_pull_duration_seconds",
			Help: "Time the kubelet spent pulling one container's image, observed once per container, by pool",
			Buckets: []float64{
				0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.75, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 17.5, 20, 22.5,
				25, 27.5, 30, 35, 40, 45, 50, 60, 75, 90, 120, 180, 240, 300, 360, 420, 480, 600, 900, 1800,
			},
		},
		[]string{poolLabel},
	)

	poolTotalImagePullDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: metrics.ArmadaExecutorMetricsPrefix + "pool_total_image_pull_duration_seconds",
			Help: "Time the kubelet spent pulling images for across all containers of a pod. Observed " +
				"once per pod, by pool.",
			Buckets: []float64{
				0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.75, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 17.5, 20, 22.5,
				25, 27.5, 30, 35, 40, 45, 50, 60, 75, 90, 120, 180, 240, 300, 360, 420, 480, 600, 900, 1800, 2700, 3600,
			},
		},
		[]string{poolLabel},
	)

	// Increments once per container request
	poolImagePullInfoTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: metrics.ArmadaExecutorMetricsPrefix + "pool_image_pull_info_total",
			Help: "Total container image pull requests, by pool.",
		},
		[]string{poolLabel, cachedLabel, successLabel},
	)

	// Real pulls only
	poolImagePullWaitingDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: metrics.ArmadaExecutorMetricsPrefix + "pool_image_pull_waiting_duration_seconds",
			Help: "Time one container's image pull spent queued behind other pulls before it " +
				"began, observed once per container, by pool.",
			Buckets: []float64{
				0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120, 300,
			},
		},
		[]string{poolLabel},
	)

	// One observation per init container
	poolInitContainerRunDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: metrics.ArmadaExecutorMetricsPrefix + "pool_init_container_run_duration_seconds",
			Help: "Time one init container ran for, by pool",
			Buckets: []float64{
				0.1, 0.25, 0.5, 0.75, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 15, 20, 30, 45, 60, 90,
				120, 180, 240, 300, 450, 600, 900, 1800,
			},
		},
		[]string{poolLabel, containerLabel},
	)

	// Only counts images we could determine size of
	poolImagePullSizeBytes = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: metrics.ArmadaExecutorMetricsPrefix + "pool_image_pull_size_bytes",
			Help: "Size of one container's image, observed once per container, by pool",
			Buckets: []float64{
				1 << 20,     // 1MiB
				10 << 20,    // 10MiB
				25 << 20,    // 25MiB
				50 << 20,    // 50MiB
				100 << 20,   // 100MiB
				150 << 20,   // 150MiB
				200 << 20,   // 200MiB
				300 << 20,   // 300MiB
				400 << 20,   // 400MiB
				500 << 20,   // 500MiB
				750 << 20,   // 750MiB
				1 << 30,     // 1GiB
				1536 << 20,  // 1.5GiB
				2 << 30,     // 2GiB
				2560 << 20,  // 2.5GiB
				3 << 30,     // 3GiB
				4 << 30,     // 4GiB
				5 << 30,     // 5GiB
				6 << 30,     // 6GiB
				7 << 30,     // 7GiB
				8 << 30,     // 8GiB
				9 << 30,     // 9GiB
				10 << 30,    // 10GiB
				12800 << 20, // 12.5GiB
				15 << 30,    // 15GiB
				20 << 30,    // 20GiB
				25 << 30,    // 25GiB
				30 << 30,    // 30GiB
				35 << 30,    // 35GiB
				40 << 30,    // 40GiB
				45 << 30,    // 45GiB
				50 << 30,    // 50GiB
				60 << 30,    // 60GiB
				70 << 30,    // 70GiB
				80 << 30,    // 80GiB
				90 << 30,    // 90GiB
				100 << 30,   // 100GiB
			},
		},
		[]string{poolLabel, cachedLabel},
	)
)

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
