package metrics

const (

	// common prefix for all metric names
	prefix = "armada_scheduler_"

	// Prometheus Labels
	poolLabel             = "pool"
	queueLabel            = "queue"
	priorityClassLabel    = "priority_class"
	nodeLabel             = "node"
	clusterLabel          = "cluster"
	errorCategoryLabel    = "category"
	errorSubcategoryLabel = "subcategory"
	stateLabel            = "state"
	priorStateLabel       = "priorState"
	resourceLabel         = "resource"

	// Job state strings
	queued    = "queued"
	running   = "running"
	pending   = "pending"
	cancelled = "cancelled"
	leased    = "leased"
	preempted = "preempted"
	failed    = "failed"
	succeeded = "succeeded"
)
