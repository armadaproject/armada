package metrics

const (

	// common prefix for all metric names
	prefix = "armada_scheduler_"

	// Prometheus Labels
	checkpointLabel          = "checkpoint_interval"
	poolLabel                = "pool"
	typeLabel                = "type"
	priorityLabel            = "priority"
	queueLabel               = "queue"
	priorityClassLabel       = "priority_class"
	nodeLabel                = "node"
	nodeTypeLabel            = "nodeType"
	nodeTypeLabelSnake       = "node_type"
	clusterLabel             = "cluster"
	errorCategoryLabel       = "category"
	errorSubcategoryLabel    = "subcategory"
	stateLabel               = "state"
	priorStateLabel          = "priorState"
	priorStateLabelSnake     = "prior_state"
	resourceLabel            = "resource"
	reservationLabel         = "reservation"
	schedulableLabel         = "schedulable"
	overAllocatedLabel       = "overAllocated"
	overAllocatedLabelSnake  = "over_allocated"
	physicalPoolLabel        = "physical_pool"
	capacityClassLabel       = "capacity_class"
	jobShapeLabel            = "job_shape"
	unschedulableReasonLabel = "unschedulable_reason"
	outcomeLabel             = "outcome"
	terminationReasonLabel   = "termination_reason"

	SchedulingOutcomeSuccess = "success"
	SchedulingOutcomeFailure = "failure"

	// Job state strings
	queued    = "queued"
	running   = "running"
	pending   = "pending"
	cancelled = "cancelled"
	leased    = "leased"
	preempted = "preempted"
	failed    = "failed"
	succeeded = "succeeded"

	noCheckpointLabelValue = "none"

	CapacityClassDedicated = "dedicated"
	CapacityClassShared    = "shared"
)
