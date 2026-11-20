package metrics

const (
	ArmadaSchedulerMetricsPrefix = "armada_scheduler_"

	// Prometheus Labels
	checkpointLabel          = "checkpoint_interval"
	poolLabel                = "pool"
	typeLabel                = "type"
	priorityLabel            = "priority"
	queueLabel               = "queue"
	priorityClassLabel       = "priority_class"
	nodeLabel                = "node"
	nodeTypeLabel            = "nodeType"
	nodeCategoryLabel        = "category"
	clusterLabel             = "cluster"
	errorCategoryLabel       = "category"
	errorSubcategoryLabel    = "subcategory"
	stateLabel               = "state"
	priorStateLabel          = "priorState"
	resourceLabel            = "resource"
	reservationLabel         = "reservation"
	schedulableLabel         = "schedulable"
	overAllocatedLabel       = "overAllocated"
	physicalPoolLabel        = "physical_pool"
	capacityClassLabel       = "capacity_class"
	scalableUnitLabel        = "scalable_unit"
	jobShapeLabel            = "job_shape"
	unschedulableReasonLabel = "unschedulable_reason"
	outcomeLabel             = "outcome"
	stageLabel               = "stage"
	terminationReasonLabel   = "termination_reason"
	retryPolicyLabel         = "policy"
	retryDecisionLabel       = "decision"

	SchedulingOutcomeSuccess = "success"
	SchedulingOutcomeFailure = "failure"

	// Pool scheduling stage label values
	setupStage      = "setup"
	processingStage = "processing"

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
