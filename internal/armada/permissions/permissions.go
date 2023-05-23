package permissions

import (
	"github.com/armadaproject/armada/internal/common/auth/permission"
)

// Each principal (e.g., a user) has permissions associated with it.
// These are the possible permissions.
// For each gRPC call, the call handler first checks if the user has permissions for that call.
const (
	SubmitJobs          permission.Permission = "submit_jobs"
	SubmitAnyJobs                             = "submit_any_jobs"
	CreateQueue                               = "create_queue"
	DeleteQueue                               = "delete_queue"
	CancelJobs                                = "cancel_jobs"
	CancelAnyJobs                             = "cancel_any_jobs"
	ReprioritizeJobs                          = "reprioritize_jobs"
	ReprioritizeAnyJobs                       = "reprioritize_any_jobs"
	WatchEvents                               = "watch_events"
	WatchAllEvents                            = "watch_all_events"
	ExecuteJobs                               = "execute_jobs"
	CordonNodes                               = "cordon_nodes"
)
