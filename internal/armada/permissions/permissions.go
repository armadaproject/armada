package permissions

import (
	"github.com/G-Research/armada/internal/common/auth/permission"
)

const (
	SubmitJobs          permission.Permission = "submit_jobs"
	SubmitAnyJobs                             = "submit_any_jobs"
	CreateQueue                               = "create_queue"
	DeleteQueue                               = "delete_queue"
	CancelJobs                                = "cancel_jobs"
	CancelAnyJobs                             = "cancel_any_jobs"
	ReprioritizeJobs                          = "reprioritize_jobs"
	ReprioritizeAnyJobs                       = "reprioritize_any_jobs"
	WatchAllEvents                            = "watch_all_events"

	ExecuteJobs = "execute_jobs"
)
