package permissions

type Permission string

const (
	SubmitJobs     Permission = "submit_jobs"
	SubmitAnyJobs             = "submit_any_jobs"
	CreateQueue               = "create_queue"
	CancelJobs                = "cancel_jobs"
	CancelAnyJobs             = "cancel_any_jobs"
	WatchAllEvents            = "watch_all_events"

	ExecuteJobs = "execute_jobs"
)
