package permissions

type Permission string

const (
	SubmitJobs     Permission = "submit_jobs"
	CreateQueue               = "create_queue"
	CancelJobs                = "cancel_obs"
	WatchAllEvents            = "watch_all_events"

	ExecuteJobs = "execute_jobs"
)
