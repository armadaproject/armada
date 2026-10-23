package submit

import "time"

// QueueVisibilityRetries/-Delay work around a known Armada race: a freshly created queue isn't
// always immediately visible to the very next submit call on the same connection.
const (
	QueueVisibilityRetries = 5
	QueueVisibilityDelay   = 1 * time.Second
)
