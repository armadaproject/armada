package domain

type JobState string

const (
	Queued  JobState = "Queued"
	Pending JobState = "Pending"
	Running JobState = "Running"
)

func (state JobState) String() string {
	return string(state)
}

var validStates = []JobState{
	Queued,
	Pending,
	Running,
}

func IsValidFilterState(state string) bool {
	for _, validPhase := range validStates {
		if JobState(state) == validPhase {
			return true
		}
	}
	return false
}
