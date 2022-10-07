package armadaevents

import (
	"encoding/json"
	"errors"
	time "time"
)

type rawES_Event struct {
	Created    *time.Time
	EventBytes *json.RawMessage `json:"Event"`
}

func (ev *EventSequence_Event) UnmarshalJSON(data []byte) error {
	if string(data) == "null" || string(data) == `""` {
		return nil
	}

	var rawEvent rawES_Event
	err := json.Unmarshal(data, &rawEvent)
	if err != nil {
		return err
	}

	ev.Created = rawEvent.Created

	// Unmarshal into a map, which is temporarily used only to get the marshaled
	// key that signifies the actual struct underlying this instance of the
	// isEventSequence_Event_Event interface.
	var mapEvent map[string]interface{}
	if err = json.Unmarshal(*rawEvent.EventBytes, &mapEvent); err != nil {
		return err
	}

	for k := range mapEvent {
		switch k {
		case "submitJob":
			var submitJob EventSequence_Event_SubmitJob
			if err = json.Unmarshal(*rawEvent.EventBytes, &submitJob); err != nil {
				return err
			}
			ev.Event = &submitJob
		case "reprioritiseJob":
			var reprioritiseJob EventSequence_Event_ReprioritiseJob
			if err = json.Unmarshal(*rawEvent.EventBytes, &reprioritiseJob); err != nil {
				return err
			}
			ev.Event = &reprioritiseJob
		case "reprioritiseJobSet":
			var reprioritiseJobSet EventSequence_Event_ReprioritiseJobSet
			if err = json.Unmarshal(*rawEvent.EventBytes, &reprioritiseJobSet); err != nil {
				return err
			}
			ev.Event = &reprioritiseJobSet
		case "reprioritisedJob":
			var reprioritisedJob EventSequence_Event_ReprioritisedJob
			if err = json.Unmarshal(*rawEvent.EventBytes, &reprioritisedJob); err != nil {
				return err
			}
			ev.Event = &reprioritisedJob
		case "cancelJob":
			var cancelJob EventSequence_Event_CancelJob
			if err = json.Unmarshal(*rawEvent.EventBytes, &cancelJob); err != nil {
				return err
			}
			ev.Event = &cancelJob
		case "cancelJobSet":
			var cancelJobSet EventSequence_Event_CancelJobSet
			if err = json.Unmarshal(*rawEvent.EventBytes, &cancelJobSet); err != nil {
				return err
			}
			ev.Event = &cancelJobSet
		case "cancelledJob":
			var cancelledJob EventSequence_Event_CancelledJob
			if err = json.Unmarshal(*rawEvent.EventBytes, &cancelledJob); err != nil {
				return err
			}
			ev.Event = &cancelledJob
		case "jobSucceeded":
			var jobSucceeded EventSequence_Event_JobSucceeded
			if err = json.Unmarshal(*rawEvent.EventBytes, &jobSucceeded); err != nil {
				return err
			}
			ev.Event = &jobSucceeded
		case "jobErrors":
			var jobErrors EventSequence_Event_JobErrors
			if err = json.Unmarshal(*rawEvent.EventBytes, &jobErrors); err != nil {
				return err
			}
			ev.Event = &jobErrors
		case "jobRunLeased":
			var jobRunLeased EventSequence_Event_JobRunLeased
			if err = json.Unmarshal(*rawEvent.EventBytes, &jobRunLeased); err != nil {
				return err
			}
			ev.Event = &jobRunLeased
		case "jobRunAssigned":
			var jobRunAssigned EventSequence_Event_JobRunAssigned
			if err = json.Unmarshal(*rawEvent.EventBytes, &jobRunAssigned); err != nil {
				return err
			}
			ev.Event = &jobRunAssigned
		case "jobRunRunning":
			var jobRunRunning EventSequence_Event_JobRunRunning
			if err = json.Unmarshal(*rawEvent.EventBytes, &jobRunRunning); err != nil {
				return err
			}
			ev.Event = &jobRunRunning
		case "jobRunSucceeded":
			var jobRunSucceeded EventSequence_Event_JobRunSucceeded
			if err = json.Unmarshal(*rawEvent.EventBytes, &jobRunSucceeded); err != nil {
				return err
			}
			ev.Event = &jobRunSucceeded
		case "jobRunErrors":
			var jobRunErrors EventSequence_Event_JobRunErrors
			if err = json.Unmarshal(*rawEvent.EventBytes, &jobRunErrors); err != nil {
				return err
			}
			ev.Event = &jobRunErrors
		case "jobDuplicateDetected":
			var jobDuplicateDetected EventSequence_Event_JobDuplicateDetected
			if err = json.Unmarshal(*rawEvent.EventBytes, &jobDuplicateDetected); err != nil {
				return err
			}
			ev.Event = &jobDuplicateDetected
		case "standaloneIngressInfo":
			var standaloneIngressInfo EventSequence_Event_StandaloneIngressInfo
			if err = json.Unmarshal(*rawEvent.EventBytes, &standaloneIngressInfo); err != nil {
				return err
			}
			ev.Event = &standaloneIngressInfo
		case "resourceUtilisation":
			var resourceUtilisation EventSequence_Event_ResourceUtilisation
			if err = json.Unmarshal(*rawEvent.EventBytes, &resourceUtilisation); err != nil {
				return err
			}
			ev.Event = &resourceUtilisation
		case "jobRunPreempted":
			var jobRunPreempted EventSequence_Event_JobRunPreempted
			if err = json.Unmarshal(*rawEvent.EventBytes, &jobRunPreempted); err != nil {
				return err
			}
			ev.Event = &jobRunPreempted
		default:
			return errors.New("could not determine EventSequence_Event.Event type for unmarshaling")
		}
	}

	return nil
}
