package armadaevents

import (
	"encoding/json"
	"errors"
	time "time"
)

type RawES_Event struct {
	Created *time.Time
	Event   *json.RawMessage `json:"Event"`
}

func (ev *EventSequence_Event) UnmarshalJSON(data []byte) error {
	if string(data) == "null" || string(data) == `""` {
		return nil
	}

	var rawEvent RawES_Event
	err := json.Unmarshal(data, &rawEvent)
	if err != nil {
		return err
	}

	ev.Created = rawEvent.Created

	// The Event member is an interface, so we must repeatedly attempt
	// unmarshaling by going through all the possible underlying struct types.

	var submitJob EventSequence_Event_SubmitJob
	if err = json.Unmarshal(*rawEvent.Event, &submitJob); err == nil {
		ev.Event = &submitJob
		return nil
	}

	var reprioritiseJob EventSequence_Event_ReprioritiseJob
	if err = json.Unmarshal(*rawEvent.Event, &reprioritiseJob); err == nil {
		ev.Event = &reprioritiseJob
		return nil
	}

	var reprioritiseJobSet EventSequence_Event_ReprioritiseJobSet
	if err = json.Unmarshal(*rawEvent.Event, &reprioritiseJobSet); err == nil {
		ev.Event = &reprioritiseJobSet
		return nil
	}

	var reprioritisedJob EventSequence_Event_ReprioritisedJob
	if err = json.Unmarshal(*rawEvent.Event, &reprioritisedJob); err == nil {
		ev.Event = &reprioritisedJob
		return nil
	}

	var cancelJob EventSequence_Event_CancelJob
	if err = json.Unmarshal(*rawEvent.Event, &cancelJob); err == nil {
		ev.Event = &cancelJob
		return nil
	}

	var cancelJobSet EventSequence_Event_CancelJobSet
	if err = json.Unmarshal(*rawEvent.Event, &cancelJobSet); err == nil {
		ev.Event = &cancelJobSet
		return nil
	}

	var cancelledJob EventSequence_Event_CancelledJob
	if err = json.Unmarshal(*rawEvent.Event, &cancelledJob); err == nil {
		ev.Event = &cancelledJob
		return nil
	}

	var jobSucceeded EventSequence_Event_JobSucceeded
	if err = json.Unmarshal(*rawEvent.Event, &jobSucceeded); err == nil {
		ev.Event = &jobSucceeded
		return nil
	}

	var jobErrors EventSequence_Event_JobErrors
	if err = json.Unmarshal(*rawEvent.Event, &jobErrors); err == nil {
		ev.Event = &jobErrors
		return nil
	}

	var jobRunLeased EventSequence_Event_JobRunLeased
	if err = json.Unmarshal(*rawEvent.Event, &jobRunLeased); err == nil {
		ev.Event = &jobRunLeased
		return nil
	}

	var jobRunAssigned EventSequence_Event_JobRunAssigned
	if err = json.Unmarshal(*rawEvent.Event, &jobRunAssigned); err == nil {
		ev.Event = &jobRunAssigned
		return nil
	}

	var jobRunRunning EventSequence_Event_JobRunRunning
	if err = json.Unmarshal(*rawEvent.Event, &jobRunRunning); err == nil {
		ev.Event = &jobRunRunning
		return nil
	}

	var jobRunSucceeded EventSequence_Event_JobRunSucceeded
	if err = json.Unmarshal(*rawEvent.Event, &jobRunSucceeded); err == nil {
		ev.Event = &jobRunSucceeded
		return nil
	}

	var jobRunErrors EventSequence_Event_JobRunErrors
	if err = json.Unmarshal(*rawEvent.Event, &jobRunErrors); err == nil {
		ev.Event = &jobRunErrors
		return nil
	}

	var jobDuplicateDetected EventSequence_Event_JobDuplicateDetected
	if err = json.Unmarshal(*rawEvent.Event, &jobDuplicateDetected); err == nil {
		ev.Event = &jobDuplicateDetected
		return nil
	}

	var standaloneIngressInfo EventSequence_Event_StandaloneIngressInfo
	if err = json.Unmarshal(*rawEvent.Event, &standaloneIngressInfo); err == nil {
		ev.Event = &standaloneIngressInfo
		return nil
	}

	var resourceUtilisation EventSequence_Event_ResourceUtilisation
	if err = json.Unmarshal(*rawEvent.Event, &resourceUtilisation); err == nil {
		ev.Event = &resourceUtilisation
		return nil
	}

	var jobRunPreempted EventSequence_Event_JobRunPreempted
	if err = json.Unmarshal(*rawEvent.Event, &jobRunPreempted); err == nil {
		ev.Event = &jobRunPreempted
		return nil
	}

	return errors.New("could not determine EventSequence_Event.Event type for unmarshaling")
}
