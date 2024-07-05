package armadaevents

func (ev *EventSequence_Event) GetEventName() string {
	switch ev.GetEvent().(type) {
	case *EventSequence_Event_SubmitJob:
		return "SubmitJob"
	case *EventSequence_Event_JobRunLeased:
		return "JobRunLeased"
	case *EventSequence_Event_JobRunRunning:
		return "JobRunRunning"
	case *EventSequence_Event_JobRunSucceeded:
		return "JobRunSucceeded"
	case *EventSequence_Event_JobRunErrors:
		return "JobRunErrors"
	case *EventSequence_Event_JobSucceeded:
		return "JobSucceeded"
	case *EventSequence_Event_JobErrors:
		return "JobErrors"
	case *EventSequence_Event_JobPreemptionRequested:
		return "JobPreemptionRequested"
	case *EventSequence_Event_JobRunPreemptionRequested:
		return "JobRunPreemptionRequested"
	case *EventSequence_Event_ReprioritiseJob:
		return "ReprioritiseJob"
	case *EventSequence_Event_ReprioritiseJobSet:
		return "ReprioritiseJobSet"
	case *EventSequence_Event_CancelJob:
		return "CancelJob"
	case *EventSequence_Event_CancelJobSet:
		return "CancelJobSet"
	case *EventSequence_Event_CancelledJob:
		return "CancelledJob"
	case *EventSequence_Event_JobRunCancelled:
		return "JobRunCancelled"
	case *EventSequence_Event_JobRequeued:
		return "JobRequeued"
	case *EventSequence_Event_PartitionMarker:
		return "PartitionMarker"
	case *EventSequence_Event_JobRunPreempted:
		return "JobRunPreemped"
	case *EventSequence_Event_JobRunAssigned:
		return "JobRunAssigned"
	case *EventSequence_Event_JobValidated:
		return "JobValidated"
	case *EventSequence_Event_ReprioritisedJob:
		return "ReprioritisedJob"
	case *EventSequence_Event_ResourceUtilisation:
		return "ResourceUtilisation"
	case *EventSequence_Event_StandaloneIngressInfo:
		return "StandloneIngressIngo"
	}
	return "Unknown"
}
