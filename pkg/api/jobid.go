package api

import "time"

func JobIdFromApiEvent(msg *EventMessage) string {
	switch e := msg.Events.(type) {
	case *EventMessage_Submitted:
		return e.Submitted.JobId
	case *EventMessage_Queued:
		return e.Queued.JobId
	case *EventMessage_DuplicateFound:
		return e.DuplicateFound.JobId
	case *EventMessage_Leased:
		return e.Leased.JobId
	case *EventMessage_LeaseReturned:
		return e.LeaseReturned.JobId
	case *EventMessage_LeaseExpired:
		return e.LeaseExpired.JobId
	case *EventMessage_Pending:
		return e.Pending.JobId
	case *EventMessage_Running:
		return e.Running.JobId
	case *EventMessage_UnableToSchedule:
		return e.UnableToSchedule.JobId
	case *EventMessage_Failed:
		return e.Failed.JobId
	case *EventMessage_Succeeded:
		return e.Succeeded.JobId
	case *EventMessage_Reprioritized:
		return e.Reprioritized.JobId
	case *EventMessage_Cancelling:
		return e.Cancelling.JobId
	case *EventMessage_Cancelled:
		return e.Cancelled.JobId
	case *EventMessage_Terminated:
		return e.Terminated.JobId
	case *EventMessage_Utilisation:
		return e.Utilisation.JobId
	case *EventMessage_IngressInfo:
		return e.IngressInfo.JobId
	case *EventMessage_Reprioritizing:
		return e.Reprioritizing.JobId
	case *EventMessage_Updated:
		return e.Updated.JobId
	}
	return ""
}

func CreatedFromApiEvent(msg *EventMessage) time.Time {
	switch e := msg.Events.(type) {
	case *EventMessage_Submitted:
		return e.Submitted.Created
	case *EventMessage_Queued:
		return e.Queued.Created
	case *EventMessage_DuplicateFound:
		return e.DuplicateFound.Created
	case *EventMessage_Leased:
		return e.Leased.Created
	case *EventMessage_LeaseReturned:
		return e.LeaseReturned.Created
	case *EventMessage_LeaseExpired:
		return e.LeaseExpired.Created
	case *EventMessage_Pending:
		return e.Pending.Created
	case *EventMessage_Running:
		return e.Running.Created
	case *EventMessage_UnableToSchedule:
		return e.UnableToSchedule.Created
	case *EventMessage_Failed:
		return e.Failed.Created
	case *EventMessage_Succeeded:
		return e.Succeeded.Created
	case *EventMessage_Reprioritized:
		return e.Reprioritized.Created
	case *EventMessage_Cancelling:
		return e.Cancelling.Created
	case *EventMessage_Cancelled:
		return e.Cancelled.Created
	case *EventMessage_Terminated:
		return e.Terminated.Created
	case *EventMessage_Utilisation:
		return e.Utilisation.Created
	case *EventMessage_IngressInfo:
		return e.IngressInfo.Created
	case *EventMessage_Reprioritizing:
		return e.Reprioritizing.Created
	case *EventMessage_Updated:
		return e.Updated.Created
	}
	return time.Time{}
}
