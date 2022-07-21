from importlib import import_module

from armada_client.gen.event_typings import (
    get_all_job_event_classes,
    get_event_states,
)

event_module = import_module("armada_client.armada.event_pb2")


def test_event_states():

    expected_event_states = [
        "submitted",
        "queued",
        "duplicate_found",
        "leased",
        "lease_returned",
        "lease_expired",
        "pending",
        "running",
        "unable_to_schedule",
        "failed",
        "succeeded",
        "reprioritized",
        "cancelling",
        "cancelled",
        "terminated",
        "utilisation",
        "ingress_info",
        "reprioritizing",
        "updated",
    ]

    assert get_event_states(event_module) == expected_event_states


def test_union_var():

    expected_events = [
        "JobSubmittedEvent",
        "JobQueuedEvent",
        "JobDuplicateFoundEvent",
        "JobLeasedEvent",
        "JobLeaseReturnedEvent",
        "JobLeaseExpiredEvent",
        "JobPendingEvent",
        "JobRunningEvent",
        "JobIngressInfoEvent",
        "JobUnableToScheduleEvent",
        "JobFailedEvent",
        "JobSucceededEvent",
        "JobUtilisationEvent",
        "JobReprioritizingEvent",
        "JobReprioritizedEvent",
        "JobCancellingEvent",
        "JobCancelledEvent",
        "JobTerminatedEvent",
        "JobUpdatedEvent",
    ]

    assert get_all_job_event_classes(event_module) == expected_events
