from armada_client.gen.event_typings import (
    get_all_job_event_classes,
    get_event_states,
    gen_file,
)

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
    "failedCompressed",
]

expected_import_text = """from enum import Enum
from typing import Union

from armada_client.armada.event_pb2 import (
    JobSubmittedEvent,
    JobQueuedEvent,
    JobDuplicateFoundEvent,
    JobLeasedEvent,
    JobLeaseReturnedEvent,
    JobLeaseExpiredEvent,
    JobPendingEvent,
    JobRunningEvent,
    JobIngressInfoEvent,
    JobUnableToScheduleEvent,
    JobFailedEvent,
    JobSucceededEvent,
    JobUtilisationEvent,
    JobReprioritizingEvent,
    JobReprioritizedEvent,
    JobCancellingEvent,
    JobCancelledEvent,
    JobTerminatedEvent,
    JobUpdatedEvent
)"""

expected_states_text = '''


class EventType(Enum):
    """
    Enum for the event states.
    """

    submitted = "submitted"
    queued = "queued"
    duplicate_found = "duplicate_found"
    leased = "leased"
    lease_returned = "lease_returned"
    lease_expired = "lease_expired"
    pending = "pending"
    running = "running"
    unable_to_schedule = "unable_to_schedule"
    failed = "failed"
    succeeded = "succeeded"
    reprioritized = "reprioritized"
    cancelling = "cancelling"
    cancelled = "cancelled"
    terminated = "terminated"
    utilisation = "utilisation"
    ingress_info = "ingress_info"
    reprioritizing = "reprioritizing"
    updated = "updated"
    failedCompressed = "failedCompressed"

'''

expected_union_text = """# Union for the Job Event Types.
OneOfJobEvent = Union[
    JobSubmittedEvent,
    JobQueuedEvent,
    JobDuplicateFoundEvent,
    JobLeasedEvent,
    JobLeaseReturnedEvent,
    JobLeaseExpiredEvent,
    JobPendingEvent,
    JobRunningEvent,
    JobIngressInfoEvent,
    JobUnableToScheduleEvent,
    JobFailedEvent,
    JobSucceededEvent,
    JobUtilisationEvent,
    JobReprioritizingEvent,
    JobReprioritizedEvent,
    JobCancellingEvent,
    JobCancelledEvent,
    JobTerminatedEvent,
    JobUpdatedEvent
]
"""


def test_event_states():

    assert get_event_states() == expected_event_states


def test_union_var():

    assert get_all_job_event_classes() == expected_events


def test_file_gen():

    import_text, states_text, union_text = gen_file(
        get_event_states(), get_all_job_event_classes()
    )

    assert import_text == expected_import_text
    assert states_text == expected_states_text
    assert union_text == expected_union_text
