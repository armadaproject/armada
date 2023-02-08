from armada_client.gen.event_typings import (
    get_all_job_event_classes,
    get_event_states,
    gen_file,
    get_job_states,
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
    "JobPreemptedEvent",
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
    "preempted",
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
    JobPreemptedEvent,
    JobSucceededEvent,
    JobUtilisationEvent,
    JobReprioritizingEvent,
    JobReprioritizedEvent,
    JobCancellingEvent,
    JobCancelledEvent,
    JobTerminatedEvent,
    JobUpdatedEvent
)
"""

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
    preempted = "preempted"

'''

expected_union_text = """
# Union for the Job Event Types.
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
    JobPreemptedEvent,
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

expected_jobstates_text = '''
class JobState(Enum):
    """
    Enum for the job states.
    Used by cancel_jobset.
    """

    QUEUED = 0
    PENDING = 1
    RUNNING = 2
    SUCCEEDED = 3
    FAILED = 4
    UNKNOWN = 5

'''

expected_jobstates = [
    ("QUEUED", 0),
    ("PENDING", 1),
    ("RUNNING", 2),
    ("SUCCEEDED", 3),
    ("FAILED", 4),
    ("UNKNOWN", 5),
]


def test_event_states():
    assert get_event_states() == expected_event_states


def test_union_var():
    assert get_all_job_event_classes() == expected_events


def test_job_states():
    assert get_job_states() == expected_jobstates


def test_file_gen():
    import_text, states_text, union_text, jobstates_text = gen_file(
        get_event_states(), get_all_job_event_classes(), get_job_states()
    )

    assert import_text == expected_import_text
    assert states_text == expected_states_text
    assert union_text == expected_union_text
    assert jobstates_text == expected_jobstates_text
