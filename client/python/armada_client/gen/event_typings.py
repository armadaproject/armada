import sys

from armada_client.armada import event_pb2, submit_pb2


def get_event_states():
    return [
        x for x in event_pb2.EventMessage.DESCRIPTOR.fields_by_name if "__" not in x
    ]


def get_all_job_event_classes():
    job_events = []

    for possible_event in event_pb2.__dict__:
        try:
            if "job_id" in getattr(event_pb2, possible_event).DESCRIPTOR.fields_by_name:
                job_events.append(possible_event)
        except AttributeError:
            continue

    return job_events


def get_job_states():
    """
    submit_pb2.JobState.DESCRIPTOR.value holds a list
    of custom google protobuf "enums".

    `enum.name` is the name of the enum.
    `enum.number` is the value of the enum.
    """
    return [(enum.name, enum.number) for enum in submit_pb2.JobState.DESCRIPTOR.values]


def gen_file(states, classes, jobstates):
    enum_options = "\n".join(f'    {x} = "{x}"' for x in states)
    classes = ",\n".join(f"    {x}" for x in classes)
    job_states = "\n".join(f"    {name} = {value}" for name, value in jobstates)

    import_text = (
        "from enum import Enum\nfrom typing import Union\n\n"
        f"from armada_client.armada.event_pb2 import (\n{classes}\n)\n"
    )

    states_docstring = '\n    """\n' + "    Enum for the event states." + '\n    """\n'
    states_text = (
        f"\n\nclass EventType(Enum):{states_docstring}\n" f"{enum_options}\n\n"
    )

    jobstates_docstring = (
        '\n    """\n'
        + "    Enum for the job states.\n    Used by cancel_jobset."
        + '\n    """\n'
    )
    jobstates_text = (
        f"\nclass JobState(Enum):{jobstates_docstring}\n" f"{job_states}\n\n"
    )

    union_docstring = "\n# Union for the Job Event Types.\n"
    union_text = f"{union_docstring}OneOfJobEvent = Union[\n{classes}\n]\n"

    return import_text, states_text, union_text, jobstates_text


def write_file(import_text, states_text, union_text, jobstates_text, file):
    with open(f"{file}", "w", encoding="utf-8") as f:
        f.write(import_text)
        f.write(states_text)
        f.write(jobstates_text)
        f.write(union_text)


def main():
    states = get_event_states()
    print("Done creating EventStates")

    classes = get_all_job_event_classes()
    print("Done creating JobEvent classes")

    jobstates = get_job_states()
    print("Done creating JobStates")

    import_text, states_text, union_text, jobstates_text = gen_file(
        states, classes, jobstates
    )
    write_file(import_text, states_text, union_text, jobstates_text, typings_file)


if __name__ == "__main__":
    # get path to this files location
    root = f"{sys.path[0]}/../../"
    typings_file = f"{root}/armada_client/typings.py"

    main()
    sys.exit(0)
