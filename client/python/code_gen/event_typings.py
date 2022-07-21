import sys

from armada_client.armada import event_pb2


def get_event_states():
    return [
        x for x in event_pb2.EventMessage.DESCRIPTOR.fields_by_name if "__" not in x
    ]


def get_all_job_event_classes():
    return [x for x in event_pb2.__dict__ if hasattr(getattr(event_pb2, x), "job_id")]


def gen_file(states, classes, file=None):
    enum_options = "\n".join(f'    {x} = "{x}"' for x in states)
    classes = ",\n".join(f"    {x}" for x in classes)

    states_docstring = '\n    """\n' + "    Enum for the event states." + '\n    """\n'

    states_text = (
        f"from enum import Enum\nfrom typing import Union\n\nfrom armada_client.armada.event_pb2 import (\n{classes}\n)"
        f"\n\n\nclass EventType(Enum):{states_docstring}\n"
        f"{enum_options}\n"
    )

    union_docstring = "# Union for the Job Event Types.\n"

    union_text = f"{union_docstring}OneOfJobEvent = Union[\n{classes}\n]\n"

    with open(f"{file}", "w", encoding="utf-8") as f:
        f.write(states_text)
        f.write("\n")
        f.write(union_text)


def main():
    states = get_event_states()
    print("Done creating EventStates")

    classes = get_all_job_event_classes()
    print("Done creating JobEvent classes")

    gen_file(states, classes, typings_file)


if __name__ == "__main__":
    # get path to this files location
    root = f"{sys.path[0]}/../"

    typings_file = f"{root}/armada_client/typings.py"

    main()
    sys.exit(0)
