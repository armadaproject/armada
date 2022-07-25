import sys

from armada_client.armada import event_pb2


def get_event_states():
    return [
        x for x in event_pb2.EventMessage.DESCRIPTOR.fields_by_name if "__" not in x
    ]


def get_all_job_event_classes():
    return [x for x in event_pb2.__dict__ if hasattr(getattr(event_pb2, x), "job_id")]


def gen_file(states, classes):
    enum_options = "\n".join(f'    {x} = "{x}"' for x in states)
    classes = ",\n".join(f"    {x}" for x in classes)

    states_docstring = '\n    """\n' + "    Enum for the event states." + '\n    """\n'

    import_text = (
        "from enum import Enum\nfrom typing import Union\n\n"
        f"from armada_client.armada.event_pb2 import (\n{classes}\n)"
    )

    states_text = (
        f"\n\n\nclass EventType(Enum):{states_docstring}\n" f"{enum_options}\n\n"
    )

    union_docstring = "# Union for the Job Event Types.\n"

    union_text = f"{union_docstring}OneOfJobEvent = Union[\n{classes}\n]\n"

    return import_text, states_text, union_text


def write_file(import_text, states_text, union_text, file):
    with open(f"{file}", "w", encoding="utf-8") as f:
        f.write(import_text)
        f.write(states_text)
        f.write(union_text)


def main():
    states = get_event_states()
    print("Done creating EventStates")

    classes = get_all_job_event_classes()
    print("Done creating JobEvent classes")

    import_text, states_text, union_text = gen_file(states, classes)
    write_file(import_text, states_text, union_text, typings_file)


if __name__ == "__main__":
    # get path to this files location
    root = f"{sys.path[0]}/../../"
    typings_file = f"{root}/armada_client/typings.py"

    main()
    sys.exit(0)
