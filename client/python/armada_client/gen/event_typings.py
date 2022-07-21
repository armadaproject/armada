import sys
from importlib import import_module


def get_event_states(module):
    return [x for x in module.EventMessage.DESCRIPTOR.fields_by_name if "__" not in x]


def get_all_job_event_classes(module):
    return [
        getattr(module, x)
        for x in module.__dict__
        if hasattr(getattr(module, x), "job_id")
    ]


def place_event_states(states):
    enum_options = "\n".join(f'    {x} = "{x}"' for x in states)

    docstring = '\n    """\n' + "    Enum for the event states." + '\n    """\n'

    states_text = (
        f"from enum import Enum\n\n\nclass EventType(Enum):{docstring}\n"
        f"{enum_options}\n"
    )

    with open(f"{root}/armada_client/typings.py", "w", encoding="utf-8") as f:
        f.write(states_text)


def main():
    event_module = import_module("armada_client.armada.event_pb2")

    states = get_event_states(event_module)
    place_event_states(states)
    print("Done creating EventStates")

    print(get_all_job_event_classes(event_module))


if __name__ == "__main__":
    # get path to this files location
    root = f"{sys.path[0]}/../../"
    # append dir
    sys.path.append(root)
    sys.path.append(f"{root}/armada_client")

    main()
    sys.exit(0)
