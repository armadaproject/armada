import sys
from importlib import import_module

# append dir
sys.path.append("./client/python")
sys.path.append("./client/python/armada_client")

event_module = import_module("armada_client.armada.event_pb2")


def get_types(module):
    return [x for x in module.EventMessage.DESCRIPTOR.fields_by_name if "__" not in x]


types = get_types(event_module)

enum_options = "\n".join(f'    {x} = "{x}"' for x in types)

docstring = '\n    """\n' + "    Enum for the event states." + '\n    """\n'

types_text = (
    f"from enum import Enum\n\n\nclass EventType(Enum):{docstring}\n"
    f"{enum_options}\n"
)

with open("./client/python/armada_client/typings.py", "w", encoding="utf-8") as f:
    f.write(types_text)
