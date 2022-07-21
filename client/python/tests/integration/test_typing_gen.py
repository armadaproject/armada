import os
from importlib import import_module

from code_gen.event_typings import (
    gen_file,
    get_all_job_event_classes,
    get_event_states,
)


def test_file_gen():
    typings_file = "./typings.py"

    states = get_event_states()
    print("Done creating EventStates")

    classes = get_all_job_event_classes()
    print("Done creating JobEvent classes")

    gen_file(states, classes, typings_file)

    # This is what we expect to not fail
    import_module("typings")

    # remove typings.py
    os.remove(typings_file)
