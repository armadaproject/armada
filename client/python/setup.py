import os
from pathlib import Path
import shutil
import subprocess
import sys
from typing import Dict
from setuptools import setup
import importlib.resources
import re
from setuptools.command.build_py import build_py


def generate_grpc_bindings(build_lib: Path):
    import grpc_tools.protoc

    proto_include = importlib.resources.path("grpc_tools", "_proto")
    proto_files = [
        "google/api/annotations.proto",
        "google/api/http.proto",
        "github.com/gogo/protobuf/gogoproto/gogo.proto",
        "k8s.io/api/core/v1/generated.proto",
        "k8s.io/apimachinery/pkg/api/resource/generated.proto",
        "k8s.io/apimachinery/pkg/apis/meta/v1/generated.proto",
        "k8s.io/apimachinery/pkg/runtime/generated.proto",
        "k8s.io/apimachinery/pkg/runtime/schema/generated.proto",
        "k8s.io/apimachinery/pkg/util/intstr/generated.proto",
        "k8s.io/api/networking/v1/generated.proto",
        "armada/event.proto",
        "armada/submit.proto",
        "armada/health.proto",
        "armada/job.proto",
        "armada/binoculars.proto",
    ]
    target_root = build_lib.absolute() / "armada_client"

    for proto_file in proto_files:
        command = [
            f"-I{proto_include}",
            f"-I{target_root / 'proto'}",
            f"--python_out={target_root}",
            f"--grpc_python_out={target_root}",
            f"--mypy_out={target_root}",
            str(target_root / "proto" / proto_file),
        ]
        if grpc_tools.protoc.main(command) != 0:
            raise Exception(f"grpc_tools.protoc.main: {command} failed")

    shutil.rmtree(target_root / "github.com")
    shutil.rmtree(target_root / "k8s.io")

    adjust_import_paths(target_root)


def adjust_import_paths(output_dir: Path):
    replacements = {
        r"from armada": "from armada_client.armada",
        r"from github.com": "from armada_client.github.com",
        r"from google.api": "from armada_client.google.api",
    }

    for file in output_dir.glob("armada/*.py"):
        replace_in_file(file, replacements)
    for file in output_dir.glob("google/api/*.py"):
        replace_in_file(file, replacements)

    replacements = {
        r"from k8s.io": "from armada_client.k8s.io",
    }
    for file in output_dir.glob("../**/*.py"):
        replace_in_file(file, replacements)

    replacements = {
        r" k8s": " armada_client.k8s",
        r"\[k8s": "[armada_client.k8s",
        r"import k8s.io": "import armada_client.k8s.io",
    }
    for file in output_dir.glob("k8s/**/*.pyi"):
        replace_in_file(file, replacements)


def replace_in_file(file: Path, replacements: Dict[str, str]):
    """Replace patterns in a file based on the replacements dictionary."""

    content = file.read_text()
    for pattern, replacement in replacements.items():
        content = re.sub(pattern, replacement, content)
    file.write_text(content)


def generate_typings(build_dir: Path):
    typings = build_dir.absolute() / "armada_client" / "typings.py"
    result = subprocess.run(
        args=[
            sys.executable,
            str(build_dir.absolute() / "armada_client" / "gen" / "event_typings.py"),
            str(typings),
        ],
        env={"PYTHONPATH": str(build_dir.absolute())},
        capture_output=True,
    )
    if result.returncode != 0:
        print(result.stdout)
        print(result.stderr)
    result.check_returncode()


class BuildPackageProtos(build_py):
    """
    Generate GRPC code before building the package.
    """

    def run(self):
        super().run()
        output_dir = Path(".") if self.editable_mode else Path(self.build_lib)
        generate_grpc_bindings(output_dir)
        generate_typings(output_dir)


setup(
    cmdclass={
        "build_py": BuildPackageProtos,
        "develop": BuildPackageProtos,
    },
)
