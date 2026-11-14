"""Test the CI wrapper's process contract without installing UI dependencies."""

import os
from pathlib import Path
import subprocess
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "lint-typescript.sh"


class TypeScriptLint(unittest.TestCase):
    def run_lint(self, exit_status, stdout="", stderr=""):
        with tempfile.TemporaryDirectory(prefix="lint-summary-") as directory:
            root = Path(directory)
            yarn = root / "yarn"
            yarn.write_text(
                '#!/bin/sh\n'
                'printf "%s" "$TEST_LINT_STDOUT"\n'
                'printf "%s" "$TEST_LINT_STDERR" >&2\n'
                'exit "$TEST_LINT_EXIT"\n'
            )
            yarn.chmod(0o755)
            summary = root / "summary.md"
            env = dict(os.environ, PATH=directory + os.pathsep + os.environ["PATH"],
                       GITHUB_STEP_SUMMARY=str(summary), TEST_LINT_EXIT=str(exit_status),
                       TEST_LINT_STDOUT=stdout, TEST_LINT_STDERR=stderr)
            result = subprocess.run(["bash", str(SCRIPT)], cwd=root, env=env,
                                    capture_output=True, text=True, timeout=10)
            return result, summary.read_text()

    def test_exit_status_is_independent_of_output_text(self):
        cases = [
            ("clean", 0, "Done\n"),
            ("successful_output_mentions_problem", 0, "No problem found\n"),
            ("lint_finding", 1, "1 problem (1 error, 0 warnings)\n"),
            ("failure_without_problem_marker", 1, "Lint command failed\n"),
            ("operational_error", 2, "ESLint could not find a configuration file\n"),
            ("empty_failure", 2, ""),
            ("command_unavailable", 127, "command not found\n"),
        ]
        for name, exit_status, output in cases:
            with self.subTest(name=name):
                result, _ = self.run_lint(exit_status, stdout=output)
                self.assertEqual(result.returncode, exit_status, result.stdout + result.stderr)

    def test_stdout_and_stderr_are_retained_in_log_and_summary(self):
        stdout = "First diagnostic\nSecond diagnostic\n"
        stderr = "Configuration could not be loaded\n"
        result, summary = self.run_lint(2, stdout=stdout, stderr=stderr)
        self.assertEqual(result.returncode, 2)
        self.assertIn(stdout + stderr, result.stdout)
        self.assertIn(stdout + stderr, summary)
        self.assertIn("exit status 2", summary)


if __name__ == "__main__":
    unittest.main()
