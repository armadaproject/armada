"""Exercise the lint wrapper's process contract without UI dependencies."""

import os
from pathlib import Path
import selectors
import shutil
import subprocess
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "lint-typescript.sh"


class TypeScriptLint(unittest.TestCase):
    def fixture(self, root, exit_status, stdout, stderr, summary_mode="file"):
        script = root / "repo/.github/scripts/lint-typescript.sh"
        script.parent.mkdir(parents=True)
        shutil.copyfile(SCRIPT, script)
        ui = root / "repo/internal/lookoutui"
        ui.mkdir(parents=True)
        tools = root / "tools"
        tools.mkdir()
        yarn = tools / "yarn"
        yarn.write_text(
            '#!/bin/sh\n'
            'printf "%s\\n" "$PWD" > "$TEST_LINT_CWD"\n'
            'printf "%s" "$TEST_LINT_STDOUT"\n'
            'printf "%s" "$TEST_LINT_STDERR" >&2\n'
            'if [ "${TEST_LINT_WAIT:-}" = 1 ]; then read -r release; fi\n'
            'exit "$TEST_LINT_EXIT"\n'
        )
        yarn.chmod(0o755)
        scratch = root / "scratch"
        scratch.mkdir()
        summary = root / "summary with spaces.md"
        env = dict(os.environ, PATH=str(tools) + os.pathsep + os.environ["PATH"],
                   TMPDIR=str(scratch), TEST_LINT_EXIT=str(exit_status),
                   TEST_LINT_STDOUT=stdout, TEST_LINT_STDERR=stderr,
                   TEST_LINT_CWD=str(root / "lint-cwd"))
        env.pop("GITHUB_STEP_SUMMARY", None)
        if summary_mode == "file":
            env["GITHUB_STEP_SUMMARY"] = str(summary)
        elif summary_mode == "unwritable":
            env["GITHUB_STEP_SUMMARY"] = str(root)  # A directory cannot be appended to.
        return script, ui, scratch, summary, env

    def run_lint(self, exit_status, stdout="", stderr="", summary_mode="file"):
        with tempfile.TemporaryDirectory(prefix="lint-summary-") as directory:
            root = Path(directory)
            script, ui, scratch, summary, env = self.fixture(
                root, exit_status, stdout, stderr, summary_mode)
            result = subprocess.run(["bash", str(script)], cwd=root, env=env,
                                    capture_output=True, text=True, timeout=10)
            self.assertEqual(Path((root / "lint-cwd").read_text().strip()).resolve(), ui.resolve())
            self.assertEqual(list(scratch.iterdir()), [])
            self.assertEqual(list(ui.iterdir()), [])
            self.assertFalse((root / "lint_results.txt").exists())
            return result, summary.read_text() if summary.exists() else ""

    def test_exit_status_and_summary(self):
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
                result, summary = self.run_lint(exit_status, stdout=output)
                self.assertEqual(result.returncode, exit_status, result.stdout + result.stderr)
                heading = ("No lint issues found." if exit_status == 0
                           else "Lint failed (exit status %s)." % exit_status)
                self.assertIn(heading, summary)
                if exit_status == 0:
                    self.assertNotIn("Lint failed", summary)
                for line in output.splitlines():
                    self.assertIn("    " + line, summary)

    def test_stdout_and_stderr_are_retained(self):
        stdout = "First diagnostic\nSecond diagnostic\n"
        stderr = "Configuration could not be loaded\n"
        result, summary = self.run_lint(2, stdout=stdout, stderr=stderr)
        self.assertEqual(result.returncode, 2)
        self.assertIn(stdout + stderr, result.stdout)
        for line in (stdout + stderr).splitlines():
            self.assertIn("    " + line, summary)

    def test_summary_failure_cannot_hide_output_or_change_lint_status(self):
        for mode in ("unset", "unwritable"):
            for status in (0, 2, 127):
                with self.subTest(mode=mode, status=status):
                    result, _ = self.run_lint(status, stdout="Diagnostic\n", summary_mode=mode)
                    self.assertEqual(result.returncode, status)
                    self.assertIn("Diagnostic", result.stdout)

    def test_summary_is_bounded_and_backticks_are_literal(self):
        output = "start\n" + "x" * 70000 + "\n```\n````\n# misleading heading\nlast\n"
        result, summary = self.run_lint(2, stdout=output)
        self.assertEqual(result.returncode, 2)
        self.assertEqual(result.stdout, output)
        self.assertLess(len(summary.encode()), 70000)
        self.assertIn("truncated", summary)
        self.assertNotIn("start", summary)
        self.assertIn("    ```\n    ````\n    # misleading heading\n    last", summary)

    def test_output_is_visible_before_lint_finishes(self):
        with tempfile.TemporaryDirectory(prefix="lint-stream-") as directory:
            root = Path(directory)
            script, _, scratch, _, env = self.fixture(root, 2, "Ready\n", "")
            env["TEST_LINT_WAIT"] = "1"
            with subprocess.Popen(["bash", str(script)], cwd=root, env=env,
                                  stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE, text=True) as process:
                try:
                    with selectors.DefaultSelector() as selector:
                        selector.register(process.stdout, selectors.EVENT_READ)
                        self.assertTrue(selector.select(timeout=5), "No live lint output")
                    self.assertEqual(process.stdout.readline(), "Ready\n")
                    self.assertIsNone(process.poll())
                    process.communicate("release\n", timeout=5)
                    self.assertEqual(process.returncode, 2)
                finally:
                    if process.poll() is None:
                        process.kill()
                        process.communicate()
            self.assertEqual(list(scratch.iterdir()), [])


if __name__ == "__main__":
    unittest.main()
