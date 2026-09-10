"""Integration tests that run real Metaflow flows.

The backends are stand-ins, so no cloud account is needed, but everything between the
decorator and Metaflow's task lifecycle is the real thing. This is where session
detachment, artifact ordering, and metadata registration are actually verified.
"""

import os
import subprocess
import sys

import pytest

FLOW_DIR = os.path.join(os.path.dirname(__file__), "flows")
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def run_flow(name, datastore, extra_args=()):
    env = dict(os.environ)
    env.update(
        {
            # Keep the test off any configured metadata service.
            "METAFLOW_DEFAULT_METADATA": "local",
            "METAFLOW_DATASTORE_SYSROOT_LOCAL": str(datastore),
            "METAFLOW_USER": "spark-tests",
            "PYTHONPATH": REPO_ROOT + os.pathsep + env.get("PYTHONPATH", ""),
        }
    )
    return subprocess.run(
        [sys.executable, os.path.join(FLOW_DIR, name), "--no-pylint", "run", *extra_args],
        capture_output=True,
        text=True,
        env=env,
        cwd=str(datastore),
        timeout=600,
    )


@pytest.mark.integration
def test_session_mode_flow(tmp_path):
    result = run_flow("session_flow.py", tmp_path)
    assert "session flow ok" in result.stdout, result.stdout + result.stderr
    assert result.returncode == 0


@pytest.mark.integration
def test_submit_mode_flow(tmp_path):
    result = run_flow("submit_flow.py", tmp_path)
    assert "submit flow ok" in result.stdout, result.stdout + result.stderr
    assert result.returncode == 0
    # The remote URLs are surfaced in the task log, not buried in metadata only.
    assert "https://example.invalid/runs/fake-run-1" in result.stdout


@pytest.mark.integration
def test_a_failed_job_fails_the_step_with_the_remote_error(tmp_path):
    result = run_flow("failure_flow.py", tmp_path)
    assert result.returncode != 0
    combined = result.stdout + result.stderr
    assert "Spark job failed" in combined
    assert "AnalysisException" in combined
    assert "the step body must not run" not in combined


@pytest.mark.integration
def test_crash_on_failure_false_lets_the_step_continue(tmp_path):
    result = run_flow("tolerated_failure_flow.py", tmp_path)
    assert "tolerated failure ok" in result.stdout, result.stdout + result.stderr
    assert result.returncode == 0
