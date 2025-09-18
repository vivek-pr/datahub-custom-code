import os
import subprocess

import pytest

TARGETS = [
    "poc-up",
    "poc-verify",
    "poc-destroy",
    "poc-logs",
    "poc-smoke",
]


@pytest.mark.parametrize("target", TARGETS)
def test_make_targets_dry_run(target):
    env = os.environ.copy()
    env["MAKE"] = "echo"
    result = subprocess.run(["make", "-n", target], capture_output=True, text=True, env=env)
    assert result.returncode == 0, result.stderr
    assert "make" in result.stdout or result.stdout.strip() != ""


def test_poc_smoke_invokes_subtargets():
    env = os.environ.copy()
    env["MAKE"] = "echo"
    result = subprocess.run(["make", "-n", "poc-smoke"], capture_output=True, text=True, env=env)
    assert "poc-up" in result.stdout
    assert "poc-verify" in result.stdout
