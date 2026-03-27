#!/usr/bin/env python3
"""
Unified test runner for the Magister multi-agent project.

Runs:
- test_demand_inventory_scenarios.py (local/unit-style)
- test_system.py (integration)
- test_procurement_agent.py (integration)
- test_kafka_procurement.py (integration)

Integration tests are executed only when required HTTP services are available.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import List, Tuple

import requests

ROOT = Path(__file__).resolve().parent

LOCAL_TESTS = [
    "test_demand_inventory_scenarios.py",
]

INTEGRATION_TESTS = [
    "test_system.py",
    "test_procurement_agent.py",
    "test_kafka_procurement.py",
]

REQUIRED_HEALTH_ENDPOINTS = [
    "http://localhost:8001/health",
    "http://localhost:8002/health",
    "http://localhost:8003/health",
    "http://localhost:8004/health",
]


def run_python_script(script_name: str) -> Tuple[bool, str]:
    script_path = ROOT / script_name
    if not script_path.exists():
        return False, f"Missing test file: {script_name}"

    print("=" * 80)
    print(f"RUNNING: {script_name}")
    print("=" * 80)

    result = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=str(ROOT),
    )

    if result.returncode == 0:
        return True, f"PASS: {script_name}"
    return False, f"FAIL: {script_name} (exit code: {result.returncode})"


def integration_stack_ready() -> bool:
    for url in REQUIRED_HEALTH_ENDPOINTS:
        try:
            response = requests.get(url, timeout=3)
            if response.status_code != 200:
                return False
        except requests.RequestException:
            return False
    return True


def main() -> int:
    summary: List[str] = []
    failures = 0

    print("\nMagister Test Runner\n")

    for script in LOCAL_TESTS:
        ok, msg = run_python_script(script)
        summary.append(msg)
        if not ok:
            failures += 1

    if integration_stack_ready():
        print("\nIntegration services are healthy. Running integration tests...\n")
        for script in INTEGRATION_TESTS:
            ok, msg = run_python_script(script)
            summary.append(msg)
            if not ok:
                failures += 1
    else:
        summary.append("SKIP: integration tests (services not healthy on ports 8001-8004)")
        print("\nSkipping integration tests: required services are not healthy.\n")

    print("\n" + "-" * 80)
    print("SUMMARY")
    print("-" * 80)
    for line in summary:
        print(line)

    if failures:
        print(f"\nTotal failures: {failures}")
        return 1

    print("\nAll executed tests passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
