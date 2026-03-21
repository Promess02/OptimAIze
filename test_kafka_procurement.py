#!/usr/bin/env python3
"""
Kafka integration test for Procurement Agent.
It emits a CDC event to Kafka topic `database_changes` and verifies
that procurement report gets refreshed.

Prerequisites:
- docker-compose stack is running
- kafka service is reachable through docker-compose exec
- procurement_agent exposed on localhost:8004
"""

import json
import os
import subprocess
import sys
import time

import requests

BASE_URL = "http://localhost:8004"
TIMEOUT = 15
MAX_WAIT_SECONDS = 120
TEST_PRODUCT_ID = os.getenv("TEST_PRODUCT_ID", "P0103")


def run_shell(command: str):
    return subprocess.run(command, shell=True, capture_output=True, text=True)


def get_latest_generated_at() -> str:
    response = requests.get(f"{BASE_URL}/report/latest", timeout=TIMEOUT)
    if response.status_code == 202:
        return ""
    response.raise_for_status()
    payload = response.json()
    return payload.get("generated_at", "")


def emit_cdc_event() -> None:
    event = {
        "event_type": "SalesCreated",
        "product_id": TEST_PRODUCT_ID,
        "timestamp": int(time.time()),
    }

    command = (
        "cd /home/miko/magister && "
        "echo '" + json.dumps(event).replace("'", "'\\''") + "' | "
        "docker-compose exec -T kafka "
        "kafka-console-producer --bootstrap-server kafka:29092 --topic database_changes"
    )

    result = run_shell(command)
    if result.returncode != 0:
        raise RuntimeError(
            "Failed to emit Kafka CDC event. "
            f"stdout={result.stdout.strip()} stderr={result.stderr.strip()}"
        )


def main() -> int:
    print("=" * 60)
    print("Kafka -> Procurement Agent Integration Test")
    print("=" * 60)

    try:
        requests.post(f"{BASE_URL}/report", json={"months_options": [3, 6, 12]}, timeout=TIMEOUT)

        before = ""
        warmup_deadline = time.time() + MAX_WAIT_SECONDS
        while time.time() < warmup_deadline:
            before = get_latest_generated_at()
            if before:
                break
            time.sleep(3)

        if not before:
            print("✗ Could not read initial generated_at from /report/latest")
            return 1

        print(f"Initial report timestamp: {before}")
        emit_cdc_event()
        print("✓ Emitted SalesCreated event to Kafka topic database_changes")

        deadline = time.time() + MAX_WAIT_SECONDS
        while time.time() < deadline:
            time.sleep(5)
            after = get_latest_generated_at()
            if after and after != before:
                print(f"✓ Report refreshed after Kafka event: {after}")
                print("✓ Kafka integration test passed")
                return 0

        print("✗ Report timestamp was not refreshed in expected time window")
        return 1

    except requests.exceptions.RequestException as exc:
        print(f"✗ HTTP error: {exc}")
        return 1
    except Exception as exc:
        print(f"✗ Test failed: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
