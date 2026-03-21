#!/usr/bin/env python3
"""
Integration tests for Procurement Agent behavior.
Requires running stack (docker-compose up -d).
"""

import os
import sys
import time
from typing import Dict, Any

import requests

BASE_URL = "http://localhost:8004"
TIMEOUT = 20
POLL_SECONDS = 180
TEST_PRODUCT_ID = os.getenv("TEST_PRODUCT_ID", "P0103")


def ok(message: str):
    print(f"✓ {message}")


def fail(message: str):
    print(f"✗ {message}")


def assert_true(condition: bool, message: str):
    if not condition:
        raise AssertionError(message)


def get_json(path: str) -> Dict[str, Any]:
    response = requests.get(f"{BASE_URL}{path}", timeout=TIMEOUT)
    response.raise_for_status()
    return response.json()


def post_json(path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    response = requests.post(f"{BASE_URL}{path}", json=payload, timeout=TIMEOUT)
    response.raise_for_status()
    return response.json()


def test_health():
    health = get_json("/health")
    assert_true(health.get("status") == "healthy", "Health status should be healthy")
    assert_true(health.get("agent") == "procurement", "Agent should be procurement")
    ok("/health endpoint works")


def test_train():
    result = post_json("/train", {})
    assert_true(result.get("status") in {"accepted", "completed"}, "Train should be accepted or completed")
    ok("/train endpoint works")


def test_forecast():
    result = post_json("/forecast", {"product_id": TEST_PRODUCT_ID, "months": 3})
    assert_true(result.get("product_id") == TEST_PRODUCT_ID, "Forecast product_id mismatch")
    monthly = result.get("monthly_forecast", [])
    assert_true(len(monthly) >= 1, "monthly_forecast should have at least 1 month")

    for row in monthly:
        assert_true("month_bucket" in row, "Forecast row missing month_bucket")
        assert_true("forecast_demand" in row, "Forecast row missing forecast_demand")
        assert_true(int(row["forecast_demand"]) >= 0, "forecast_demand must be non-negative")

    ok("/forecast endpoint works")


def test_report():
    result = post_json("/report", {"months_options": [3, 6, 12]})
    assert_true(result.get("status") in {"accepted", "completed"}, "Report should be accepted or completed")

    latest = None
    started = time.time()
    while time.time() - started < POLL_SECONDS:
        response = requests.get(f"{BASE_URL}/report/latest", timeout=TIMEOUT)
        if response.status_code == 200:
            latest = response.json()
            break
        time.sleep(3)

    assert_true(latest is not None, "Latest report should become available")
    assert_true("reports" in latest, "Latest report should contain reports")
    assert_true(latest.get("scope") == "top_products", "Report should be limited to top products scope")
    assert_true(int(latest.get("products_limit", 0)) == 10, "Report products_limit should be 10")

    reports = latest.get("reports", [])
    horizons = sorted([int(item.get("horizon_months", 0)) for item in reports])
    assert_true(horizons == [3, 6, 12], "Report should contain horizons 3, 6, 12")

    for block in reports:
        for row in block.get("rows", []):
            for field in [
                "product_id",
                "month",
                "forecast_demand",
                "current_stock",
                "recommended_buy_qty",
                "generated_at",
            ]:
                assert_true(field in row, f"Missing field in report row: {field}")
            assert_true(int(row["recommended_buy_qty"]) >= 0, "recommended_buy_qty must be non-negative")

    ok("/report and /report/latest endpoints work")


def test_ask():
    per_product = post_json("/ask", {"question": "What is demand?", "product_id": TEST_PRODUCT_ID})
    assert_true("answer" in per_product, "Ask response for product should contain answer")

    top = post_json("/ask", {"question": "Which product has highest demand?"})
    assert_true("answer" in top, "Ask response for top demand should contain answer")
    ok("/ask endpoint works")


def main():
    print("=" * 60)
    print("Procurement Agent Integration Test")
    print("=" * 60)

    try:
        test_health()
        test_train()
        test_forecast()
        test_report()
        test_ask()
    except requests.exceptions.RequestException as exc:
        fail(f"HTTP request failed: {exc}")
        return 1
    except AssertionError as exc:
        fail(str(exc))
        return 1

    ok("All procurement agent tests passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
