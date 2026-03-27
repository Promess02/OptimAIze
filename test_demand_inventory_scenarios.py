#!/usr/bin/env python3
import importlib.util
import json
import os
import pickle
import sqlite3
import sys
import tempfile
import types
from datetime import datetime

import numpy as np


def install_dependency_stubs():
    confluent_kafka = types.ModuleType("confluent_kafka")

    class FakeConsumer:
        def __init__(self, *_args, **_kwargs):
            self.topics = []

        def subscribe(self, topics):
            self.topics = topics

        def poll(self, _timeout):
            return None

        def close(self):
            return None

    class FakeProducer:
        def __init__(self, *_args, **_kwargs):
            self.messages = []

        def produce(self, topic, key=None, value=None, callback=None):
            self.messages.append({"topic": topic, "key": key, "value": value})
            if callback:
                callback(None, types.SimpleNamespace(topic=lambda: topic, partition=lambda: 0))

        def poll(self, _timeout):
            return None

        def flush(self):
            return None

    confluent_kafka.Consumer = FakeConsumer
    confluent_kafka.Producer = FakeProducer
    sys.modules["confluent_kafka"] = confluent_kafka

    neo4j = types.ModuleType("neo4j")

    class FakeSession:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            return False

        def run(self, *_args, **_kwargs):
            return None

    class FakeDriver:
        def session(self):
            return FakeSession()

    class GraphDatabase:
        @staticmethod
        def driver(*_args, **_kwargs):
            return FakeDriver()

    neo4j.GraphDatabase = GraphDatabase
    sys.modules["neo4j"] = neo4j

    redis_mod = types.ModuleType("redis")

    class FakeRedis:
        def __init__(self, *_args, **_kwargs):
            self.store = {}

        def get(self, key):
            return self.store.get(key)

        def set(self, key, value):
            self.store[key] = value
            return True

        def setex(self, key, _ttl, value):
            self.store[key] = value
            return True

        def exists(self, key):
            return key in self.store

        def delete(self, key):
            self.store.pop(key, None)
            return True

    redis_mod.Redis = FakeRedis
    sys.modules["redis"] = redis_mod

    fastapi = types.ModuleType("fastapi")

    class HTTPException(Exception):
        def __init__(self, status_code, detail):
            self.status_code = status_code
            self.detail = detail
            super().__init__(f"{status_code}: {detail}")

    class FastAPI:
        def __init__(self, *_args, **_kwargs):
            pass

        def post(self, *_args, **_kwargs):
            def decorator(func):
                return func

            return decorator

        def get(self, *_args, **_kwargs):
            def decorator(func):
                return func

            return decorator

    fastapi.FastAPI = FastAPI
    fastapi.HTTPException = HTTPException
    sys.modules["fastapi"] = fastapi

    pydantic = types.ModuleType("pydantic")

    class BaseModel:
        def __init__(self, **kwargs):
            for key, value in kwargs.items():
                setattr(self, key, value)

    pydantic.BaseModel = BaseModel
    sys.modules["pydantic"] = pydantic

    uvicorn = types.ModuleType("uvicorn")

    def run(*_args, **_kwargs):
        return None

    uvicorn.run = run
    sys.modules["uvicorn"] = uvicorn


class LinearModel:
    def predict(self, X):
        length = len(X)
        return np.array([idx - 2 for idx in range(length)], dtype=float)


class ConstantNegativeModel:
    def predict(self, X):
        return np.array([-10.0 for _ in range(len(X))], dtype=float)


def load_module(module_name, file_path):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def create_temp_inventory_db():
    fd, db_path = tempfile.mkstemp(prefix="inventory_agent_", suffix=".db")
    os.close(fd)

    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE inventory (
            product_id TEXT PRIMARY KEY,
            current_stock INTEGER,
            price REAL
        )
        """
    )
    conn.execute("INSERT INTO inventory (product_id, current_stock, price) VALUES (?, ?, ?)", ("P001", 10, 99.99))
    conn.execute("INSERT INTO inventory (product_id, current_stock, price) VALUES (?, ?, ?)", ("P002", 30, 129.50))
    conn.commit()
    conn.close()

    return db_path


def assert_equal(actual, expected, message):
    if actual != expected:
        raise AssertionError(f"{message}. Expected={expected}, actual={actual}")


def assert_true(condition, message):
    if not condition:
        raise AssertionError(message)


def run_tests():
    install_dependency_stubs()

    demand = load_module("demand_main_test", "/home/miko/magister/agents/demand/main.py")
    inventory = load_module("inventory_main_test", "/home/miko/magister/agents/inventory/main.py")

    print("[1] demand: missing model returns error")
    demand.redis_client.store.clear()
    missing = demand.predict_demand("UNKNOWN", horizon_days=7)
    assert_true("error" in missing, "Missing model should return error payload")

    print("[2] demand: clipped non-negative predictions and cache write")
    payload = {
        "model": LinearModel(),
        "min_date": datetime(2024, 1, 1),
        "feature_cols": [
            "dayofweek",
            "month",
            "day",
            "quarter",
            "year",
            "day_of_year",
            "is_month_end",
            "is_quarter_end",
            "days_since_start",
            "sales_lag_1",
            "sales_lag_7",
            "sales_lag_14",
            "sales_lag_30",
            "sales_rolling_mean_7",
            "sales_rolling_std_7",
            "sales_rolling_mean_14",
            "sales_rolling_std_14",
            "sales_rolling_mean_30",
            "sales_rolling_std_30",
            "extra_missing_feature",
        ],
        "historical_sales_mean": 50.0,
        "historical_sales_std": 7.0,
    }
    demand.redis_client.set("model:P001", pickle.dumps(payload))
    result = demand.predict_demand("P001", horizon_days=5)
    assert_true("error" not in result, f"Expected successful prediction, got {result}")
    assert_equal(result["predicted_demand"], 3, "Predicted demand should clip negatives and sum positives")
    cached = json.loads(demand.redis_client.get("prediction:P001"))
    assert_equal(cached["horizon_days"], 5, "Cached prediction should include requested horizon")

    print("[3] demand: all-negative model clipped to zero")
    payload_negative = dict(payload)
    payload_negative["model"] = ConstantNegativeModel()
    demand.redis_client.set("model:PNEG", pickle.dumps(payload_negative))
    negative_result = demand.predict_demand("PNEG", horizon_days=10)
    assert_true("error" not in negative_result, "Negative model should still return successful payload")
    assert_equal(negative_result["predicted_demand"], 0, "All-negative predictions should sum to zero after clipping")

    print("[4] inventory: order created and DB updated when demand exceeds stock")
    db_path = create_temp_inventory_db()
    inventory.DB_PATH = db_path
    order = inventory.calculate_order("P001", predicted_demand=25)
    assert_true("error" not in order, f"Inventory order should succeed, got {order}")
    assert_equal(order["order_quantity"], 15, "Order quantity should fill the shortage")
    conn = sqlite3.connect(db_path)
    updated_stock = conn.execute("SELECT current_stock FROM inventory WHERE product_id = 'P001'").fetchone()[0]
    conn.close()
    assert_equal(updated_stock, 25, "Inventory DB should be updated to projected stock")

    print("[5] inventory: no order and no DB increase when stock is sufficient")
    order_no_buy = inventory.calculate_order("P002", predicted_demand=20)
    assert_true("error" not in order_no_buy, f"Inventory order should succeed, got {order_no_buy}")
    assert_equal(order_no_buy["order_quantity"], 0, "Order quantity should be zero when stock is sufficient")
    conn = sqlite3.connect(db_path)
    unchanged_stock = conn.execute("SELECT current_stock FROM inventory WHERE product_id = 'P002'").fetchone()[0]
    conn.close()
    assert_equal(unchanged_stock, 30, "Inventory DB should remain unchanged when no order is needed")

    print("[6] inventory: unknown product fallback stock logic")
    fallback = inventory.calculate_order("UNKNOWN", predicted_demand=80)
    assert_true("error" not in fallback, f"Fallback flow should succeed, got {fallback}")
    assert_equal(fallback["current_stock"], 100, "Unknown product should use fallback current stock=100")
    assert_equal(fallback["order_quantity"], 0, "No order needed when fallback stock already covers demand")

    os.remove(db_path)
    print("\nAll demand/inventory scenario tests passed.")


if __name__ == "__main__":
    run_tests()
