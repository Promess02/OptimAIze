import json
import math
import os
import pickle
import sqlite3
import threading
from contextlib import closing
from datetime import datetime, timedelta
from typing import Dict, List
from uuid import uuid4

import numpy as np
import pandas as pd
import redis
import uvicorn
import xgboost as xgb
from apscheduler.schedulers.background import BackgroundScheduler
from confluent_kafka import Consumer, Producer
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

app = FastAPI(title="Procurement Demand Agent API")

KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
REDIS_HOST = os.getenv("REDIS_HOST", "redis_procurement")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
DB_PATH = os.getenv("DB_PATH", "/data/ecommerce.db")
MODELS_PATH = os.getenv("MODELS_PATH", "/models")
SCHEDULE_MINUTES = int(os.getenv("SCHEDULE_MINUTES", "30"))
TOP_PRODUCTS_LIMIT = int(os.getenv("TOP_PRODUCTS_LIMIT", "10"))
FORCE_RETRAIN = os.getenv("FORCE_RETRAIN", "0") == "1"

REPORT_KEY = "procurement:report:latest"
LOCK_KEY = "procurement:job:lock"
MODEL_PREFIX = "procurement:model"

ABC_THRESHOLDS = (0.80, 0.95)
XYZ_CV_THRESHOLDS = (0.5, 1.0)
XYZ_SAFETY_FACTORS = {"X": 0.20, "Y": 0.35, "Z": 0.50}
DEFAULT_LEAD_TIME_DAYS = int(os.getenv("DEFAULT_LEAD_TIME_DAYS", "7"))
DEFAULT_ORDER_COST = float(os.getenv("DEFAULT_ORDER_COST", "50"))
DEFAULT_HOLDING_COST = float(os.getenv("DEFAULT_HOLDING_COST", "2"))

BEST_XGB_PARAMS = {
    "objective": "reg:squarederror",
    "n_estimators": 200,
    "max_depth": 6,
    "learning_rate": 0.05,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "random_state": 42,
    "tree_method": "hist",
    "verbosity": 0,
}

FEATURE_COLUMNS = [
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
]

redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=False)

producer_conf = {"bootstrap.servers": KAFKA_BROKER}
producer = Producer(producer_conf)

consumer_conf = {
    "bootstrap.servers": KAFKA_BROKER,
    "group.id": "procurement_agent_group",
    "auto.offset.reset": "earliest",
}
consumer = Consumer(consumer_conf)
consumer.subscribe(["database_changes"])

scheduler = BackgroundScheduler()


class ForecastRequest(BaseModel):
    product_id: str
    months: int = Field(default=3, ge=1, le=12)


class ReportRequest(BaseModel):
    months_options: List[int] = Field(default_factory=lambda: [3, 6, 12])


class AskRequest(BaseModel):
    question: str
    product_id: str | None = None


def extract_time_features(df: pd.DataFrame, is_future: bool = False, min_date: pd.Timestamp | None = None):
    df = df.copy()
    df["dayofweek"] = df["date"].dt.dayofweek
    df["month"] = df["date"].dt.month
    df["day"] = df["date"].dt.day
    df["quarter"] = df["date"].dt.quarter
    df["year"] = df["date"].dt.year
    df["day_of_year"] = df["date"].dt.dayofyear
    df["is_month_end"] = df["date"].dt.is_month_end.astype(int)
    df["is_quarter_end"] = df["date"].dt.is_quarter_end.astype(int)

    if not is_future:
        min_date = df["date"].min()

    df["days_since_start"] = (df["date"] - min_date).dt.days

    if not is_future:
        for lag in [1, 7, 14, 30]:
            df[f"sales_lag_{lag}"] = df["sales"].shift(lag)

        for window in [7, 14, 30]:
            df[f"sales_rolling_mean_{window}"] = df["sales"].rolling(window=window).mean()
            df[f"sales_rolling_std_{window}"] = df["sales"].rolling(window=window).std()

        df = df.bfill().ffill()
        df = df.fillna(df.mean(numeric_only=True))

    return df, min_date


def get_connection():
    return sqlite3.connect(DB_PATH)


def list_products() -> List[str]:
    with closing(get_connection()) as conn:
        df = pd.read_sql("SELECT DISTINCT product_id FROM sales_aggregated", conn)
    return df["product_id"].astype(str).tolist()


def list_top_products(limit: int = TOP_PRODUCTS_LIMIT) -> List[str]:
    with closing(get_connection()) as conn:
        df = pd.read_sql(
            """
            SELECT product_id, SUM(sales) AS total_sales
            FROM sales_aggregated
            GROUP BY product_id
            ORDER BY total_sales DESC
            LIMIT ?
            """,
            conn,
            params=(int(limit),),
        )
    return df["product_id"].astype(str).tolist()


def train_model_for_product(product_id: str) -> bool:
    model_key = f"{MODEL_PREFIX}:{product_id}"
    if not FORCE_RETRAIN and redis_client.exists(model_key):
        return True

    with closing(get_connection()) as conn:
        df_sales = pd.read_sql(
            "SELECT product_id, date, sales FROM sales_aggregated WHERE product_id = ? ORDER BY date",
            conn,
            params=(product_id,),
        )

    if len(df_sales) < 5:
        return False

    df_sales["date"] = pd.to_datetime(df_sales["date"])
    df_sales, min_date = extract_time_features(df_sales)

    X_train = df_sales[FEATURE_COLUMNS]
    y_train = df_sales["sales"]

    model = xgb.XGBRegressor(**BEST_XGB_PARAMS)
    model.fit(X_train, y_train)

    payload = {
        "model": model,
        "min_date": min_date,
        "feature_cols": FEATURE_COLUMNS,
        "historical_sales_mean": float(df_sales["sales"].mean()),
        "historical_sales_std": float(df_sales["sales"].std() or 0.0),
        "trained_at": datetime.utcnow().isoformat(),
        "product_id": product_id,
    }

    model_bytes = pickle.dumps(payload)
    redis_client.set(model_key, model_bytes)

    os.makedirs(MODELS_PATH, exist_ok=True)
    with open(f"{MODELS_PATH}/procurement_model_{product_id}.pkl", "wb") as model_file:
        pickle.dump(payload, model_file)

    return True


def train_all_models() -> Dict:
    products = list_top_products()
    successful = 0
    failed = 0

    for product_id in products:
        try:
            if train_model_for_product(product_id):
                successful += 1
            else:
                failed += 1
        except Exception:
            failed += 1

    result = {
        "timestamp": datetime.utcnow().isoformat(),
        "total_products": len(products),
        "successful_trainings": successful,
        "failed_trainings": failed,
        "scope": "top_products",
        "products_limit": TOP_PRODUCTS_LIMIT,
    }

    producer.produce(
        "training_completed",
        key="procurement".encode("utf-8"),
        value=json.dumps({"source": "procurement_agent", **result}).encode("utf-8"),
    )
    producer.flush()
    return result


def load_model(product_id: str):
    model_bytes = redis_client.get(f"{MODEL_PREFIX}:{product_id}")
    if not model_bytes:
        return None
    return pickle.loads(model_bytes)


def monthly_forecast(product_id: str, months: int = 3) -> Dict:
    model_data = load_model(product_id)
    if not model_data:
        trained = train_model_for_product(product_id)
        if not trained:
            raise ValueError(f"Model not found for product {product_id}")
        model_data = load_model(product_id)
        if not model_data:
            raise ValueError(f"Model not found for product {product_id}")

    model = model_data["model"]
    min_date = model_data["min_date"]
    feature_cols = model_data.get("feature_cols", ["dayofweek", "month", "year", "day", "days_since_start"])
    historical_sales_mean = float(model_data.get("historical_sales_mean", 0.0))
    historical_sales_std = float(model_data.get("historical_sales_std", 0.0))

    start_date = datetime.utcnow().date() + timedelta(days=1)
    horizon_days = int(months * 30.5)
    future_dates = [start_date + timedelta(days=i) for i in range(horizon_days)]

    df_future = pd.DataFrame({"date": pd.to_datetime(future_dates)})
    df_future, _ = extract_time_features(df_future, is_future=True, min_date=min_date)

    for lag in [1, 7, 14, 30]:
        df_future[f"sales_lag_{lag}"] = historical_sales_mean

    for window in [7, 14, 30]:
        df_future[f"sales_rolling_mean_{window}"] = historical_sales_mean
        df_future[f"sales_rolling_std_{window}"] = historical_sales_std

    for col in feature_cols:
        if col not in df_future.columns:
            df_future[col] = 0

    X_future = df_future[feature_cols]
    preds = model.predict(X_future)
    preds = np.maximum(0, preds)

    df_future["pred"] = preds
    df_future["month_bucket"] = df_future["date"].dt.strftime("%Y-%m")

    monthly = (
        df_future.groupby("month_bucket", as_index=False)["pred"].sum().rename(columns={"pred": "forecast_demand"})
    )
    monthly["forecast_demand"] = monthly["forecast_demand"].round().astype(int)

    return {
        "product_id": product_id,
        "months": months,
        "generated_at": datetime.utcnow().isoformat(),
        "monthly_forecast": monthly.to_dict(orient="records"),
    }


def get_current_stock(product_id: str) -> int:
    with closing(get_connection()) as conn:
        stock_df = pd.read_sql(
            "SELECT current_stock FROM inventory WHERE product_id = ?",
            conn,
            params=(product_id,),
        )

    if stock_df.empty:
        return 0
    return int(stock_df.iloc[0]["current_stock"])


def ensure_product_initialized(product_id: str, default_stock: int = 30, default_price: float = 99.99) -> bool:
    with closing(get_connection()) as conn:
        current = pd.read_sql(
            "SELECT product_id FROM inventory WHERE product_id = ?",
            conn,
            params=(product_id,),
        )

        if not current.empty:
            return False

        conn.execute(
            "INSERT INTO inventory (product_id, current_stock, price) VALUES (?, ?, ?)",
            (product_id, int(default_stock), float(default_price)),
        )
        conn.commit()
    return True


def get_stock_and_price(product_id: str) -> tuple[int, float]:
    with closing(get_connection()) as conn:
        stock_df = pd.read_sql(
            "SELECT current_stock, price FROM inventory WHERE product_id = ?",
            conn,
            params=(product_id,),
        )

    if stock_df.empty:
        return 0, 99.99
    return int(stock_df.iloc[0]["current_stock"]), float(stock_df.iloc[0]["price"])


def classify_products_abc_xyz(product_ids: List[str]) -> Dict[str, Dict[str, float | str]]:
    if not product_ids:
        return {}

    placeholders = ",".join(["?"] * len(product_ids))
    with closing(get_connection()) as conn:
        sales_df = pd.read_sql(
            f"""
            SELECT product_id, date, sales, price
            FROM sales_aggregated
            WHERE product_id IN ({placeholders})
            """,
            conn,
            params=tuple(product_ids),
        )

    if sales_df.empty:
        return {
            pid: {
                "abc_class": "C",
                "xyz_class": "Z",
                "policy_code": "CZ",
                "annual_value": 0.0,
                "cv": 9.99,
                "safety_factor": XYZ_SAFETY_FACTORS["Z"],
            }
            for pid in product_ids
        }

    sales_df["price"] = sales_df["price"].fillna(99.99)
    sales_df["annual_value"] = sales_df["sales"] * sales_df["price"]

    value_df = (
        sales_df.groupby("product_id", as_index=False)["annual_value"]
        .sum()
        .sort_values("annual_value", ascending=False)
        .reset_index(drop=True)
    )

    total_value = float(value_df["annual_value"].sum() or 1.0)
    value_df["cum_share"] = value_df["annual_value"].cumsum() / total_value

    def abc_label(cum_share: float) -> str:
        if cum_share <= ABC_THRESHOLDS[0]:
            return "A"
        if cum_share <= ABC_THRESHOLDS[1]:
            return "B"
        return "C"

    value_df["abc_class"] = value_df["cum_share"].apply(abc_label)

    demand_stats = sales_df.groupby("product_id")["sales"].agg(["mean", "std"]).reset_index()
    demand_stats["cv"] = demand_stats.apply(
        lambda row: float(row["std"] / row["mean"]) if row["mean"] and row["mean"] > 0 else 9.99,
        axis=1,
    )

    def xyz_label(cv_value: float) -> str:
        if cv_value <= XYZ_CV_THRESHOLDS[0]:
            return "X"
        if cv_value <= XYZ_CV_THRESHOLDS[1]:
            return "Y"
        return "Z"

    demand_stats["xyz_class"] = demand_stats["cv"].apply(xyz_label)

    merged = value_df.merge(demand_stats[["product_id", "cv", "xyz_class"]], on="product_id", how="left")
    result: Dict[str, Dict[str, float | str]] = {}
    for _, row in merged.iterrows():
        abc = row["abc_class"]
        xyz = row["xyz_class"] if pd.notna(row["xyz_class"]) else "Z"
        result[str(row["product_id"])] = {
            "abc_class": abc,
            "xyz_class": xyz,
            "policy_code": f"{abc}{xyz}",
            "annual_value": float(row["annual_value"]),
            "cv": float(row["cv"] if pd.notna(row["cv"]) else 9.99),
            "safety_factor": float(XYZ_SAFETY_FACTORS.get(xyz, XYZ_SAFETY_FACTORS["Z"])),
        }

    for product_id in product_ids:
        if product_id not in result:
            result[product_id] = {
                "abc_class": "C",
                "xyz_class": "Z",
                "policy_code": "CZ",
                "annual_value": 0.0,
                "cv": 9.99,
                "safety_factor": XYZ_SAFETY_FACTORS["Z"],
            }

    return result


def calculate_rop_eoq(
    monthly_demand: int,
    stock_position: int,
    safety_factor: float,
    lead_time_days: int = DEFAULT_LEAD_TIME_DAYS,
    order_cost: float = DEFAULT_ORDER_COST,
    holding_cost: float = DEFAULT_HOLDING_COST,
) -> Dict[str, float]:
    daily_demand = float(monthly_demand) / 30.5
    safety_stock = daily_demand * lead_time_days * max(safety_factor, 0.1)
    rop_value = (daily_demand * lead_time_days) + safety_stock

    annual_demand = max(float(monthly_demand) * 12.0, 1.0)
    safe_holding_cost = max(float(holding_cost), 0.01)
    eoq_value = math.sqrt((2.0 * annual_demand * float(order_cost)) / safe_holding_cost)

    buy_qty = max(0.0, max(eoq_value, rop_value - float(stock_position)))
    return {
        "daily_demand": daily_demand,
        "safety_stock": safety_stock,
        "rop_value": rop_value,
        "eoq_value": eoq_value,
        "recommended_buy_qty": buy_qty,
    }


def publish_prediction_error_alert(product_id: str, reason: str, stockout_rate: float):
    alert = {
        "event_id": str(uuid4()),
        "timestamp": datetime.utcnow().isoformat(),
        "source_agent": "procurement_agent",
        "product_id": product_id,
        "reason": reason,
        "stockout_rate": round(float(stockout_rate), 4),
    }
    producer.produce(
        "prediction_error_alert",
        key=product_id.encode("utf-8"),
        value=json.dumps(alert).encode("utf-8"),
    )
    producer.poll(0)


def build_report_for_horizon(months: int) -> Dict:
    products = list_top_products()
    classifications = classify_products_abc_xyz(products)
    rows = []

    for product_id in products:
        try:
            forecast = monthly_forecast(product_id, months=months)
        except ValueError:
            continue

        current_stock, current_price = get_stock_and_price(product_id)
        projected_stock = current_stock
        class_info = classifications.get(
            product_id,
            {
                "abc_class": "C",
                "xyz_class": "Z",
                "policy_code": "CZ",
                "safety_factor": XYZ_SAFETY_FACTORS["Z"],
                "cv": 9.99,
            },
        )
        alert_sent = False

        for month_data in forecast["monthly_forecast"][:months]:
            demand = int(month_data["forecast_demand"])
            controls = calculate_rop_eoq(
                monthly_demand=demand,
                stock_position=projected_stock,
                safety_factor=float(class_info["safety_factor"]),
            )
            buy_qty = int(round(controls["recommended_buy_qty"]))
            projected_stock = max(0, projected_stock - demand) + buy_qty

            if (
                not alert_sent
                and class_info["xyz_class"] == "X"
                and projected_stock < controls["safety_stock"]
            ):
                stockout_rate = 1.0 - (projected_stock / max(controls["safety_stock"], 1.0))
                publish_prediction_error_alert(
                    product_id=product_id,
                    reason="projected_stock_below_safety_stock_for_X_class",
                    stockout_rate=max(0.0, stockout_rate),
                )
                alert_sent = True

            rows.append(
                {
                    "product_id": product_id,
                    "month": month_data["month_bucket"],
                    "forecast_demand": demand,
                    "current_stock": current_stock,
                    "current_price": round(float(current_price), 2),
                    "abc_class": class_info["abc_class"],
                    "xyz_class": class_info["xyz_class"],
                    "policy_code": class_info["policy_code"],
                    "demand_cv": round(float(class_info["cv"]), 4),
                    "safety_stock": round(float(controls["safety_stock"]), 2),
                    "rop_value": round(float(controls["rop_value"]), 2),
                    "eoq_value": round(float(controls["eoq_value"]), 2),
                    "recommended_buy_qty": int(buy_qty),
                    "model_version": "xgboost_v1",
                    "generated_at": forecast["generated_at"],
                }
            )

    return {
        "horizon_months": months,
        "generated_at": datetime.utcnow().isoformat(),
        "scope": "top_products",
        "products_limit": TOP_PRODUCTS_LIMIT,
        "rows": rows,
    }


def build_full_report(months_options: List[int] | None = None) -> Dict:
    horizons = sorted(list(set(months_options or [3, 6, 12])))
    reports = [build_report_for_horizon(months=horizon) for horizon in horizons]
    payload = {
        "agent": "procurement",
        "generated_at": datetime.utcnow().isoformat(),
        "scope": "top_products",
        "products_limit": TOP_PRODUCTS_LIMIT,
        "reports": reports,
    }

    redis_client.set(REPORT_KEY, json.dumps(payload).encode("utf-8"))

    producer.produce(
        "procurement_reports",
        key="latest".encode("utf-8"),
        value=json.dumps(payload).encode("utf-8"),
    )
    producer.flush()

    return payload


def run_background_cycle(reason: str):
    lock_acquired = redis_client.set(LOCK_KEY, reason.encode("utf-8"), nx=True, ex=600)
    if not lock_acquired:
        return

    try:
        train_all_models()
        build_full_report([3, 6, 12])
    finally:
        redis_client.delete(LOCK_KEY)


def kafka_consumer_loop():
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            continue

        try:
            payload = json.loads(msg.value().decode("utf-8"))
        except Exception:
            continue

        event_type = payload.get("event_type")
        payload_data = payload.get("data", {}) if isinstance(payload.get("data"), dict) else {}
        product_id = payload_data.get("product_id") or payload.get("product_id")

        if product_id:
            ensure_product_initialized(product_id)

        if event_type in ["SalesCreated", "InventoryUpdated", "ProductCreated", "PredictionErrorAlert"]:
            run_background_cycle(reason=f"kafka:{event_type}")


@app.on_event("startup")
def on_startup():
    scheduler.add_job(
        lambda: run_background_cycle(reason="scheduled"),
        trigger="interval",
        minutes=SCHEDULE_MINUTES,
        id="procurement-scheduled-job",
        replace_existing=True,
    )
    scheduler.start()

    kafka_thread = threading.Thread(target=kafka_consumer_loop, daemon=True)
    kafka_thread.start()

    startup_thread = threading.Thread(
        target=lambda: run_background_cycle(reason="startup"),
        daemon=True,
    )
    startup_thread.start()


@app.on_event("shutdown")
def on_shutdown():
    try:
        scheduler.shutdown(wait=False)
    except Exception:
        pass
    consumer.close()
    producer.flush()


@app.get("/health")
def health():
    return {
        "status": "healthy",
        "agent": "procurement",
        "schedule_minutes": SCHEDULE_MINUTES,
        "products_limit": TOP_PRODUCTS_LIMIT,
    }


@app.post("/train")
def train_endpoint():
    thread = threading.Thread(target=lambda: run_background_cycle(reason="manual_train"), daemon=True)
    thread.start()
    return {
        "status": "accepted",
        "message": "Training and report generation started in background.",
    }


@app.post("/forecast")
def forecast_endpoint(request: ForecastRequest):
    try:
        return monthly_forecast(request.product_id, request.months)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.post("/report")
def report_endpoint(request: ReportRequest):
    months_options = request.months_options

    thread = threading.Thread(
        target=lambda: build_full_report(months_options),
        daemon=True,
    )
    thread.start()

    return {
        "status": "accepted",
        "message": "Report generation started in background.",
        "months_options": months_options,
    }


@app.get("/report/latest")
def report_latest_endpoint():
    raw = redis_client.get(REPORT_KEY)
    if not raw:
        thread = threading.Thread(
            target=lambda: run_background_cycle(reason="report_latest_on_demand"),
            daemon=True,
        )
        thread.start()
        raise HTTPException(status_code=202, detail="Report generation in progress")
    return json.loads(raw.decode("utf-8"))


@app.post("/ask")
def ask_endpoint(request: AskRequest):
    question = request.question.lower()

    if request.product_id:
        try:
            forecast = monthly_forecast(request.product_id, months=3)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

        total_3m = sum(item["forecast_demand"] for item in forecast["monthly_forecast"][:3])
        return {
            "question": request.question,
            "answer": f"Forecasted demand for product {request.product_id} in the next 3 months is {int(total_3m)} units.",
            "context": forecast,
        }

    if "top" in question or "highest" in question:
        latest = build_report_for_horizon(3)
        totals = {}
        for row in latest["rows"]:
            product_id = row["product_id"]
            totals[product_id] = totals.get(product_id, 0) + row["forecast_demand"]

        if not totals:
            return {"question": request.question, "answer": "No demand data available."}

        top_product = max(totals, key=totals.get)
        return {
            "question": request.question,
            "answer": f"Product with highest expected 3-month demand is {top_product} with {int(totals[top_product])} units.",
        }

    return {
        "question": request.question,
        "answer": "Ask about a specific product with product_id, or ask for highest/top demand products.",
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8004)
