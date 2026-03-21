#!/usr/bin/env python3
"""
Train demand models only for TOP N products by historical sales.
Stores models in Redis under keys model:{product_id} for Demand Agent usage.
"""

import os
import pickle
import sqlite3
from datetime import datetime

import pandas as pd
import redis
import xgboost as xgb

DB_PATH = os.getenv("DB_PATH", "/home/miko/magister/ecommerce.db")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6380"))
TOP_PRODUCTS_LIMIT = int(os.getenv("TOP_PRODUCTS_LIMIT", "10"))
FORCE_RETRAIN = os.getenv("FORCE_RETRAIN", "0") == "1"

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


def extract_time_features(df: pd.DataFrame):
    df = df.copy()
    df["dayofweek"] = df["date"].dt.dayofweek
    df["month"] = df["date"].dt.month
    df["day"] = df["date"].dt.day
    df["quarter"] = df["date"].dt.quarter
    df["year"] = df["date"].dt.year
    df["day_of_year"] = df["date"].dt.dayofyear
    df["is_month_end"] = df["date"].dt.is_month_end.astype(int)
    df["is_quarter_end"] = df["date"].dt.is_quarter_end.astype(int)
    min_date = df["date"].min()
    df["days_since_start"] = (df["date"] - min_date).dt.days

    for lag in [1, 7, 14, 30]:
        df[f"sales_lag_{lag}"] = df["sales"].shift(lag)

    for window in [7, 14, 30]:
        df[f"sales_rolling_mean_{window}"] = df["sales"].rolling(window=window).mean()
        df[f"sales_rolling_std_{window}"] = df["sales"].rolling(window=window).std()

    df = df.bfill().ffill()
    df = df.fillna(df.mean(numeric_only=True))
    return df, min_date


def list_top_products(conn: sqlite3.Connection, limit: int):
    query = """
        SELECT product_id, SUM(sales) AS total_sales
        FROM sales_aggregated
        GROUP BY product_id
        ORDER BY total_sales DESC
        LIMIT ?
    """
    df = pd.read_sql(query, conn, params=(int(limit),))
    return df["product_id"].astype(str).tolist()


def train_model_for_product(conn: sqlite3.Connection, redis_client: redis.Redis, product_id: str) -> bool:
    model_key = f"model:{product_id}"
    if not FORCE_RETRAIN and redis_client.exists(model_key):
        return True

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
    redis_client.set(model_key, pickle.dumps(payload))
    return True


def main():
    conn = sqlite3.connect(DB_PATH)
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=False)

    products = list_top_products(conn, TOP_PRODUCTS_LIMIT)
    successful = 0
    reused = 0

    total = len(products)
    print(f"Starting demand model preparation for top {total} products...")

    for idx, product_id in enumerate(products, start=1):
        model_key = f"model:{product_id}"
        was_cached = redis_client.exists(model_key) and not FORCE_RETRAIN
        if train_model_for_product(conn, redis_client, product_id):
            successful += 1
            if was_cached:
                reused += 1
        print(f"PROGRESS:{idx}/{total}")

    conn.close()

    print(
        {
            "status": "completed",
            "scope": "top_products",
            "products_limit": TOP_PRODUCTS_LIMIT,
            "total_products": len(products),
            "successful_trainings": successful,
            "reused_models": reused,
            "force_retrain": FORCE_RETRAIN,
        }
    )


if __name__ == "__main__":
    main()
