#!/usr/bin/env python3
"""Initialize SQLite database under data/ecommerce.db."""

from __future__ import annotations

import os
import sqlite3
import sys
from pathlib import Path

import pandas as pd

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from shared.paths import DB_PATH, DATA_DIR, ensure_data_dir  # noqa: E402

ensure_data_dir()
db_file = str(DB_PATH)
sales_csv = DATA_DIR / "sales.csv"

if os.path.exists(db_file):
    print(f"'{db_file}' already exists. Skipping database creation.")
    sys.exit(0)

if sales_csv.is_file():
    df_sales = pd.read_csv(
        sales_csv,
        usecols=["product_id", "date", "sales", "revenue", "price", "stock"],
    )
else:
    print(f"{sales_csv} not found. Creating sample data...")
    import numpy as np
    from datetime import datetime, timedelta

    np.random.seed(42)
    dates = [datetime.now() - timedelta(days=x) for x in range(90, 0, -1)]
    products = [f"PROD{str(i).zfill(3)}" for i in range(1, 11)]

    data = []
    for date in dates:
        for product in products:
            data.append(
                {
                    "product_id": product,
                    "date": date.strftime("%Y-%m-%d"),
                    "sales": np.random.randint(10, 100),
                    "revenue": np.random.uniform(500, 5000),
                    "price": np.random.uniform(50, 200),
                    "stock": np.random.randint(100, 1000),
                }
            )

    df_sales = pd.DataFrame(data)
    print(f"Created sample data with {len(df_sales)} records")

df_sales["date"] = pd.to_datetime(df_sales["date"])

agg_sales = (
    df_sales.groupby(["product_id", "date"])
    .agg({"sales": "sum", "revenue": "sum", "price": "mean", "stock": "sum"})
    .reset_index()
)

if os.path.exists(db_file) and not os.access(db_file, os.W_OK):
    raise PermissionError(
        f"Cannot write to '{db_file}'. It may be owned by root. "
        f"Run 'sudo chown $USER:$USER {db_file}' to fix this."
    )

conn = sqlite3.connect(db_file, timeout=30.0)
cursor = conn.cursor()

agg_sales.to_sql("sales_aggregated", conn, if_exists="replace", index=False)
print("✓ Dane sprzedaży zostały zagregowane i zapisane w tabeli 'sales_aggregated'.")

inventory_df = (
    df_sales.sort_values("date")
    .groupby("product_id")
    .agg({"stock": "last", "price": "mean"})
    .reset_index()
)
inventory_df.rename(columns={"stock": "current_stock", "price": "price"}, inplace=True)
inventory_df["current_stock"] = inventory_df["current_stock"].fillna(0).astype(int)
inventory_df["price"] = inventory_df["price"].fillna(99.99).astype(float).round(2)

inventory_df.to_sql("inventory", conn, if_exists="replace", index=False)
print("✓ Stany magazynowe i ceny zostały zapisane w tabeli 'inventory'.")

print("\n" + "=" * 60)
print("DATABASE SCHEMA VERIFICATION")
print("=" * 60)

cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()
print(f"\nTables created: {[t[0] for t in tables]}")

print("\nInventory table structure:")
cursor.execute("PRAGMA table_info(inventory);")
for col in cursor.fetchall():
    print(f"  - {col[1]} ({col[2]})")

print("\nSample inventory data:")
print(pd.read_sql("SELECT * FROM inventory LIMIT 3", conn).to_string())

print("\nSales_aggregated table structure:")
cursor.execute("PRAGMA table_info(sales_aggregated);")
for col in cursor.fetchall():
    print(f"  - {col[1]} ({col[2]})")

print("\n" + "=" * 60)
print("✓ Database initialized successfully!")
print(f"  Path: {db_file}")
print(f"  Total products: {len(inventory_df)}")
print(f"  Total sales records: {len(agg_sales)}")
print("=" * 60 + "\n")

conn.close()
