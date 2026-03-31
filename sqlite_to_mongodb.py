#!/usr/bin/env python3
"""Migrate selected tables from SQLite to MongoDB.

Source tables (SQLite):
- sales_aggregated(product_id, date, sales, revenue, price, stock)
- inventory(product_id, current_stock, price)

Target collections (MongoDB):
- sales_aggregated
- inventory
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

try:
    from pymongo import MongoClient, UpdateOne
except ImportError as exc:
    raise SystemExit(
        "Missing dependency: pymongo\n"
        "Install with: pip install pymongo"
    ) from exc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Migrate SQLite tables to MongoDB")
    parser.add_argument(
        "--sqlite-path",
        default="ecommerce.db",
        help="Path to SQLite database file (default: ecommerce.db)",
    )
    parser.add_argument(
        "--mongo-uri",
        default="mongodb://localhost:27017",
        help="MongoDB connection URI (default: mongodb://localhost:27017)",
    )
    parser.add_argument(
        "--mongo-db",
        default="ecommerce",
        help="MongoDB database name (default: ecommerce)",
    )
    parser.add_argument(
        "--drop-existing",
        action="store_true",
        help="Drop target collections before import",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Bulk upsert batch size (default: 1000)",
    )
    return parser.parse_args()


def ensure_sqlite_tables(conn: sqlite3.Connection, required_tables: Iterable[str]) -> None:
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    existing = {row[0] for row in cur.fetchall()}

    missing = [name for name in required_tables if name not in existing]
    if missing:
        raise RuntimeError(f"Missing required SQLite table(s): {', '.join(missing)}")


def parse_date(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        return value

    # Accept ISO-like text date/time; fallback to original string.
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return value


def migrate_sales_aggregated(
    conn: sqlite3.Connection,
    mongo_db: Any,
    batch_size: int,
) -> int:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT product_id, date, sales, revenue, price, stock
        FROM sales_aggregated
        """
    )

    collection = mongo_db["sales_aggregated"]
    collection.create_index([("product_id", 1), ("date", 1)], unique=True)

    total = 0
    ops = []

    for product_id, date_txt, sales, revenue, price, stock in cur:
        doc = {
            "product_id": product_id,
            "date": parse_date(date_txt),
            "sales": float(sales) if sales is not None else None,
            "revenue": float(revenue) if revenue is not None else None,
            "price": float(price) if price is not None else None,
            "stock": float(stock) if stock is not None else None,
        }

        ops.append(
            UpdateOne(
                {"product_id": doc["product_id"], "date": doc["date"]},
                {"$set": doc},
                upsert=True,
            )
        )

        if len(ops) >= batch_size:
            collection.bulk_write(ops, ordered=False)
            total += len(ops)
            ops.clear()

    if ops:
        collection.bulk_write(ops, ordered=False)
        total += len(ops)

    return total


def migrate_inventory(conn: sqlite3.Connection, mongo_db: Any, batch_size: int) -> int:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT product_id, current_stock, price
        FROM inventory
        """
    )

    collection = mongo_db["inventory"]
    collection.create_index([("product_id", 1)], unique=True)

    total = 0
    ops = []

    for product_id, current_stock, price in cur:
        doc = {
            "product_id": product_id,
            "current_stock": int(current_stock) if current_stock is not None else 0,
            "price": float(price) if price is not None else None,
        }

        ops.append(
            UpdateOne(
                {"product_id": doc["product_id"]},
                {"$set": doc},
                upsert=True,
            )
        )

        if len(ops) >= batch_size:
            collection.bulk_write(ops, ordered=False)
            total += len(ops)
            ops.clear()

    if ops:
        collection.bulk_write(ops, ordered=False)
        total += len(ops)

    return total


def main() -> int:
    args = parse_args()

    sqlite_path = Path(args.sqlite_path)
    if not sqlite_path.exists():
        print(f"SQLite file not found: {sqlite_path}", file=sys.stderr)
        return 1

    conn = sqlite3.connect(str(sqlite_path))
    try:
        ensure_sqlite_tables(conn, ["sales_aggregated", "inventory"])

        client = MongoClient(args.mongo_uri)
        mongo_db = client[args.mongo_db]

        if args.drop_existing:
            mongo_db["sales_aggregated"].drop()
            mongo_db["inventory"].drop()

        sales_count = migrate_sales_aggregated(conn, mongo_db, args.batch_size)
        inventory_count = migrate_inventory(conn, mongo_db, args.batch_size)

        print("Migration completed successfully.")
        print(f"MongoDB database: {args.mongo_db}")
        print(f"sales_aggregated upserts: {sales_count}")
        print(f"inventory upserts: {inventory_count}")
        return 0
    except Exception as exc:  # noqa: BLE001
        print(f"Migration failed: {exc}", file=sys.stderr)
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
