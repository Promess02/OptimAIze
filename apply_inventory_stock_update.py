#!/usr/bin/env python3
"""Apply the inventory stock adjustment used by the inventory analysis notebook.

The script updates `inventory.current_stock` using the same demand anchor and
reorder-point logic as `agents/inventory/inventory_abc_xyz_rop_eoq.ipynb`.

By default it runs in dry-run mode and rolls back the transaction after reporting
how many rows would change. Pass `--commit` to persist the update.
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


DEFAULT_DB_PATH = Path(__file__).resolve().with_name("ecommerce.db")

SQL_UPDATE = """
WITH anchor AS (
  SELECT MAX(date) AS anchor_date
  FROM sales_aggregated
  WHERE sales > 0
), recent AS (
  SELECT
    product_id,
    COUNT(*) AS n_days,
    AVG(sales) AS avg_daily_demand,
    AVG(sales * sales) AS mean_sq
  FROM sales_aggregated
  WHERE date BETWEEN date((SELECT anchor_date FROM anchor), '-29 days')
                 AND (SELECT anchor_date FROM anchor)
  GROUP BY product_id
), targets AS (
  SELECT
    i.product_id,
    i.current_stock,
    CAST(
      ROUND(
        CASE
          WHEN r.product_id IS NULL THEN i.current_stock
          ELSE MAX(
            i.current_stock,
            1.15 * (
              (r.avg_daily_demand * 7.0) +
              1.645 * SQRT(
                CASE
                  WHEN r.n_days > 1
                  THEN MAX(
                    (r.mean_sq - (r.avg_daily_demand * r.avg_daily_demand)) * r.n_days / (r.n_days - 1),
                    0
                  )
                  ELSE 0
                END
              ) * SQRT(7.0)
            )
          )
        END
      ) AS INTEGER
    ) AS new_stock
  FROM inventory i
  LEFT JOIN recent r USING (product_id)
)
UPDATE inventory
SET current_stock = (
  SELECT new_stock
  FROM targets
  WHERE targets.product_id = inventory.product_id
)
WHERE product_id IN (
  SELECT product_id
  FROM targets
  WHERE new_stock > current_stock
);
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the inventory stock adjustment SQL against ecommerce.db."
    )
    parser.add_argument(
        "--db-path",
        default=str(DEFAULT_DB_PATH),
        help=f"Path to the SQLite database (default: {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--commit",
        action="store_true",
        help="Persist the update. Without this flag the script rolls back.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    db_path = Path(args.db_path)

    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    conn = sqlite3.connect(db_path)
    try:
        conn.execute("BEGIN")
        conn.executescript(SQL_UPDATE)
        changed_rows = conn.execute("SELECT changes()").fetchone()[0]

        if args.commit:
            conn.commit()
            print(f"Committed update for {changed_rows} inventory rows.")
        else:
            conn.rollback()
            print(f"Dry run only. {changed_rows} inventory rows would be updated.")
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())