"""
Playground API — proxy do agentów systemu wieloagentowego.
Uruchom: uvicorn server:app --reload --host 0.0.0.0 --port 8090
"""

from __future__ import annotations

import os
import sqlite3
import sys
import time
from contextlib import closing
from pathlib import Path
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from shared.paths import DB_PATH as DEFAULT_DB_PATH  # noqa: E402

try:
    from jobs import job_manager, run_tests_job, run_train_job  # type: ignore
except ImportError:
    from playground.jobs import job_manager, run_tests_job, run_train_job  # noqa: E402

STATIC_DIR = Path(__file__).resolve().parent / "static"

DEMAND_URL = os.getenv("PLAYGROUND_DEMAND_URL", "http://localhost:8001").rstrip("/")
INVENTORY_URL = os.getenv("PLAYGROUND_INVENTORY_URL", "http://localhost:8002").rstrip("/")
PROCUREMENT_URL = os.getenv("PLAYGROUND_PROCUREMENT_URL", "http://localhost:8004").rstrip("/")
DB_PATH = os.getenv("PLAYGROUND_DB_PATH", str(DEFAULT_DB_PATH))

REPORT_POLL_SECONDS = int(os.getenv("PLAYGROUND_REPORT_POLL_SECONDS", "120"))
REPORT_POLL_INTERVAL = float(os.getenv("PLAYGROUND_REPORT_POLL_INTERVAL", "2"))

app = FastAPI(title="OptimAIze Playground", version="1.0.0")


class OrderProposalRequest(BaseModel):
    product_id: str
    horizon_days: int = Field(..., ge=1, le=365)


class BatchOrderRequest(BaseModel):
    horizon_days: int = Field(..., ge=1, le=365)
    product_ids: list[str] | None = None


def _http_client() -> httpx.Client:
    return httpx.Client(timeout=5.0)


def _fetch_report_latest(client: httpx.Client) -> dict[str, Any] | None:
    try:
        response = client.get(f"{PROCUREMENT_URL}/report/latest")
    except httpx.HTTPError:
        return None
    if response.status_code == 200:
        return response.json()
    return None


def _procurement_reachable(client: httpx.Client) -> bool:
    try:
        response = client.get(f"{PROCUREMENT_URL}/health")
        return response.status_code == 200
    except httpx.HTTPError:
        return False


def _ensure_report(client: httpx.Client) -> dict[str, Any]:
    latest = _fetch_report_latest(client)
    if latest:
        return latest

    if not _procurement_reachable(client):
        raise HTTPException(
            status_code=503,
            detail="Agent procurement niedostępny — używany będzie fallback SQLite.",
        )

    try:
        client.post(
            f"{PROCUREMENT_URL}/report",
            json={"months_options": [1, 2, 3]},
        )
    except httpx.HTTPError as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Nie udało się zlecić raportu procurement: {exc}",
        ) from exc

    deadline = time.time() + REPORT_POLL_SECONDS
    while time.time() < deadline:
        latest = _fetch_report_latest(client)
        if latest:
            return latest
        time.sleep(REPORT_POLL_INTERVAL)

    raise HTTPException(
        status_code=503,
        detail="Raport produktów nie jest jeszcze gotowy. Upewnij się, że agent procurement (port 8004) działa.",
    )


def _products_from_sqlite(limit: int = 10) -> list[dict[str, Any]]:
    db = Path(DB_PATH)
    if not db.is_file():
        return []

    with closing(sqlite3.connect(db)) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT s.product_id,
                   SUM(s.sales) AS total_sales,
                   COALESCE(i.current_stock, 0) AS current_stock,
                   COALESCE(i.price, 0) AS current_price
            FROM sales_aggregated s
            LEFT JOIN inventory i ON i.product_id = s.product_id
            GROUP BY s.product_id
            ORDER BY total_sales DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

    return [
        {
            "product_id": str(row["product_id"]),
            "current_stock": int(row["current_stock"] or 0),
            "current_price": round(float(row["current_price"] or 0), 2),
            "abc_class": None,
            "xyz_class": None,
            "policy_code": None,
            "forecast_demand_1m": None,
            "recommended_buy_qty": None,
            "needs_order": None,
            "source": "sqlite",
        }
        for row in rows
    ]


def _aggregate_products_from_report(report: dict[str, Any]) -> list[dict[str, Any]]:
    """Agreguje wiersze raportu procurement do jednego wpisu na produkt."""
    by_product: dict[str, dict[str, Any]] = {}

    for block in report.get("reports", []):
        horizon = int(block.get("horizon_months", 0))
        for row in block.get("rows", []):
            pid = str(row["product_id"])
            entry = by_product.setdefault(
                pid,
                {
                    "product_id": pid,
                    "current_stock": int(row.get("current_stock", 0)),
                    "current_price": round(float(row.get("current_price", 0)), 2),
                    "abc_class": row.get("abc_class"),
                    "xyz_class": row.get("xyz_class"),
                    "policy_code": row.get("policy_code"),
                    "forecast_demand_1m": None,
                    "recommended_buy_qty": 0,
                    "needs_order": False,
                    "source": "procurement_report",
                },
            )
            buy_qty = int(row.get("recommended_buy_qty", 0))
            demand = int(row.get("forecast_demand", 0))

            if horizon == 1:
                entry["forecast_demand_1m"] = demand
                entry["recommended_buy_qty"] = max(entry["recommended_buy_qty"], buy_qty)
            elif entry["forecast_demand_1m"] is None and horizon <= 3:
                entry["forecast_demand_1m"] = demand

            entry["recommended_buy_qty"] = max(entry["recommended_buy_qty"], buy_qty)
            entry["needs_order"] = entry["recommended_buy_qty"] > 0

    products = list(by_product.values())
    products.sort(key=lambda p: (not p["needs_order"], -p["recommended_buy_qty"], p["product_id"]))
    return products


@app.get("/api/health")
def api_health():
    agents = {
        "demand": DEMAND_URL,
        "inventory": INVENTORY_URL,
        "procurement": PROCUREMENT_URL,
    }
    status: dict[str, Any] = {"playground": "ok", "agents": {}}

    with _http_client() as client:
        for name, base in agents.items():
            try:
                response = client.get(f"{base}/health")
                status["agents"][name] = {
                    "url": base,
                    "reachable": response.status_code == 200,
                    "body": response.json() if response.status_code == 200 else None,
                }
            except httpx.HTTPError as exc:
                status["agents"][name] = {"url": base, "reachable": False, "error": str(exc)}

    status["all_healthy"] = all(
        agent.get("reachable") for agent in status["agents"].values()
    )
    return status


def _load_monitored_products() -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    products: list[dict[str, Any]] = []
    report_meta: dict[str, Any] | None = None

    try:
        with _http_client() as client:
            report = _ensure_report(client)
            products = _aggregate_products_from_report(report)
            report_meta = {
                "generated_at": report.get("generated_at"),
                "scope": report.get("scope"),
                "products_limit": report.get("products_limit"),
            }
    except (HTTPException, httpx.HTTPError):
        products = _products_from_sqlite()
        if not products:
            raise HTTPException(
                status_code=503,
                detail="Brak raportu procurement i brak produktów w data/ecommerce.db.",
            )
        report_meta = {"source": "sqlite_fallback", "db_path": DB_PATH}

    return products, report_meta


def _order_line_for_product(
    client: httpx.Client,
    product_id: str,
    horizon_days: int,
) -> dict[str, Any]:
    try:
        demand_resp = client.post(
            f"{DEMAND_URL}/predict",
            json={"product_id": product_id, "horizon_days": horizon_days},
        )
        if demand_resp.status_code != 200:
            return {
                "product_id": product_id,
                "error": demand_resp.text or "Błąd prognozy popytu",
                "order_quantity": 0,
            }

        demand = demand_resp.json()
        predicted = int(demand.get("predicted_demand", 0))

        inventory_resp = client.post(
            f"{INVENTORY_URL}/order",
            json={"product_id": product_id, "predicted_demand": predicted},
        )
        if inventory_resp.status_code != 200:
            return {
                "product_id": product_id,
                "error": inventory_resp.text or "Błąd generowania zamówienia",
                "order_quantity": 0,
            }

        order = inventory_resp.json()
        order_quantity = int(order.get("order_quantity", 0))
        return {
            "product_id": product_id,
            "predicted_demand": predicted,
            "current_stock": int(order.get("current_stock", 0)),
            "order_quantity": order_quantity,
            "projected_stock": int(order.get("projected_stock", 0)),
            "error": None,
        }
    except httpx.HTTPError as exc:
        return {
            "product_id": product_id,
            "error": str(exc),
            "order_quantity": 0,
        }


@app.get("/api/products")
def api_products():
    products, report_meta = _load_monitored_products()
    return {
        "products": products,
        "count": len(products),
        "report": report_meta,
    }


def _validate_horizon(horizon_days: int) -> None:
    if horizon_days not in (30, 60):
        raise HTTPException(
            status_code=400,
            detail="Obsługiwane horyzonty: 30 lub 60 dni.",
        )


@app.post("/api/order-proposal")
def api_order_proposal(body: OrderProposalRequest):
    _validate_horizon(body.horizon_days)

    with _http_client() as client:
        line = _order_line_for_product(client, body.product_id, body.horizon_days)

    if line.get("error"):
        raise HTTPException(status_code=502, detail=line["error"])

    order_quantity = int(line["order_quantity"])
    return {
        "product_id": body.product_id,
        "horizon_days": body.horizon_days,
        "summary": {
            "predicted_demand": line["predicted_demand"],
            "current_stock": line["current_stock"],
            "order_quantity": order_quantity,
            "projected_stock": line["projected_stock"],
            "recommendation": (
                f"Zamów {order_quantity} szt. na najbliższe {body.horizon_days} dni."
                if order_quantity > 0
                else f"Brak zamówienia — stan magazynowy wystarcza na {body.horizon_days} dni."
            ),
        },
        "lines": [line],
    }


@app.post("/api/orders-batch")
def api_orders_batch(body: BatchOrderRequest):
    _validate_horizon(body.horizon_days)

    if body.product_ids:
        product_ids = body.product_ids
    else:
        products, _ = _load_monitored_products()
        product_ids = [str(p["product_id"]) for p in products]

    if not product_ids:
        raise HTTPException(status_code=404, detail="Brak produktów do zamówienia.")

    lines: list[dict[str, Any]] = []
    with _http_client() as client:
        for product_id in product_ids:
            lines.append(_order_line_for_product(client, product_id, body.horizon_days))

    to_order = [line for line in lines if not line.get("error") and int(line.get("order_quantity", 0)) > 0]
    total_units = sum(int(line["order_quantity"]) for line in to_order)
    errors = [line for line in lines if line.get("error")]

    return {
        "horizon_days": body.horizon_days,
        "lines": lines,
        "summary": {
            "products_checked": len(lines),
            "products_to_order": len(to_order),
            "total_units": total_units,
            "errors_count": len(errors),
        },
    }


@app.get("/api/jobs")
def api_jobs_list():
    return {"jobs": job_manager.list_recent()}


@app.get("/api/jobs/{job_id}")
def api_job_status(job_id: str):
    job = job_manager.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Nie znaleziono zadania")
    return job.to_dict()


@app.post("/api/jobs/tests")
def api_start_tests():
    try:
        job = job_manager.start("tests", run_tests_job)
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return job.to_dict()


@app.post("/api/jobs/train")
def api_start_train():
    try:
        job = job_manager.start("train", run_train_job)
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return job.to_dict()


@app.get("/")
def index():
    return FileResponse(STATIC_DIR / "index.html")


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
