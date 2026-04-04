#!/usr/bin/env python3
"""Generate a larger synthetic ecommerce SQLite database using CTGAN and TVAE.

The script reads `ecommerce.db`, trains two popular tabular synthesizers (CTGAN, TVAE),
selects the better synthetic candidate by distribution similarity, enforces business
constraints, and writes `ecommerce_generated.db`.
"""

from __future__ import annotations

import argparse
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import numpy as np
import pandas as pd

try:
    from sdv.metadata import SingleTableMetadata
    from sdv.single_table import CTGANSynthesizer, TVAESynthesizer
except ImportError as exc:
    raise SystemExit(
        "Missing dependency: sdv. Install with `pip install sdv` and rerun."
    ) from exc


REQUIRED_SALES_COLS = ["product_id", "date", "sales", "revenue", "price", "stock"]
REQUIRED_INVENTORY_COLS = ["product_id", "current_stock", "price"]


@dataclass
class GenConfig:
    source_db: Path
    output_db: Path
    product_multiplier: float
    sales_multiplier: float
    seed: int
    epochs: int
    max_train_rows: int
    min_records_per_product: int


def parse_args() -> GenConfig:
    parser = argparse.ArgumentParser(description="Generate synthetic ecommerce database")
    parser.add_argument("--source-db", default="ecommerce.db", help="Path to source SQLite DB")
    parser.add_argument("--output-db", default="ecommerce_generated.db", help="Path to output SQLite DB")
    parser.add_argument("--product-multiplier", type=float, default=2.0, help="Scale for product count")
    parser.add_argument("--sales-multiplier", type=float, default=3.0, help="Scale for sales row count")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--epochs", type=int, default=30, help="Epochs for each synthesizer")
    parser.add_argument(
        "--max-train-rows",
        type=int,
        default=120_000,
        help="Max rows sampled from source sales table for model training",
    )
    parser.add_argument(
        "--min-records-per-product",
        type=int,
        default=20,
        help="Minimum generated sales rows for each synthetic product",
    )
    args = parser.parse_args()

    return GenConfig(
        source_db=Path(args.source_db),
        output_db=Path(args.output_db),
        product_multiplier=args.product_multiplier,
        sales_multiplier=args.sales_multiplier,
        seed=args.seed,
        epochs=args.epochs,
        max_train_rows=args.max_train_rows,
        min_records_per_product=args.min_records_per_product,
    )


def load_source_data(source_db: Path) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if not source_db.exists():
        raise FileNotFoundError(f"Source DB not found: {source_db}")

    conn = sqlite3.connect(source_db)
    try:
        sales_df = pd.read_sql("SELECT * FROM sales_aggregated", conn)
        inventory_df = pd.read_sql("SELECT * FROM inventory", conn)
    finally:
        conn.close()

    missing_sales = [c for c in REQUIRED_SALES_COLS if c not in sales_df.columns]
    missing_inventory = [c for c in REQUIRED_INVENTORY_COLS if c not in inventory_df.columns]
    if missing_sales:
        raise ValueError(f"sales_aggregated missing columns: {missing_sales}")
    if missing_inventory:
        raise ValueError(f"inventory missing columns: {missing_inventory}")

    sales_df = sales_df[REQUIRED_SALES_COLS].copy()
    inventory_df = inventory_df[REQUIRED_INVENTORY_COLS].copy()

    sales_df["date"] = pd.to_datetime(sales_df["date"], errors="coerce")
    if sales_df["date"].isna().all():
        raise ValueError("Could not parse source dates in sales_aggregated.date")

    return sales_df, inventory_df


def similarity_score(real_df: pd.DataFrame, synth_df: pd.DataFrame, cols: Iterable[str]) -> float:
    score = 0.0
    for col in cols:
        real = pd.to_numeric(real_df[col], errors="coerce").dropna()
        synth = pd.to_numeric(synth_df[col], errors="coerce").dropna()
        if real.empty or synth.empty:
            score += 1e6
            continue

        mean_term = abs(real.mean() - synth.mean()) / (abs(real.mean()) + 1e-9)
        std_term = abs(real.std() - synth.std()) / (abs(real.std()) + 1e-9)

        real_q = real.quantile([0.1, 0.5, 0.9]).values
        synth_q = synth.quantile([0.1, 0.5, 0.9]).values
        quant_term = float(np.mean(np.abs(real_q - synth_q) / (np.abs(real_q) + 1e-9)))
        score += mean_term + std_term + quant_term
    return float(score)


def fit_and_sample(
    train_df: pd.DataFrame,
    n_samples: int,
    epochs: int,
    random_seed: int,
) -> Tuple[pd.DataFrame, str, Dict[str, float]]:
    metadata = SingleTableMetadata()
    metadata.detect_from_dataframe(data=train_df)

    ctgan = CTGANSynthesizer(metadata, epochs=epochs, verbose=False, enforce_min_max_values=True)
    ctgan.fit(train_df)
    ctgan_samples = ctgan.sample(num_rows=n_samples)

    tvae = TVAESynthesizer(metadata, epochs=epochs, enforce_min_max_values=True)
    tvae.fit(train_df)
    tvae_samples = tvae.sample(num_rows=n_samples)

    ctgan_score = similarity_score(train_df, ctgan_samples, ["sales", "price", "stock"])
    tvae_score = similarity_score(train_df, tvae_samples, ["sales", "price", "stock"])

    if ctgan_score <= tvae_score:
        chosen = ctgan_samples
        chosen_name = "CTGAN"
    else:
        chosen = tvae_samples
        chosen_name = "TVAE"

    return chosen, chosen_name, {"ctgan": ctgan_score, "tvae": tvae_score}


def build_product_profiles(sales_df: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    per_product = (
        sales_df.groupby("product_id", as_index=False)
        .agg(
            total_sales=("sales", "sum"),
            avg_sales=("sales", "mean"),
            std_sales=("sales", "std"),
            avg_price=("price", "mean"),
            avg_stock=("stock", "mean"),
        )
        .fillna(0)
    )

    cutoff = per_product["total_sales"].quantile(0.8)
    top_products = per_product[per_product["total_sales"] >= cutoff].copy()
    if top_products.empty:
        top_products = per_product.nlargest(min(10, len(per_product)), "total_sales").copy()

    top_products["weight"] = top_products["total_sales"] / top_products["total_sales"].sum()
    top_products = top_products.reset_index(drop=True)

    if top_products.empty:
        raise ValueError("No product profiles found in source data")

    top_products["template_noise"] = rng.uniform(0.95, 1.05, size=len(top_products))
    return top_products


def generate_inventory(
    source_inventory_df: pd.DataFrame,
    top_profiles: pd.DataFrame,
    target_products: int,
    seed: int,
) -> pd.DataFrame:
    rng = np.random.default_rng(seed)

    metadata = SingleTableMetadata()
    train_inv = source_inventory_df[["current_stock", "price"]].copy()
    metadata.detect_from_dataframe(train_inv)

    ctgan = CTGANSynthesizer(metadata, epochs=25, verbose=False, enforce_min_max_values=True)
    ctgan.fit(train_inv)
    inv_ctgan = ctgan.sample(num_rows=target_products)

    tvae = TVAESynthesizer(metadata, epochs=25, enforce_min_max_values=True)
    tvae.fit(train_inv)
    inv_tvae = tvae.sample(num_rows=target_products)

    score_ctgan = similarity_score(train_inv, inv_ctgan, ["current_stock", "price"])
    score_tvae = similarity_score(train_inv, inv_tvae, ["current_stock", "price"])
    inv_out = inv_ctgan if score_ctgan <= score_tvae else inv_tvae

    weights = top_profiles["weight"].to_numpy()
    idx = rng.choice(len(top_profiles), size=target_products, p=weights)
    chosen_templates = top_profiles.iloc[idx].reset_index(drop=True)

    inv_out["price"] = (
        0.7 * pd.to_numeric(inv_out["price"], errors="coerce").fillna(chosen_templates["avg_price"]) +
        0.3 * chosen_templates["avg_price"] * rng.uniform(0.9, 1.1, size=target_products)
    ).clip(lower=0.05)

    inv_out["current_stock"] = (
        0.7 * pd.to_numeric(inv_out["current_stock"], errors="coerce").fillna(chosen_templates["avg_stock"]) +
        0.3 * chosen_templates["avg_stock"] * rng.uniform(0.85, 1.15, size=target_products)
    ).clip(lower=1)

    inv_out["product_id"] = [f"GP{str(i + 1).zfill(5)}" for i in range(target_products)]
    inv_out["current_stock"] = inv_out["current_stock"].round().astype(int)
    inv_out["price"] = inv_out["price"].astype(float).round(4)

    return inv_out[["product_id", "current_stock", "price"]]


def assign_dates(source_sales_df: pd.DataFrame, n_rows: int, seed: int) -> pd.Series:
    rng = np.random.default_rng(seed)
    date_totals = source_sales_df.groupby("date")["sales"].sum()
    probs = (date_totals / date_totals.sum()).to_numpy()
    sampled_dates = rng.choice(date_totals.index.to_numpy(), size=n_rows, p=probs)
    return pd.to_datetime(sampled_dates)


def assign_products_with_popularity(
    source_sales_df: pd.DataFrame,
    inventory_df: pd.DataFrame,
    n_rows: int,
    seed: int,
) -> np.ndarray:
    rng = np.random.default_rng(seed)

    source_pop = source_sales_df.groupby("product_id")["sales"].sum().sort_values(ascending=False)
    source_weights = source_pop / source_pop.sum()

    target_products = inventory_df["product_id"].to_numpy()
    # Map source popularity curve onto generated product list.
    curve = np.interp(
        np.linspace(0, len(source_weights) - 1, num=len(target_products)),
        np.arange(len(source_weights)),
        source_weights.to_numpy(),
    )

    curve = curve / curve.sum()
    return rng.choice(target_products, size=n_rows, p=curve)


def enforce_constraints(
    sales_df: pd.DataFrame,
    inventory_df: pd.DataFrame,
    min_records_per_product: int,
    seed: int,
) -> pd.DataFrame:
    rng = np.random.default_rng(seed)

    sales_df["sales"] = pd.to_numeric(sales_df["sales"], errors="coerce").fillna(0).clip(lower=0)
    sales_df["price"] = pd.to_numeric(sales_df["price"], errors="coerce").fillna(0.05).clip(lower=0.05)
    sales_df["stock"] = pd.to_numeric(sales_df["stock"], errors="coerce").fillna(1).clip(lower=1)

    inv_price_map = inventory_df.set_index("product_id")["price"]
    inv_stock_map = inventory_df.set_index("product_id")["current_stock"]

    sales_df["price"] = sales_df.apply(
        lambda row: 0.6 * row["price"] + 0.4 * float(inv_price_map.get(row["product_id"], row["price"])),
        axis=1,
    )
    sales_df["stock"] = sales_df.apply(
        lambda row: 0.6 * row["stock"] + 0.4 * float(inv_stock_map.get(row["product_id"], row["stock"])),
        axis=1,
    )

    counts = sales_df["product_id"].value_counts()
    underrepresented = counts[counts < min_records_per_product]

    if not underrepresented.empty:
        templates = sales_df.sample(
            n=len(underrepresented) * min_records_per_product,
            replace=True,
            random_state=seed,
        ).reset_index(drop=True)
        expanded = []
        for idx, (pid, current_count) in enumerate(underrepresented.items()):
            needed = min_records_per_product - int(current_count)
            chunk = templates.iloc[idx * min_records_per_product : idx * min_records_per_product + needed].copy()
            chunk["product_id"] = pid
            chunk["sales"] = chunk["sales"] * rng.uniform(0.7, 1.3, size=needed)
            expanded.append(chunk)
        if expanded:
            sales_df = pd.concat([sales_df] + expanded, ignore_index=True)

    sales_df["sales"] = sales_df["sales"].round(2)
    sales_df["price"] = sales_df["price"].round(4)
    sales_df["stock"] = sales_df["stock"].round(2)
    sales_df["revenue"] = (sales_df["sales"] * sales_df["price"]).round(4)

    sales_df = sales_df.sort_values(["product_id", "date"]).reset_index(drop=True)
    return sales_df


def generate_sales(
    source_sales_df: pd.DataFrame,
    target_rows: int,
    inventory_df: pd.DataFrame,
    cfg: GenConfig,
) -> Tuple[pd.DataFrame, str, Dict[str, float]]:
    rng = np.random.default_rng(cfg.seed)

    model_train = source_sales_df[["sales", "price", "stock"]].copy()
    if len(model_train) > cfg.max_train_rows:
        model_train = model_train.sample(n=cfg.max_train_rows, random_state=cfg.seed)

    generated_base, model_name, scores = fit_and_sample(
        train_df=model_train,
        n_samples=target_rows,
        epochs=cfg.epochs,
        random_seed=cfg.seed,
    )

    generated_base["date"] = assign_dates(source_sales_df, len(generated_base), cfg.seed + 1)
    generated_base["product_id"] = assign_products_with_popularity(
        source_sales_df=source_sales_df,
        inventory_df=inventory_df,
        n_rows=len(generated_base),
        seed=cfg.seed + 2,
    )

    generated_base = enforce_constraints(
        generated_base,
        inventory_df,
        min_records_per_product=cfg.min_records_per_product,
        seed=cfg.seed + 3,
    )

    # Add small product-specific jitter to guarantee varied sales trajectories.
    product_factor = {
        pid: rng.uniform(0.85, 1.25) for pid in inventory_df["product_id"].tolist()
    }
    generated_base["sales"] = generated_base.apply(
        lambda row: max(0.0, row["sales"] * product_factor.get(row["product_id"], 1.0)),
        axis=1,
    )
    generated_base["revenue"] = (generated_base["sales"] * generated_base["price"]).round(4)

    return generated_base[["product_id", "date", "sales", "revenue", "price", "stock"]], model_name, scores


def write_output_db(output_db: Path, sales_df: pd.DataFrame, inventory_df: pd.DataFrame) -> None:
    if output_db.exists():
        output_db.unlink()

    conn = sqlite3.connect(output_db)
    try:
        sales_to_write = sales_df.copy()
        sales_to_write["date"] = pd.to_datetime(sales_to_write["date"]).dt.strftime("%Y-%m-%d")

        sales_to_write.to_sql("sales_aggregated", conn, if_exists="replace", index=False)
        inventory_df.to_sql("inventory", conn, if_exists="replace", index=False)

        cursor = conn.cursor()
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_sales_product_date ON sales_aggregated(product_id, date)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_inventory_product_id ON inventory(product_id)"
        )
        conn.commit()
    finally:
        conn.close()


def print_summary(
    source_sales_df: pd.DataFrame,
    generated_sales_df: pd.DataFrame,
    source_inventory_df: pd.DataFrame,
    generated_inventory_df: pd.DataFrame,
    chosen_model: str,
    scores: Dict[str, float],
) -> None:
    src_products = source_inventory_df["product_id"].nunique()
    gen_products = generated_inventory_df["product_id"].nunique()
    src_sales = len(source_sales_df)
    gen_sales = len(generated_sales_df)

    print("\n" + "=" * 70)
    print("SYNTHETIC DATABASE GENERATION SUMMARY")
    print("=" * 70)
    print(f"Model comparison score (lower is better): CTGAN={scores['ctgan']:.4f}, TVAE={scores['tvae']:.4f}")
    print(f"Chosen model for sales generation: {chosen_model}")
    print(f"Products: source={src_products}, generated={gen_products} (x{gen_products / max(1, src_products):.2f})")
    print(f"Sales rows: source={src_sales}, generated={gen_sales} (x{gen_sales / max(1, src_sales):.2f})")

    top_gen = (
        generated_sales_df.groupby("product_id", as_index=False)["sales"]
        .sum()
        .sort_values("sales", ascending=False)
        .head(10)
    )
    print("\nTop generated products by total sales:")
    print(top_gen.to_string(index=False))
    print("=" * 70 + "\n")


def main() -> None:
    cfg = parse_args()
    np.random.seed(cfg.seed)

    source_sales_df, source_inventory_df = load_source_data(cfg.source_db)

    src_products = source_inventory_df["product_id"].nunique()
    src_sales_rows = len(source_sales_df)

    target_products = int(np.ceil(src_products * cfg.product_multiplier))
    target_sales_rows = int(np.ceil(src_sales_rows * cfg.sales_multiplier))

    profile_rng = np.random.default_rng(cfg.seed + 10)
    top_profiles = build_product_profiles(source_sales_df, profile_rng)

    generated_inventory_df = generate_inventory(
        source_inventory_df=source_inventory_df,
        top_profiles=top_profiles,
        target_products=target_products,
        seed=cfg.seed + 20,
    )

    generated_sales_df, chosen_model, scores = generate_sales(
        source_sales_df=source_sales_df,
        target_rows=target_sales_rows,
        inventory_df=generated_inventory_df,
        cfg=cfg,
    )

    write_output_db(cfg.output_db, generated_sales_df, generated_inventory_df)
    print_summary(
        source_sales_df=source_sales_df,
        generated_sales_df=generated_sales_df,
        source_inventory_df=source_inventory_df,
        generated_inventory_df=generated_inventory_df,
        chosen_model=chosen_model,
        scores=scores,
    )


if __name__ == "__main__":
    main()
