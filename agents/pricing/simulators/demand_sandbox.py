import os
import pickle
import sqlite3
from datetime import datetime

import numpy as np
import pandas as pd


class DemandSandbox:
    def __init__(self, db_path: str, models_path: str = "/models"):
        self.db_path = db_path
        self.models_path = models_path

    def _build_xgb_features(self, date_value: datetime, inventory: float, base_sales: float):
        df_future = pd.DataFrame({"date": [pd.to_datetime(date_value)]})
        df_future["dayofweek"] = df_future["date"].dt.dayofweek
        df_future["month"] = df_future["date"].dt.month
        df_future["day"] = df_future["date"].dt.day
        df_future["quarter"] = df_future["date"].dt.quarter
        df_future["year"] = df_future["date"].dt.year
        df_future["day_of_year"] = df_future["date"].dt.dayofyear
        df_future["is_month_end"] = df_future["date"].dt.is_month_end.astype(int)
        df_future["is_quarter_end"] = df_future["date"].dt.is_quarter_end.astype(int)
        df_future["days_since_start"] = 0

        for lag in [1, 7, 14, 30]:
            df_future[f"sales_lag_{lag}"] = base_sales
        for window in [7, 14, 30]:
            df_future[f"sales_rolling_mean_{window}"] = base_sales
            df_future[f"sales_rolling_std_{window}"] = max(1.0, base_sales * 0.2)

        df_future["inventory"] = inventory
        return df_future

    def _load_xgb_payload(self, product_id: str):
        candidate_paths = [
            os.path.join(self.models_path, f"model_{product_id}.pkl"),
            os.path.join(self.models_path, f"procurement_model_{product_id}.pkl"),
        ]
        for path in candidate_paths:
            if os.path.exists(path):
                with open(path, "rb") as model_file:
                    return pickle.load(model_file)
        return None

    def _historical_sales_mean(self, product_id: str) -> float:
        try:
            conn = sqlite3.connect(self.db_path)
            row = conn.execute(
                "SELECT AVG(sales) FROM sales_aggregated WHERE product_id = ?",
                (product_id,),
            ).fetchone()
            conn.close()
            if row and row[0] is not None:
                return float(row[0])
        except Exception:
            pass
        return 50.0

    def predict_sales_volume(
        self,
        product_id: str,
        candidate_price: float,
        competitor_price: float,
        season: int,
        inventory: float,
    ) -> float:
        base_sales = self._historical_sales_mean(product_id)
        payload = self._load_xgb_payload(product_id)

        if payload and isinstance(payload, dict) and "model" in payload:
            try:
                model = payload["model"]
                feature_cols = payload.get("feature_cols")
                if not feature_cols:
                    feature_cols = [
                        "dayofweek", "month", "day", "quarter", "year", "day_of_year",
                        "is_month_end", "is_quarter_end", "days_since_start",
                        "sales_lag_1", "sales_lag_7", "sales_lag_14", "sales_lag_30",
                        "sales_rolling_mean_7", "sales_rolling_std_7", "sales_rolling_mean_14",
                        "sales_rolling_std_14", "sales_rolling_mean_30", "sales_rolling_std_30",
                    ]

                df_features = self._build_xgb_features(datetime.utcnow(), inventory, base_sales)
                for col in feature_cols:
                    if col not in df_features.columns:
                        df_features[col] = 0
                x = df_features[feature_cols].values
                prediction = float(model.predict(x)[0])
            except Exception:
                prediction = base_sales
        else:
            prediction = base_sales

        # apply simple price-elasticity adjustment around competitor reference
        reference = competitor_price if competitor_price and competitor_price > 0 else max(candidate_price, 1.0)
        relative_gap = (candidate_price - reference) / max(reference, 1.0)
        elasticity = -1.1
        adjusted = prediction * max(0.1, (1 + elasticity * relative_gap))

        # season factor in [1..4] -> mild multiplier
        season_factor = {1: 0.95, 2: 1.0, 3: 1.05, 4: 1.1}.get(int(season), 1.0)
        adjusted *= season_factor

        return float(np.maximum(0.0, adjusted))
