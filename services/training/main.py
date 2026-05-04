import os
import json
import sys
import pickle
import sqlite3
import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.metrics import mean_squared_error
from datetime import timedelta, datetime
from confluent_kafka import Consumer, Producer
import redis
import logging
from contextlib import closing

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
REDIS_HOST = os.getenv('REDIS_HOST', 'redis_training')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
DB_PATH = os.getenv('DB_PATH', '/data/ecommerce.db')
TOP_PRODUCTS_LIMIT = os.getenv('TOP_PRODUCTS_LIMIT')
MODELS_PATH = os.getenv('MODELS_PATH', '/models')
MIN_TRAIN_ROWS = int(os.getenv('MIN_TRAIN_ROWS', '30'))
VARIANCE_STD_THRESHOLD = float(os.getenv('VARIANCE_STD_THRESHOLD', '1.0'))
VAL_RATIO = float(os.getenv('VAL_RATIO', '0.15'))
TEST_RATIO = float(os.getenv('TEST_RATIO', '0.15'))
MIN_SPLIT_ROWS = int(os.getenv('MIN_SPLIT_ROWS', '7'))
MIN_TRAIN_ROWS_SPLIT = int(os.getenv('MIN_TRAIN_ROWS_SPLIT', '14'))

FEATURE_COLUMNS = [
    'dayofweek',
    'month',
    'day',
    'year',
    'sales_lag_1',
    'sales_lag_7',
    'sales_lag_14',
    'sales_lag_30',
    'revenue_lag_1',
    'revenue_lag_7',
    'revenue_lag_14',
    'revenue_lag_30',
    'sales_rolling_mean_7',
    'sales_rolling_std_7',
    'sales_rolling_mean_14',
    'sales_rolling_std_14',
    'is_out_of_stock'
]

redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=False)

producer_conf = {'bootstrap.servers': KAFKA_BROKER}
producer = Producer(producer_conf)

consumer_conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'training_service_group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumer_conf)
consumer.subscribe(['database_changes'])


def _rmse(y_true, y_pred):
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)
    n = min(len(y_true), len(y_pred))
    if n == 0:
        return float('inf')
    return float(np.sqrt(mean_squared_error(y_true[:n], y_pred[:n])))


def split_train_val_test(df):
    """Chronological train/validation/test split used for leakage-safe strategy selection."""
    n = len(df)
    if n < (MIN_TRAIN_ROWS_SPLIT + 2 * MIN_SPLIT_ROWS):
        return None

    val_rows = max(MIN_SPLIT_ROWS, int(round(n * VAL_RATIO)))
    test_rows = max(MIN_SPLIT_ROWS, int(round(n * TEST_RATIO)))
    train_rows = n - val_rows - test_rows

    if train_rows < MIN_TRAIN_ROWS_SPLIT:
        needed = MIN_TRAIN_ROWS_SPLIT - train_rows
        trim_val = min(needed, val_rows - MIN_SPLIT_ROWS)
        val_rows -= max(0, trim_val)
        needed -= max(0, trim_val)
        if needed > 0:
            trim_test = min(needed, test_rows - MIN_SPLIT_ROWS)
            test_rows -= max(0, trim_test)

    train_rows = n - val_rows - test_rows
    if train_rows < MIN_TRAIN_ROWS_SPLIT:
        return None

    train_df = df.iloc[:train_rows].copy()
    val_df = df.iloc[train_rows:train_rows + val_rows].copy()
    test_df = df.iloc[train_rows + val_rows:].copy()
    return train_df, val_df, test_df

def extract_time_features(df, is_future=False, min_date=None):
    """Extract notebook-aligned temporal features."""
    df = df.copy()
    df['dayofweek'] = df['date'].dt.dayofweek
    df['month'] = df['date'].dt.month
    df['day'] = df['date'].dt.day
    df['quarter'] = df['date'].dt.quarter
    df['year'] = df['date'].dt.year
    df['day_of_year'] = df['date'].dt.dayofyear
    df['is_month_end'] = df['date'].dt.is_month_end.astype(int)
    df['is_quarter_end'] = df['date'].dt.is_quarter_end.astype(int)

    if not is_future:
        min_date = df['date'].min()

    df['days_since_start'] = (df['date'] - min_date).dt.days

    if not is_future:
        for lag in [1, 7, 14, 30]:
            df[f'sales_lag_{lag}'] = df['sales'].shift(lag)
            if 'revenue' in df.columns:
                df[f'revenue_lag_{lag}'] = df['revenue'].shift(lag)
            else:
                df[f'revenue_lag_{lag}'] = 0.0

        for window in [7, 14, 30]:
            rolling = df['sales'].rolling(window=window)
            df[f'sales_rolling_mean_{window}'] = rolling.mean()
            df[f'sales_rolling_std_{window}'] = rolling.std()

        if 'stock' in df.columns:
            df['is_out_of_stock'] = (df['stock'] <= 0).astype(int)
        else:
            df['is_out_of_stock'] = 0

        df = df.bfill().ffill()
        df = df.fillna(df.mean(numeric_only=True))

    return df, min_date


def build_fallback_payload(product_id, df_sales, min_date):
    sales_series = df_sales['sales'].astype(float)
    last_sales = float(sales_series.iloc[-1]) if not sales_series.empty else 0.0
    mean_sales = float(sales_series.mean()) if not sales_series.empty else 0.0
    std_sales = float(sales_series.std() or 0.0)
    mean_price = float(df_sales['price'].mean()) if 'price' in df_sales.columns and not df_sales.empty else 99.99
    return {
        'model': None,
        'min_date': min_date,
        'trained_at': datetime.now().isoformat(),
        'product_id': product_id,
        'feature_cols': FEATURE_COLUMNS,
        'model_type': 'naive_fallback',
        'selected_strategy': 'lag1_or_mean',
        'historical_sales_mean': mean_sales,
        'historical_sales_std': std_sales,
        'historical_price': mean_price,
        'recent_sales': [float(v) for v in sales_series.tail(60).tolist()],
        'variance_metrics': {
            'std_sales': std_sales,
            'mean_sales': mean_sales,
            'num_rows': int(len(df_sales)),
            'unique_sales': int(sales_series.nunique()),
        },
        'fallback_config': {
            'type': 'lag1_or_mean',
            'last_sales': last_sales,
            'mean_sales': mean_sales,
        },
    }

def train_model_for_product(product_id):
    """Train a per-product model, or persist fallback for low-variance products."""
    try:
        with closing(sqlite3.connect(DB_PATH)) as conn:
            query = """
            SELECT product_id, date, sales, revenue, price, stock
            FROM sales_aggregated
            WHERE product_id = ?
            ORDER BY date
            """
            df_sales = pd.read_sql(query, conn, params=(product_id,))

        if len(df_sales) < MIN_TRAIN_ROWS:
            logger.warning(f"Product {product_id} has insufficient data ({len(df_sales)} rows), storing fallback")
            df_sales['date'] = pd.to_datetime(df_sales['date']) if not df_sales.empty else pd.to_datetime([])
            min_date = df_sales['date'].min() if not df_sales.empty else datetime.now()
            payload = build_fallback_payload(product_id, df_sales, min_date)
            redis_client.set(f"model:{product_id}", pickle.dumps(payload))
            return {'status': 'fallback'}

        df_sales['date'] = pd.to_datetime(df_sales['date'])
        df_sales, min_date = extract_time_features(df_sales)

        sales_std = float(df_sales['sales'].std() or 0.0)
        unique_sales = int(df_sales['sales'].nunique())
        if sales_std < VARIANCE_STD_THRESHOLD or unique_sales <= 2:
            logger.info(
                "Product %s low variance (std=%.4f unique=%s), storing fallback",
                product_id,
                sales_std,
                unique_sales,
            )
            payload = build_fallback_payload(product_id, df_sales, min_date)
            redis_client.set(f"model:{product_id}", pickle.dumps(payload))
            return {'status': 'fallback'}

        split = split_train_val_test(df_sales)
        if split is None:
            logger.info(
                "Product %s has too few rows for train/val/test split (%s), using default XGBoost strategy",
                product_id,
                len(df_sales),
            )
            train_df = df_sales
            val_df = None
            test_df = None
        else:
            train_df, val_df, test_df = split

        X_train = train_df[FEATURE_COLUMNS]
        y_train = train_df['sales']

        train_mask = train_df['stock'] > 0 if 'stock' in train_df.columns else pd.Series(True, index=train_df.index)
        X_train_fit = train_df.loc[train_mask, FEATURE_COLUMNS]
        y_train_fit = train_df.loc[train_mask, 'sales']

        model = xgb.XGBRegressor(
            objective='reg:squarederror',
            eval_metric='rmse',
            n_estimators=2000,
            max_depth=4,
            learning_rate=0.03,
            min_child_weight=5,
            subsample=0.85,
            colsample_bytree=0.85,
            reg_alpha=0.2,
            reg_lambda=2.0,
            tree_method='hist',
            random_state=42,
            n_jobs=-1,
            early_stopping_rounds=120,
        )

        selected_strategy = 'xgb'
        strategy_scores = {}
        test_metrics = {}

        if val_df is not None and len(val_df) > 0:
            val_mask = val_df['stock'] > 0 if 'stock' in val_df.columns else pd.Series(True, index=val_df.index)
            X_val = val_df[FEATURE_COLUMNS]
            y_val = val_df['sales']
            X_val_fit = val_df.loc[val_mask, FEATURE_COLUMNS]
            y_val_fit = val_df.loc[val_mask, 'sales']
            model.fit(X_train_fit, y_train_fit, eval_set=[(X_val_fit, y_val_fit)], verbose=False)

            val_xgb = np.clip(model.predict(X_val), 0, None)
            val_lag1 = np.clip(val_df['sales_lag_1'].values, 0, None)
            val_zero = np.zeros(len(y_val), dtype=float)
            val_mean = np.full(len(y_val), float(y_train.mean()), dtype=float)

            strategy_scores = {
                'xgb': _rmse(y_val.values, val_xgb),
                'lag1': _rmse(y_val.values, val_lag1),
                'zero': _rmse(y_val.values, val_zero),
                'mean': _rmse(y_val.values, val_mean),
            }

            zero_share = float((train_df['sales'] <= 0).mean())
            mean_sales = float(train_df['sales'].mean())
            intermittent_low_demand = (zero_share >= 0.35) and (mean_sales < 50)

            if intermittent_low_demand:
                candidate_keys = [k for k in ('lag1', 'zero', 'mean') if np.isfinite(strategy_scores.get(k, float('inf')))]
                selected_strategy = min(candidate_keys, key=lambda k: strategy_scores[k]) if candidate_keys else 'xgb'
            else:
                selected_strategy = min(strategy_scores, key=strategy_scores.get)

            if test_df is not None and len(test_df) > 0:
                X_test = test_df[FEATURE_COLUMNS]
                y_test = test_df['sales'].values
                test_preds = {
                    'xgb': np.clip(model.predict(X_test), 0, None),
                    'lag1': np.clip(test_df['sales_lag_1'].values, 0, None),
                    'zero': np.zeros(len(y_test), dtype=float),
                    'mean': np.full(len(y_test), float(y_train.mean()), dtype=float),
                }
                selected_test = test_preds[selected_strategy]
                test_metrics = {
                    'selected_strategy_rmse': _rmse(y_test, selected_test),
                    'xgb_rmse': _rmse(y_test, test_preds['xgb']),
                    'lag1_rmse': _rmse(y_test, test_preds['lag1']),
                    'zero_rmse': _rmse(y_test, test_preds['zero']),
                    'mean_rmse': _rmse(y_test, test_preds['mean']),
                }
        else:
            model.fit(X_train_fit, y_train_fit, verbose=False)

        sales_series = df_sales['sales'].astype(float)
        if selected_strategy == 'xgb':
            payload_model = model
            payload_model_type = 'xgboost'
        else:
            payload_model = None
            payload_model_type = 'naive_fallback'

        fallback_type_map = {
            'xgb': 'model',
            'lag1': 'lag1',
            'zero': 'zero',
            'mean': 'mean',
        }

        # Store model in Redis
        payload = {
            'model': payload_model,
            'min_date': min_date,
            'trained_at': datetime.now().isoformat(),
            'product_id': product_id,
            'feature_cols': FEATURE_COLUMNS,
            'model_type': payload_model_type,
            'selected_strategy': selected_strategy,
            'historical_sales_mean': float(sales_series.mean()),
            'historical_sales_std': float(sales_series.std() or 0.0),
            'historical_price': float(df_sales['price'].mean()) if 'price' in df_sales.columns and not df_sales.empty else 99.99,
            'recent_sales': [float(v) for v in sales_series.tail(60).tolist()],
            'variance_metrics': {
                'std_sales': float(sales_series.std() or 0.0),
                'mean_sales': float(sales_series.mean()),
                'num_rows': int(len(df_sales)),
                'unique_sales': int(sales_series.nunique()),
            },
            'strategy_scores_val_rmse': strategy_scores,
            'test_metrics': test_metrics,
            'split_meta': {
                'val_ratio': VAL_RATIO,
                'test_ratio': TEST_RATIO,
                'train_rows': int(len(train_df)),
                'val_rows': int(len(val_df)) if val_df is not None else 0,
                'test_rows': int(len(test_df)) if test_df is not None else 0,
            },
            'fallback_config': {
                'type': fallback_type_map.get(selected_strategy, 'lag1_or_mean'),
                'last_sales': float(sales_series.iloc[-1]) if len(sales_series) else 0.0,
                'mean_sales': float(sales_series.mean()) if len(sales_series) else 0.0,
            },
        }
        model_bytes = pickle.dumps(payload)
        redis_client.set(f"model:{product_id}", model_bytes)

        # Also save to disk
        os.makedirs(MODELS_PATH, exist_ok=True)
        with open(f"{MODELS_PATH}/model_{product_id}.pkl", 'wb') as f:
            pickle.dump(payload, f)

        logger.info(f"Model trained for product {product_id}")
        return {'status': 'trained'}

    except Exception as e:
        logger.error(f"Error training model for {product_id}: {e}")
        return None

def train_all_models():
    """Train models for all products in database, or top N if TOP_PRODUCTS_LIMIT is set."""
    try:
        with closing(sqlite3.connect(DB_PATH)) as conn:
            if TOP_PRODUCTS_LIMIT and TOP_PRODUCTS_LIMIT.isdigit():
                limit = int(TOP_PRODUCTS_LIMIT)
                logger.info(f"Training limited to top {limit} products by sales volume.")
                query = """
                    SELECT product_id
                    FROM sales_aggregated
                    GROUP BY product_id
                    ORDER BY SUM(sales) DESC
                    LIMIT ?
                """
                products = pd.read_sql(query, conn, params=(limit,))['product_id'].astype(str).tolist()
            else:
                logger.info("Starting training for all products")
                query = "SELECT DISTINCT product_id FROM sales_aggregated"
                products = pd.read_sql(query, conn)['product_id'].astype(str).tolist()

        total_products = len(products)
        logger.info(f"Starting training for {total_products} products")

        success_count = 0
        fallback_count = 0
        failed_count = 0
        for i, product_id in enumerate(products):
            print(f"PROGRESS:{i + 1}/{total_products}", file=sys.stderr, flush=True)
            result = train_model_for_product(product_id)
            if result and result.get('status') == 'trained':
                success_count += 1
            elif result and result.get('status') == 'fallback':
                fallback_count += 1
            else:
                failed_count += 1

        training_result = {
            "timestamp": datetime.now().isoformat(),
            "total_products": len(products),
            "successful_trainings": success_count,
            "fallback_trainings": fallback_count,
            "failed_trainings": failed_count,
            "status": "completed"
        }

        producer.produce(
            'training_completed',
            key='all'.encode('utf-8'),
            value=json.dumps(training_result).encode('utf-8')
        )
        producer.flush()

        logger.info(f"Training completed: {success_count}/{len(products)} models")

    except Exception as e:
        logger.error(f"Error in train_all_models: {e}")

def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()}")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == '--batch-train':
        logger.info("Running in batch training mode.")
        train_all_models()
        logger.info("Batch training finished.")
    else:
        logger.info("Training Service started. Listening for CDC events...")
        # Initial training on startup
        train_all_models()
        try:
            while True:
                msg = consumer.poll(1.0)

                if msg is None:
                    continue
                if msg.error():
                    logger.error(f"Consumer error: {msg.error()}")
                    continue

                data = json.loads(msg.value().decode('utf-8'))
                event_type = data.get('event_type')

                logger.info(f"Received CDC event: {event_type}")

                if event_type in ['SalesCreated', 'InventoryUpdated']:
                    # Retrain all models asynchronously
                    train_all_models()

        except KeyboardInterrupt:
            logger.info("Shutting down Training Service")
        finally:
            consumer.close()
            producer.flush()
