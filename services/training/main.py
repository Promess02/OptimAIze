import os
import json
import pickle
import sqlite3
import pandas as pd
import numpy as np
import xgboost as xgb
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
MODELS_PATH = os.getenv('MODELS_PATH', '/models')
MIN_TRAIN_ROWS = int(os.getenv('MIN_TRAIN_ROWS', '30'))
VARIANCE_STD_THRESHOLD = float(os.getenv('VARIANCE_STD_THRESHOLD', '1.0'))

FEATURE_COLUMNS = [
    'dayofweek',
    'month',
    'day',
    'quarter',
    'year',
    'day_of_year',
    'is_month_end',
    'is_quarter_end',
    'days_since_start',
    'sales_lag_1',
    'sales_lag_7',
    'sales_lag_14',
    'sales_lag_30',
    'sales_rolling_mean_7',
    'sales_rolling_std_7',
    'sales_rolling_mean_14',
    'sales_rolling_std_14',
    'sales_rolling_mean_30',
    'sales_rolling_std_30',
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
        for window in [7, 14, 30]:
            rolling = df['sales'].rolling(window=window)
            df[f'sales_rolling_mean_{window}'] = rolling.mean()
            df[f'sales_rolling_std_{window}'] = rolling.std()

        df = df.bfill().ffill()
        df = df.fillna(df.mean(numeric_only=True))

    return df, min_date


def build_fallback_payload(product_id, df_sales, min_date):
    sales_series = df_sales['sales'].astype(float)
    last_sales = float(sales_series.iloc[-1]) if not sales_series.empty else 0.0
    mean_sales = float(sales_series.mean()) if not sales_series.empty else 0.0
    std_sales = float(sales_series.std() or 0.0)
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
            SELECT product_id, date, sales
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

        X_train = df_sales[FEATURE_COLUMNS]
        y_train = df_sales['sales']

        model = xgb.XGBRegressor(
            objective='reg:squarederror',
            n_estimators=200,
            max_depth=6,
            learning_rate=0.05,
            subsample=0.8,
            colsample_bytree=0.8,
            tree_method='hist',
            random_state=42,
        )
        model.fit(X_train, y_train)

        sales_series = df_sales['sales'].astype(float)
        # Store model in Redis
        payload = {
            'model': model,
            'min_date': min_date,
            'trained_at': datetime.now().isoformat(),
            'product_id': product_id,
            'feature_cols': FEATURE_COLUMNS,
            'model_type': 'xgboost',
            'selected_strategy': 'xgboost_default',
            'historical_sales_mean': float(sales_series.mean()),
            'historical_sales_std': float(sales_series.std() or 0.0),
            'recent_sales': [float(v) for v in sales_series.tail(60).tolist()],
            'variance_metrics': {
                'std_sales': float(sales_series.std() or 0.0),
                'mean_sales': float(sales_series.mean()),
                'num_rows': int(len(df_sales)),
                'unique_sales': int(sales_series.nunique()),
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
    """Train models for all products in database"""
    try:
        with closing(sqlite3.connect(DB_PATH)) as conn:
            query = "SELECT DISTINCT product_id FROM sales_aggregated"
            products = pd.read_sql(query, conn)['product_id'].astype(str).tolist()
        
        logger.info(f"Starting training for {len(products)} products")
        
        success_count = 0
        fallback_count = 0
        failed_count = 0
        for product_id in products:
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
