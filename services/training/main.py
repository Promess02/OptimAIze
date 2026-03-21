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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
REDIS_HOST = os.getenv('REDIS_HOST', 'redis_training')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
DB_PATH = os.getenv('DB_PATH', '/data/ecommerce.db')
MODELS_PATH = os.getenv('MODELS_PATH', '/models')

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
    """Extract time features for XGBoost"""
    df = df.copy()
    df['dayofweek'] = df['date'].dt.dayofweek
    df['month'] = df['date'].dt.month
    df['year'] = df['date'].dt.year
    df['day'] = df['date'].dt.day
    
    if not is_future:
        min_date = df['date'].min()
        
    df['days_since_start'] = (df['date'] - min_date).dt.days
    return df, min_date

def train_model_for_product(product_id):
    """Train XGBoost model for a single product"""
    try:
        conn = sqlite3.connect(DB_PATH)
        
        query = "SELECT * FROM sales_aggregated WHERE product_id = ?"
        df_sales = pd.read_sql(query, conn, params=(product_id,))
        conn.close()
        
        if len(df_sales) < 5:
            logger.warning(f"Product {product_id} has insufficient data ({len(df_sales)} rows)")
            return None
            
        df_sales['date'] = pd.to_datetime(df_sales['date'])
        df_sales, min_date = extract_time_features(df_sales)
        
        X_train = df_sales[['dayofweek', 'month', 'year', 'day', 'days_since_start']]
        y_train = df_sales['sales']
        
        model = xgb.XGBRegressor(objective='reg:squarederror', n_estimators=50, random_state=42)
        model.fit(X_train, y_train)
        
        # Store model in Redis
        model_bytes = pickle.dumps({
            'model': model,
            'min_date': min_date,
            'trained_at': datetime.now().isoformat(),
            'product_id': product_id
        })
        redis_client.set(f"model:{product_id}", model_bytes)
        
        # Also save to disk
        os.makedirs(MODELS_PATH, exist_ok=True)
        with open(f"{MODELS_PATH}/model_{product_id}.pkl", 'wb') as f:
            pickle.dump({'model': model, 'min_date': min_date}, f)
        
        logger.info(f"Model trained for product {product_id}")
        return True
        
    except Exception as e:
        logger.error(f"Error training model for {product_id}: {e}")
        return None

def train_all_models():
    """Train models for all products in database"""
    try:
        conn = sqlite3.connect(DB_PATH)
        query = "SELECT DISTINCT product_id FROM sales_aggregated"
        products = pd.read_sql(query, conn)['product_id'].tolist()
        conn.close()
        
        logger.info(f"Starting training for {len(products)} products")
        
        success_count = 0
        for product_id in products:
            result = train_model_for_product(product_id)
            if result:
                success_count += 1
        
        training_result = {
            "timestamp": datetime.now().isoformat(),
            "total_products": len(products),
            "successful_trainings": success_count,
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
