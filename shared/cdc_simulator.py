import os
import json
import time
import sqlite3
from confluent_kafka import Producer
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
DB_PATH = os.getenv('DB_PATH', './ecommerce.db')

producer_conf = {'bootstrap.servers': KAFKA_BROKER}
producer = Producer(producer_conf)

def delivery_report(err, msg):
    if err is not None:
        logger.error(f"CDC event delivery failed: {err}")
    else:
        logger.info(f"CDC event delivered to {msg.topic()}")

def emit_sales_event(product_id, sales_amount):
    """Emit SalesCreated CDC event"""
    event = {
        "event_type": "SalesCreated",
        "timestamp": datetime.now().isoformat(),
        "data": {
            "product_id": product_id,
            "sales": sales_amount,
            "date": datetime.now().strftime("%Y-%m-%d")
        }
    }
    
    producer.produce(
        'database_changes',
        key=product_id.encode('utf-8'),
        value=json.dumps(event).encode('utf-8'),
        callback=delivery_report
    )
    producer.poll(0)

def emit_inventory_event(product_id, new_stock):
    """Emit InventoryUpdated CDC event"""
    event = {
        "event_type": "InventoryUpdated",
        "timestamp": datetime.now().isoformat(),
        "data": {
            "product_id": product_id,
            "current_stock": new_stock
        }
    }
    
    producer.produce(
        'database_changes',
        key=product_id.encode('utf-8'),
        value=json.dumps(event).encode('utf-8'),
        callback=delivery_report
    )
    producer.poll(0)

def simulate_sale(product_id, amount=10):
    """Simulate a sale transaction"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Insert sale record
        cursor.execute("""
            INSERT INTO sales_aggregated (product_id, date, sales, revenue, price, stock)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (product_id, datetime.now().strftime("%Y-%m-%d"), amount, amount * 99.99, 99.99, 100))
        
        # Update inventory
        cursor.execute("""
            UPDATE inventory 
            SET current_stock = current_stock - ?
            WHERE product_id = ?
        """, (amount, product_id))
        
        conn.commit()
        conn.close()
        
        # Emit CDC events
        emit_sales_event(product_id, amount)
        logger.info(f"Simulated sale of {amount} units for product {product_id}")
        
    except Exception as e:
        logger.error(f"Error simulating sale: {e}")

if __name__ == "__main__":
    logger.info("CDC Simulator started")
    
    # Get sample products from database
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT product_id FROM sales_aggregated LIMIT 5")
        products = [row[0] for row in cursor.fetchall()]
        conn.close()
        
        if not products:
            logger.warning("No products found in database")
        else:
            logger.info(f"Found {len(products)} products for simulation")
            
            # Simulate sales periodically
            while True:
                for product_id in products:
                    simulate_sale(product_id, amount=5)
                    time.sleep(2)
                
                logger.info("Waiting 60 seconds before next batch...")
                time.sleep(60)
                
    except KeyboardInterrupt:
        logger.info("CDC Simulator stopped")
    finally:
        producer.flush()
