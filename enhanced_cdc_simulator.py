#!/usr/bin/env python3
"""
Enhanced CDC Simulator for Event-Driven Multi-Agent ML System
Simulates sales events and triggers the complete agent workflow
"""

import os
import json
import time
import sqlite3
import random
from confluent_kafka import Producer
from datetime import datetime, timedelta
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
DB_PATH = os.getenv('DB_PATH', './ecommerce.db')

# Kafka Producer configuration
producer_conf = {'bootstrap.servers': KAFKA_BROKER}
producer = Producer(producer_conf)

class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    RESET = '\033[0m'
    BOLD = '\033[1m'

def colored(text, color):
    return f"{color}{text}{Colors.RESET}"

def delivery_report(err, msg):
    """Kafka delivery callback"""
    if err is not None:
        logger.error(f"CDC event delivery failed: {err}")
    else:
        logger.info(f"CDC event delivered to {msg.topic()} [{msg.partition()}] with offset {msg.offset()}")

def get_products_from_db():
    """Get list of products from database"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT product_id FROM inventory LIMIT 10")
        products = [row[0] for row in cursor.fetchall()]
        conn.close()
        return products
    except Exception as e:
        logger.error(f"Error reading products from DB: {e}")
        return []

def emit_sales_event(product_id, sales_amount, revenue, price):
    """Emit SalesCreated CDC event to Kafka"""
    event = {
        "event_type": "SalesCreated",
        "timestamp": datetime.now().isoformat(),
        "data": {
            "product_id": product_id,
            "sales": sales_amount,
            "revenue": revenue,
            "price": price,
            "date": datetime.now().strftime("%Y-%m-%d")
        }
    }

    try:
        producer.produce(
            'database_changes',
            key=product_id.encode('utf-8'),
            value=json.dumps(event).encode('utf-8'),
            callback=delivery_report
        )
        producer.poll(0)
        logger.info(f"Emitted SalesCreated event: {product_id}, qty={sales_amount}")
    except Exception as e:
        logger.error(f"Error emitting sales event: {e}")

def simulate_sale(product_id, sales_amount=None, revenue=None, price=None):
    """Simulate a sale transaction and emit CDC event"""
    if sales_amount is None:
        sales_amount = random.randint(5, 50)
    if price is None:
        price = random.uniform(50, 200)
    if revenue is None:
        revenue = sales_amount * price

    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Insert sale record
        cursor.execute("""
            INSERT INTO sales_aggregated (product_id, date, sales, revenue, price, stock)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            product_id,
            datetime.now().strftime("%Y-%m-%d"),
            sales_amount,
            round(revenue, 2),
            round(price, 2),
            100  # placeholder
        ))

        # Update inventory (reduce stock)
        cursor.execute("""
            UPDATE inventory
            SET current_stock = current_stock - ?
            WHERE product_id = ?
        """, (sales_amount, product_id))

        conn.commit()
        conn.close()

        # Emit CDC event to trigger agent workflow
        emit_sales_event(product_id, sales_amount, revenue, price)

        return {
            "product_id": product_id,
            "sales": sales_amount,
            "revenue": round(revenue, 2),
            "price": round(price, 2)
        }

    except Exception as e:
        logger.error(f"Error simulating sale: {e}")
        return None

def print_header(title):
    print(f"\n{Colors.BOLD}{Colors.BLUE}{'='*70}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.BLUE}{title:^70}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.BLUE}{'='*70}{Colors.RESET}\n")

def print_event(product_id, sales, revenue, price):
    print(f"{Colors.GREEN}✓{Colors.RESET} {Colors.BOLD}Sales Event:{Colors.RESET}")
    print(f"  Product:  {product_id}")
    print(f"  Quantity: {sales} units")
    print(f"  Revenue:  ${revenue:.2f}")
    print(f"  Price:    ${price:.2f}")
    print()

def main():
    """Main CDC simulator"""
    print_header("CDC Simulator - Event-Driven Agent System")

    logger.info("CDC Simulator started")
    print(colored("Connecting to Kafka broker: " + KAFKA_BROKER, Colors.YELLOW))

    # Get products from database
    products = get_products_from_db()
    if not products:
        logger.error("No products found in database")
        print(colored("✗ No products found in database", Colors.RED))
        print(colored("Make sure to run: python create_db.py", Colors.YELLOW))
        return

    print(colored(f"✓ Found {len(products)} products", Colors.GREEN))
    print(f"  Products: {', '.join(products[:5])}{'...' if len(products) > 5 else ''}\n")

    print_header("Starting Sales Event Simulation")
    print(colored("Press Ctrl+C to stop the simulator\n", Colors.YELLOW))

    event_count = 0
    try:
        while True:
            # Simulate sales for each product
            for product_id in products:
                # Generate realistic sales variability
                is_peak_time = random.random() < 0.3  # 30% peak time
                sales = random.randint(10, 100) if is_peak_time else random.randint(5, 30)
                price = random.uniform(50, 200)
                revenue = sales * price

                result = simulate_sale(product_id, sales, revenue, price)
                if result:
                    event_count += 1
                    print_event(product_id, sales, revenue, price)

                time.sleep(0.5)  # Small delay between events

            print(colored(f"Batch complete ({event_count} total events). Waiting 30 seconds...\n", Colors.BLUE))
            time.sleep(30)

    except KeyboardInterrupt:
        print("\n")
        print_header("CDC Simulator Stopped")
        logger.info(f"CDC Simulator stopped. Total events emitted: {event_count}")
        print(colored(f"Total events emitted: {event_count}", Colors.GREEN))
        print(colored("Check Neo4j for reasoning traces: http://localhost:7474", Colors.YELLOW))
        print()
    finally:
        producer.flush()

if __name__ == "__main__":
    main()
