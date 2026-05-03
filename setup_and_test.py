#!/usr/bin/env python3
"""
Comprehensive setup and end-to-end test for Event-Driven Multi-Agent ML System
Tests the complete dataflow: SQLite -> Demand Agent -> Inventory -> Pricing -> SQLite
"""

import subprocess
import time
import sys
import sqlite3
import json
import re
import requests
import pandas as pd
from datetime import datetime

TOP_PRODUCTS_LIMIT = 10

class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    RESET = '\033[0m'
    BOLD = '\033[1m'

def print_header(title):
    print(f"\n{Colors.BOLD}{Colors.BLUE}{'='*70}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.BLUE}{title:^70}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.BLUE}{'='*70}{Colors.RESET}\n")

def print_step(step, description):
    print(f"{Colors.BOLD}{Colors.YELLOW}[{step}] {description}{Colors.RESET}")

def print_success(message):
    print(f"{Colors.GREEN}✓ {message}{Colors.RESET}")

def print_error(message):
    print(f"{Colors.RED}✗ {message}{Colors.RESET}")

def print_info(message):
    print(f"{Colors.BLUE}ℹ {message}{Colors.RESET}")

def run_command(cmd, description, show_output=False):
    """Run a shell command"""
    try:
        if show_output:
            result = subprocess.run(cmd, shell=True, check=True)
        else:
            result = subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)
        print_success(description)
        return True
    except subprocess.CalledProcessError as e:
        print_error(f"{description} failed")
        if not show_output and e.stderr:
            print(f"  Error: {e.stderr[:200]}")
        return False

def print_progress_bar(current, total, prefix="Progress", width=40):
    """Render an inline progress bar"""
    total = max(total, 1)
    ratio = min(max(current / total, 0), 1)
    filled = int(width * ratio)
    bar = "#" * filled + "-" * (width - filled)
    percent = int(ratio * 100)
    end = "\n" if current >= total else ""
    print(f"\r{Colors.BLUE}{prefix}: [{bar}] {current}/{total} ({percent} %){Colors.RESET}", end=end, flush=True)

def check_cached_models(products, horizon_days=1):
    """Check if demand models are already cached by probing predict endpoint."""
    cached = []
    missing = []

    print_step("4", "Checking for previously trained demand models...")
    for idx, product_id in enumerate(products, start=1):
        try:
            response = requests.post(
                "http://localhost:8001/predict",
                json={"product_id": product_id, "horizon_days": horizon_days},
                timeout=8,
            )
            if response.status_code == 200:
                cached.append(product_id)
            else:
                missing.append(product_id)
        except Exception:
            missing.append(product_id)

        print_progress_bar(idx, len(products), prefix="Model cache scan")

    print_info(f"Cached models: {len(cached)}/{len(products)}")
    return cached, missing

def run_pretraining_with_progress(limit=TOP_PRODUCTS_LIMIT):
    """Run training service in batch mode and parse progress."""
    cmd = (
        "cd /home/miko/magister && "
        "docker-compose run --rm "
        f"-e TOP_PRODUCTS_LIMIT={int(limit)} "
        "-e FORCE_RETRAIN=0 "
        "-e DB_PATH=/data/ecommerce.db "
        "-e REDIS_HOST=redis_demand "
        "-e REDIS_PORT=6379 "
        "training_service python /data/services/training/main.py --batch-train"
    )

    try:
        process = subprocess.Popen(
            cmd,
            shell=True,
            cwd="/home/miko/magister",
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        last_current, last_total = 0, int(limit)
        progress_pattern = re.compile(r"PROGRESS:(\d+)/(\d+)")

        for raw_line in process.stdout:
            line = raw_line.strip()
            match = progress_pattern.search(line)
            if match:
                last_current = int(match.group(1))
                last_total = int(match.group(2))
                print_progress_bar(last_current, last_total, prefix="First training")
                continue

            if line:
                print_info(line)

        return_code = process.wait()
        if return_code == 0:
            print_progress_bar(last_total, last_total, prefix="First training")
            print_success("Top products pre-training")
            return True

        print_error("Top products pre-training failed")
        return False
    except Exception as e:
        print_error(f"Top products pre-training failed: {e}")
        return False

def check_database():
    """Check if database was created correctly"""
    try:
        conn = sqlite3.connect('ecommerce.db')
        cursor = conn.cursor()

        # Check inventory table structure
        cursor.execute("PRAGMA table_info(inventory);")
        columns = {row[1]: row[2] for row in cursor.fetchall()}

        if 'product_id' not in columns:
            print_error("Missing product_id column in inventory table")
            return False
        if 'current_stock' not in columns:
            print_error("Missing current_stock column in inventory table")
            return False
        if 'price' not in columns:
            print_error("Missing price column in inventory table")
            return False

        # Get sample data
        cursor.execute("SELECT COUNT(*) FROM inventory;")
        inv_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM sales_aggregated;")
        sales_count = cursor.fetchone()[0]

        conn.close()

        print_success(f"Database structure verified")
        print_info(f"  - Inventory records: {inv_count}")
        print_info(f"  - Sales records: {sales_count}")

        return inv_count > 0 and sales_count > 0

    except Exception as e:
        print_error(f"Database check failed: {e}")
        return False

def await_service(port, max_retries=30):
    """Wait for a service to be ready"""
    for i in range(max_retries):
        try:
            response = requests.get(f"http://localhost:{port}/health", timeout=2)
            if response.status_code == 200:
                return True
        except:
            pass
        time.sleep(1)
    return False

def test_agent_health():
    """Test all agent health endpoints"""
    agents = {
        'demand': 8001,
        'inventory': 8002,
        'pricing': 8003
    }

    print_header("Testing Agent Health Checks")

    all_healthy = True
    for agent_name, port in agents.items():
        print_step("Health", f"Checking {agent_name.capitalize()} Agent (port {port})...")
        try:
            response = requests.get(f"http://localhost:{port}/health", timeout=5)
            if response.status_code == 200:
                data = response.json()
                print_success(f"{agent_name.capitalize()} Agent is healthy")
                print_info(f"  Response: {data}")
            else:
                print_error(f"{agent_name.capitalize()} Agent returned {response.status_code}")
                all_healthy = False
        except requests.exceptions.RequestException as e:
            print_error(f"{agent_name.capitalize()} Agent is not responding: {e}")
            all_healthy = False

    return all_healthy

def test_demand_prediction(product_id="PROD001"):
    """Test demand prediction"""
    print_header("Testing Demand Prediction")

    payload = {
        "product_id": product_id,
        "horizon_days": 30
    }

    try:
        response = requests.post(
            "http://localhost:8001/predict",
            json=payload,
            timeout=15
        )

        if response.status_code == 200:
            result = response.json()
            print_success("Demand prediction successful")
            print_info(f"  Product ID: {result.get('product_id')}")
            print_info(f"  Predicted Demand: {result.get('predicted_demand')} units")
            print_info(f"  Confidence Score: {result.get('confidence_score')}")
            return result
        else:
            print_error(f"Demand prediction failed with status {response.status_code}")
            print(f"  Response: {response.text}")
            return None
    except Exception as e:
        print_error(f"Demand prediction error: {e}")
        return None

def test_inventory_order(product_id="PROD001", predicted_demand=500):
    """Test inventory ordering"""
    print_header("Testing Inventory Order")

    payload = {
        "product_id": product_id,
        "predicted_demand": predicted_demand
    }

    try:
        response = requests.post(
            "http://localhost:8002/order",
            json=payload,
            timeout=15
        )

        if response.status_code == 200:
            result = response.json()
            print_success("Inventory order calculation successful")
            print_info(f"  Product ID: {result.get('product_id')}")
            print_info(f"  Current Stock: {result.get('current_stock')} units")
            print_info(f"  Order Quantity: {result.get('order_quantity')} units")
            print_info(f"  Projected Stock: {result.get('projected_stock', 'N/A')} units")
            return result
        else:
            print_error(f"Inventory order failed with status {response.status_code}")
            print(f"  Response: {response.text}")
            return None
    except Exception as e:
        print_error(f"Inventory order error: {e}")
        return None

def test_dynamic_pricing(product_id="PROD001", inventory_level=500):
    """Test dynamic pricing"""
    print_header("Testing Dynamic Pricing")

    payload = {
        "product_id": product_id,
        "inventory_level": inventory_level
    }

    try:
        response = requests.post(
            "http://localhost:8003/price",
            json=payload,
            timeout=15
        )

        if response.status_code == 200:
            result = response.json()
            print_success("Dynamic pricing calculation successful")
            print_info(f"  Product ID: {result.get('product_id')}")
            print_info(f"  New Price: ${result.get('new_price'):.2f}")
            print_info(f"  Inventory Level: {result.get('inventory_level')} units")
            print_info(f"  Reason: {result.get('reason')}")
            return result
        else:
            print_error(f"Dynamic pricing failed with status {response.status_code}")
            print(f"  Response: {response.text}")
            return None
    except Exception as e:
        print_error(f"Dynamic pricing error: {e}")
        return None

def verify_database_updates(product_ids=None):
    """Verify that prices were updated in the database"""
    print_header("Verifying Database Updates")

    try:
        conn = sqlite3.connect('ecommerce.db')
        df = pd.read_sql("SELECT * FROM inventory LIMIT 5", conn)
        conn.close()

        print_success("Database verification completed")
        print_info("Inventory table (sample):")
        print(df.to_string(index=False))

        # Check if prices were updated
        if df['price'].notna().all():
            print_success("All products have price values in database")
            return True
        else:
            print_error("Some products missing price values")
            return False

    except Exception as e:
        print_error(f"Database verification failed: {e}")
        return False

def check_containers_status():
    """Check status of running containers"""
    print_header("Checking Container Status")

    try:
        result = subprocess.run(
            "docker-compose ps",
            shell=True,
            capture_output=True,
            text=True,
            cwd="/home/miko/magister"
        )

        print(result.stdout)

        if "Up" in result.stdout:
            print_success("Docker containers are running")
            return True
        else:
            print_error("Some containers are not running")
            return False

    except Exception as e:
        print_error(f"Failed to check container status: {e}")
        return False


def get_top_products(limit=TOP_PRODUCTS_LIMIT):
    """Get top products by sales for quick test mode"""
    try:
        conn = sqlite3.connect('ecommerce.db')
        query = """
            SELECT product_id, SUM(sales) as total_sales
            FROM sales_aggregated
            GROUP BY product_id
            ORDER BY total_sales DESC
            LIMIT ?
        """
        df = pd.read_sql(query, conn, params=(int(limit),))
        conn.close()
        return df['product_id'].astype(str).tolist()
    except Exception as e:
        print_error(f"Failed to fetch top products: {e}")
        return []

def main():
    """Main test runner"""
    print_header("Event-Driven Multi-Agent ML System Setup & Test")

    print_step("1", "Initializing database...")
    if not run_command("python /home/miko/magister/create_db.py", "Database initialization"):
        print_error("Database initialization failed. Exiting.")
        return False

    if not check_database():
        print_error("Database verification failed. Exiting.")
        return False

    time.sleep(2)

    print_header("Checking Docker Status")
    if not check_containers_status():
        print_info("Docker containers not running. This is expected for initial setup.")
        print_step("2", "Starting Docker containers...")
        print_info("Run: cd /home/miko/magister && docker-compose up --build")
        print_info("In a separate terminal, run: python /home/miko/magister/setup_and_test.py continue")
        return True

    time.sleep(5)

    print_step("3", "Waiting for agents to be ready...")
    agents_ready = True
    for agent_name, port in [('Demand', 8001), ('Inventory', 8002), ('Pricing', 8003), ('Procurement', 8004)]:
        print_info(f"Waiting for {agent_name} Agent (port {port})...")
        if await_service(port):
            print_success(f"{agent_name} Agent is ready")
        else:
            print_error(f"{agent_name} Agent failed to start")
            agents_ready = False

    if not agents_ready:
        print_error("Not all agents are ready. Check docker-compose logs.")
        return False

    time.sleep(2)

    top_products = get_top_products(TOP_PRODUCTS_LIMIT)
    if not top_products:
        print_error("Could not resolve top products list. Exiting.")
        return False

    print_info(f"Using top products scope: {top_products}")

    cached_models, missing_models = check_cached_models(top_products)
    if missing_models:
        print_info(f"Need to train missing models: {missing_models}")
        if not run_pretraining_with_progress(TOP_PRODUCTS_LIMIT):
            print_error("Top products pre-training failed. Exiting.")
            return False
    else:
        print_success("All demand models already available in cache. Skipping first training.")

    # Run comprehensive tests
    all_tests_passed = True

    if not test_agent_health():
        all_tests_passed = False

    print_header(f"Testing end-to-end flow for TOP {TOP_PRODUCTS_LIMIT} products")
    for product_id in top_products:
        print_info(f"Running flow for {product_id}")

        demand_result = test_demand_prediction(product_id)
        if not demand_result:
            all_tests_passed = False
            continue

        predicted_demand = demand_result.get('predicted_demand', 500)
        inventory_result = test_inventory_order(product_id, predicted_demand)
        if not inventory_result:
            all_tests_passed = False
            continue

        inventory_level = inventory_result.get('projected_stock', inventory_result.get('current_stock', 250))
        pricing_result = test_dynamic_pricing(product_id, inventory_level)
        if not pricing_result:
            all_tests_passed = False

        time.sleep(0.3)

    # Verify database updates
    time.sleep(2)
    if not verify_database_updates():
        all_tests_passed = False

    test_product_id = top_products[0]

    print_header("Testing Procurement Agent")
    if not run_command(
        f"TEST_PRODUCT_ID={test_product_id} python /home/miko/magister/test_procurement_agent.py",
        "Procurement behavior tests",
        show_output=True,
    ):
        all_tests_passed = False

    print_header("Testing Kafka Integration")
    if not run_command(
        f"TEST_PRODUCT_ID={test_product_id} python /home/miko/magister/test_kafka_procurement.py",
        "Kafka CDC to procurement integration test",
        show_output=True,
    ):
        all_tests_passed = False

    # Final summary
    print_header("Test Summary")
    if all_tests_passed:
        print_success("All tests passed!")
        print("\n" + Colors.BOLD + "Next Steps:" + Colors.RESET)
        print("1. Access Neo4j Browser: http://localhost:7474 (user: neo4j, password: password)")
        print("2. Run CDC simulator: python /home/miko/magister/shared/cdc_simulator.py")
        print("3. View logs: docker-compose logs -f [service_name]")
        print("4. Inspect database: sqlite3 ecommerce.db")
        print("5. Re-run procurement tests: python /home/miko/magister/test_procurement_agent.py")
        print("6. Re-run Kafka test: python /home/miko/magister/test_kafka_procurement.py")
        return True
    else:
        print_error("Some tests failed. Check output above.")
        return False

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print_error("\n\nTests interrupted by user")
        sys.exit(1)
