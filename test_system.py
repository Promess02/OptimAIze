#!/usr/bin/env python3
"""
Test script for Event-Driven Multi-Agent ML System
"""
import requests
import json
import time
import sys
import subprocess
import sqlite3

BASE_URLS = {
    'demand': 'http://localhost:8001',
    'inventory': 'http://localhost:8002',
    'pricing': 'http://localhost:8003'
}


def get_existing_test_product(default_product="P0001"):
    """Pick an existing top-selling product from local SQLite DB."""
    try:
        conn = sqlite3.connect('/home/miko/magister/ecommerce.db')
        row = conn.execute(
            """
            SELECT product_id
            FROM sales_aggregated
            GROUP BY product_id
            ORDER BY SUM(sales) DESC
            LIMIT 1
            """
        ).fetchone()
        conn.close()
        if row and row[0]:
            return str(row[0])
    except Exception:
        pass
    return default_product


def run_demand_pretraining(limit=10):
    """Pre-train demand models so /predict is available."""
    cmd = (
        "cd /home/miko/magister && "
        "docker-compose run --rm "
        f"-e TOP_PRODUCTS_LIMIT={int(limit)} "
        "-e FORCE_RETRAIN=0 "
        "-e DB_PATH=/data/ecommerce.db "
        "-e REDIS_HOST=redis_demand "
        "-e REDIS_PORT=6379 "
        "training_service python /data/agents/demand/train_top10.py"
    )
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        return result.returncode == 0
    except Exception:
        return False

def test_health_checks():
    """Test health endpoints for all agents"""
    print("=" * 60)
    print("Testing Health Checks")
    print("=" * 60)
    
    for agent, base_url in BASE_URLS.items():
        try:
            response = requests.get(f"{base_url}/health", timeout=5)
            if response.status_code == 200:
                print(f"✓ {agent.capitalize()} Agent: {response.json()}")
            else:
                print(f"✗ {agent.capitalize()} Agent: Failed (HTTP {response.status_code})")
        except requests.exceptions.RequestException as e:
            print(f"✗ {agent.capitalize()} Agent: Connection failed - {e}")
    print()

def test_demand_prediction(test_product=None):
    """Test demand prediction endpoint"""
    print("=" * 60)
    print("Testing Demand Prediction")
    print("=" * 60)
    
    test_product = test_product or get_existing_test_product()
    payload = {
        "product_id": test_product,
        "horizon_days": 60
    }
    
    try:
        response = requests.post(
            f"{BASE_URLS['demand']}/predict",
            json=payload,
            timeout=10
        )
        
        if response.status_code == 200:
            result = response.json()
            print(f"✓ Prediction successful:")
            print(f"  Product: {result.get('product_id')}")
            print(f"  Predicted Demand: {result.get('predicted_demand')}")
            print(f"  Confidence: {result.get('confidence_score')}")
            return result
        elif response.status_code == 404 and "Model not found" in response.text:
            print("ℹ Model not found, running demand pre-training and retrying once...")
            if run_demand_pretraining(limit=10):
                retry = requests.post(
                    f"{BASE_URLS['demand']}/predict",
                    json=payload,
                    timeout=10
                )
                if retry.status_code == 200:
                    result = retry.json()
                    print(f"✓ Prediction successful after pre-training:")
                    print(f"  Product: {result.get('product_id')}")
                    print(f"  Predicted Demand: {result.get('predicted_demand')}")
                    print(f"  Confidence: {result.get('confidence_score')}")
                    return result
                print(f"✗ Retry failed: {retry.status_code}")
                print(f"  Response: {retry.text}")
                return None
            print("✗ Pre-training failed")
            return None
        else:
            print(f"✗ Prediction failed: {response.status_code}")
            print(f"  Response: {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"✗ Connection failed: {e}")
        return None
    finally:
        print()

def test_inventory_order(test_product=None, predicted_demand=500):
    """Test inventory order endpoint"""
    print("=" * 60)
    print("Testing Inventory Order")
    print("=" * 60)
    
    test_product = test_product or get_existing_test_product()
    payload = {
        "product_id": test_product,
        "predicted_demand": int(predicted_demand)
    }
    
    try:
        response = requests.post(
            f"{BASE_URLS['inventory']}/order",
            json=payload,
            timeout=10
        )
        
        if response.status_code == 200:
            result = response.json()
            print(f"✓ Order calculation successful:")
            print(f"  Product: {result.get('product_id')}")
            print(f"  Order Quantity: {result.get('order_quantity')}")
            print(f"  Current Stock: {result.get('current_stock')}")
            print(f"  Action: {result.get('action')}")
            return result
        else:
            print(f"✗ Order failed: {response.status_code}")
            print(f"  Response: {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"✗ Connection failed: {e}")
        return None
    finally:
        print()

def test_dynamic_pricing(test_product=None, inventory_level=250):
    """Test dynamic pricing endpoint"""
    print("=" * 60)
    print("Testing Dynamic Pricing")
    print("=" * 60)
    
    test_product = test_product or get_existing_test_product()
    payload = {
        "product_id": test_product,
        "inventory_level": int(inventory_level)
    }
    
    try:
        response = requests.post(
            f"{BASE_URLS['pricing']}/price",
            json=payload,
            timeout=10
        )
        
        if response.status_code == 200:
            result = response.json()
            print(f"✓ Pricing calculation successful:")
            print(f"  Product: {result.get('product_id')}")
            print(f"  New Price: ${result.get('new_price')}")
            print(f"  Reason: {result.get('reason')}")
            return result
        else:
            print(f"✗ Pricing failed: {response.status_code}")
            print(f"  Response: {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"✗ Connection failed: {e}")
        return None
    finally:
        print()

def test_full_workflow():
    """Test complete workflow: Demand → Inventory → Pricing"""
    print("=" * 60)
    print("Testing Full Workflow")
    print("=" * 60)
    
    test_product = get_existing_test_product()
    
    # Step 1: Get demand prediction
    print(f"Step 1: Predicting demand for {test_product}...")
    demand_result = test_demand_prediction(test_product)
    
    if not demand_result:
        print("✗ Workflow failed at demand prediction step")
        return False
    
    time.sleep(1)
    
    # Step 2: Calculate inventory order
    print(f"Step 2: Calculating inventory order...")
    predicted_demand = demand_result.get('predicted_demand', 500)
    inventory_result = test_inventory_order(test_product, predicted_demand)
    
    if not inventory_result:
        print("✗ Workflow failed at inventory order step")
        return False
    
    time.sleep(1)
    
    # Step 3: Calculate dynamic price
    print(f"Step 3: Calculating dynamic price...")
    inventory_level = inventory_result.get('projected_stock', inventory_result.get('current_stock', 250))
    pricing_result = test_dynamic_pricing(test_product, inventory_level)
    
    if not pricing_result:
        print("✗ Workflow failed at pricing step")
        return False
    
    print("=" * 60)
    print("✓ Full Workflow Completed Successfully!")
    print("=" * 60)
    print(f"Summary for {test_product}:")
    print(f"  Predicted Demand: {demand_result.get('predicted_demand')}")
    print(f"  Order Quantity: {inventory_result.get('order_quantity')}")
    print(f"  New Price: ${pricing_result.get('new_price')}")
    print()
    return True

def main():
    """Main test runner"""
    print("\n" + "=" * 60)
    print("Event-Driven Multi-Agent ML System - Test Suite")
    print("=" * 60 + "\n")
    
    print("Waiting for services to be ready...")
    time.sleep(2)
    
    # Run all tests
    test_health_checks()
    time.sleep(1)
    
    workflow_ok = test_full_workflow()
    
    print("=" * 60)
    print("Test Suite Completed")
    print("=" * 60)
    print("\nNext Steps:")
    print("1. Check Neo4j Browser at http://localhost:7474")
    print("2. Run CDC simulator: python shared/cdc_simulator.py")
    print("3. View logs: docker-compose logs -f [service_name]")
    print()
    return bool(workflow_ok)

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\nTests interrupted by user")
        sys.exit(1)
