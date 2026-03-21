#!/usr/bin/env python3
"""
Test script for Event-Driven Multi-Agent ML System
"""
import requests
import json
import time
import sys

BASE_URLS = {
    'demand': 'http://localhost:8001',
    'inventory': 'http://localhost:8002',
    'pricing': 'http://localhost:8003'
}

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

def test_demand_prediction():
    """Test demand prediction endpoint"""
    print("=" * 60)
    print("Testing Demand Prediction")
    print("=" * 60)
    
    test_product = "PROD001"
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
        else:
            print(f"✗ Prediction failed: {response.status_code}")
            print(f"  Response: {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"✗ Connection failed: {e}")
        return None
    finally:
        print()

def test_inventory_order():
    """Test inventory order endpoint"""
    print("=" * 60)
    print("Testing Inventory Order")
    print("=" * 60)
    
    test_product = "PROD001"
    payload = {
        "product_id": test_product,
        "predicted_demand": 500
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

def test_dynamic_pricing():
    """Test dynamic pricing endpoint"""
    print("=" * 60)
    print("Testing Dynamic Pricing")
    print("=" * 60)
    
    test_product = "PROD001"
    payload = {
        "product_id": test_product,
        "inventory_level": 250
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
    
    test_product = "PROD001"
    
    # Step 1: Get demand prediction
    print(f"Step 1: Predicting demand for {test_product}...")
    demand_result = test_demand_prediction()
    
    if not demand_result:
        print("✗ Workflow failed at demand prediction step")
        return
    
    time.sleep(1)
    
    # Step 2: Calculate inventory order
    print(f"Step 2: Calculating inventory order...")
    predicted_demand = demand_result.get('predicted_demand', 500)
    inventory_result = test_inventory_order()
    
    if not inventory_result:
        print("✗ Workflow failed at inventory order step")
        return
    
    time.sleep(1)
    
    # Step 3: Calculate dynamic price
    print(f"Step 3: Calculating dynamic price...")
    order_quantity = inventory_result.get('order_quantity', 250)
    pricing_result = test_dynamic_pricing()
    
    if not pricing_result:
        print("✗ Workflow failed at pricing step")
        return
    
    print("=" * 60)
    print("✓ Full Workflow Completed Successfully!")
    print("=" * 60)
    print(f"Summary for {test_product}:")
    print(f"  Predicted Demand: {demand_result.get('predicted_demand')}")
    print(f"  Order Quantity: {inventory_result.get('order_quantity')}")
    print(f"  New Price: ${pricing_result.get('new_price')}")
    print()

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
    
    test_full_workflow()
    
    print("=" * 60)
    print("Test Suite Completed")
    print("=" * 60)
    print("\nNext Steps:")
    print("1. Check Neo4j Browser at http://localhost:7474")
    print("2. Run CDC simulator: python shared/cdc_simulator.py")
    print("3. View logs: docker-compose logs -f [service_name]")
    print()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nTests interrupted by user")
        sys.exit(0)
