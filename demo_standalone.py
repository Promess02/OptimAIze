#!/usr/bin/env python3
"""
Standalone Demo - Event-Driven Multi-Agent ML System
Demonstrates the architecture without requiring Docker/Kafka
"""
import sqlite3
import json
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

# Mock implementations to demonstrate the flow

class MockRedis:
    """Mock Redis for caching"""
    def __init__(self):
        self.store = {}
    
    def set(self, key, value):
        self.store[key] = value
    
    def get(self, key):
        return self.store.get(key)
    
    def setex(self, key, ttl, value):
        self.store[key] = value

class MockNeo4j:
    """Mock Neo4j for reasoning traces"""
    def __init__(self):
        self.traces = []
    
    def log_trace(self, agent, product_id, reasoning, result):
        self.traces.append({
            'timestamp': datetime.now().isoformat(),
            'agent': agent,
            'product_id': product_id,
            'reasoning': reasoning,
            'result': result
        })
        print(f"[Neo4j] Logged trace for {agent} on {product_id}")

class TrainingService:
    """Training Service - Trains XGBoost-like models"""
    def __init__(self, db_path, redis_client):
        self.db_path = db_path
        self.redis = redis_client
        self.models = {}
    
    def train_all_models(self):
        """Train models for all products"""
        print("\n" + "="*60)
        print("TRAINING SERVICE: Starting model training")
        print("="*60)
        
        conn = sqlite3.connect(self.db_path)
        
        # Get top 10 products
        query = """
            SELECT product_id, COUNT(*) as record_count
            FROM sales_aggregated 
            GROUP BY product_id 
            HAVING COUNT(*) >= 5
            ORDER BY COUNT(*) DESC 
            LIMIT 10
        """
        products = pd.read_sql(query, conn)
        
        trained_count = 0
        for _, row in products.iterrows():
            product_id = row['product_id']
            
            # Get historical data
            sales_query = f"SELECT date, sales FROM sales_aggregated WHERE product_id = '{product_id}'"
            df_sales = pd.read_sql(sales_query, conn)
            
            if len(df_sales) >= 5:
                # Simple mock training (using mean as prediction)
                avg_sales = df_sales['sales'].mean()
                
                model_data = {
                    'product_id': product_id,
                    'avg_daily_sales': float(avg_sales),
                    'trained_at': datetime.now().isoformat(),
                    'records_used': len(df_sales)
                }
                
                # Store in Redis
                self.redis.set(f"model:{product_id}", json.dumps(model_data))
                self.models[product_id] = model_data
                
                trained_count += 1
                print(f"  ✓ Trained model for {product_id} (avg: {avg_sales:.2f} units/day)")
        
        conn.close()
        
        print(f"\n✓ Training completed: {trained_count} models trained")
        return trained_count

class DemandAgent:
    """Demand Agent - Forecasts product demand"""
    def __init__(self, redis_client, neo4j_client):
        self.redis = redis_client
        self.neo4j = neo4j_client
    
    def predict_demand(self, product_id, horizon_days=60):
        """Make demand prediction"""
        print(f"\n[DEMAND AGENT] Predicting demand for {product_id}")
        
        # Get model from Redis
        model_data = self.redis.get(f"model:{product_id}")
        if not model_data:
            print(f"  ✗ No model found for {product_id}")
            return None
        
        model = json.loads(model_data)
        avg_daily_sales = model['avg_daily_sales']
        
        # Predict demand for horizon
        predicted_demand = int(avg_daily_sales * horizon_days)
        
        reasoning = f"Using trained model with avg daily sales {avg_daily_sales:.2f}. " \
                   f"Forecasting {horizon_days} days ahead."
        
        result = {
            'product_id': product_id,
            'predicted_demand': predicted_demand,
            'confidence_score': 0.85,
            'horizon_days': horizon_days
        }
        
        # Cache result
        self.redis.setex(f"prediction:{product_id}", 3600, json.dumps(result))
        
        # Log to Neo4j
        self.neo4j.log_trace('DemandAgent', product_id, reasoning, result)
        
        print(f"  ✓ Predicted demand: {predicted_demand} units")
        return result

class InventoryAgent:
    """Inventory Agent - Optimizes stock levels"""
    def __init__(self, redis_client, neo4j_client, db_path):
        self.redis = redis_client
        self.neo4j = neo4j_client
        self.db_path = db_path
    
    def calculate_order(self, product_id, predicted_demand):
        """Calculate order quantity"""
        print(f"\n[INVENTORY AGENT] Calculating order for {product_id}")
        
        # Get current stock from database
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT current_stock FROM inventory WHERE product_id = ?", (product_id,))
        row = cursor.fetchone()
        conn.close()
        
        current_stock = row[0] if row else 0
        
        # Calculate order quantity
        order_quantity = max(0, predicted_demand - current_stock)
        
        reasoning = f"Current stock: {current_stock}, Predicted demand: {predicted_demand}. " \
                   f"Order quantity: {order_quantity}"
        
        result = {
            'product_id': product_id,
            'order_quantity': order_quantity,
            'current_stock': current_stock,
            'action': 'ORDERED_STOCK'
        }
        
        # Cache result
        self.redis.setex(f"order:{product_id}", 3600, json.dumps(result))
        
        # Log to Neo4j
        self.neo4j.log_trace('InventoryAgent', product_id, reasoning, result)
        
        print(f"  ✓ Order quantity: {order_quantity} units (current stock: {current_stock})")
        return result

class PricingAgent:
    """Pricing Agent - Dynamically adjusts prices"""
    def __init__(self, redis_client, neo4j_client):
        self.redis = redis_client
        self.neo4j = neo4j_client
    
    def calculate_price(self, product_id, inventory_level):
        """Calculate dynamic price"""
        print(f"\n[PRICING AGENT] Calculating price for {product_id}")
        
        base_price = 99.99
        
        # Dynamic pricing logic
        if inventory_level > 1000:
            new_price = base_price * 0.85  # Discount for high inventory
            reason = "High inventory - applying discount"
        elif inventory_level < 100:
            new_price = base_price * 1.15  # Premium for low inventory
            reason = "Low inventory - applying premium"
        else:
            new_price = base_price
            reason = "Normal inventory - standard pricing"
        
        reasoning = f"Inventory level: {inventory_level}. {reason}"
        
        result = {
            'product_id': product_id,
            'new_price': round(new_price, 2),
            'base_price': base_price,
            'reason': reason
        }
        
        # Cache result
        self.redis.setex(f"price:{product_id}", 3600, json.dumps(result))
        
        # Log to Neo4j
        self.neo4j.log_trace('PricingAgent', product_id, reasoning, result)
        
        print(f"  ✓ New price: ${new_price:.2f} (base: ${base_price:.2f})")
        return result

class Orchestrator:
    """Orchestrator - Coordinates agent workflows"""
    def __init__(self, training_service, demand_agent, inventory_agent, pricing_agent):
        self.training = training_service
        self.demand = demand_agent
        self.inventory = inventory_agent
        self.pricing = pricing_agent
    
    def handle_cdc_event(self, event_type, product_id):
        """Handle CDC event and trigger agent workflows"""
        print(f"\n{'='*60}")
        print(f"ORCHESTRATOR: Handling {event_type} for {product_id}")
        print("="*60)
        
        if event_type in ['SalesCreated', 'InventoryUpdated']:
            # Trigger retraining
            print("\n→ Triggering model retraining...")
            self.training.train_all_models()
            
            # Trigger demand prediction
            print("\n→ Triggering demand prediction...")
            demand_result = self.demand.predict_demand(product_id)
            
            if demand_result:
                # Trigger inventory calculation
                print("\n→ Triggering inventory calculation...")
                inventory_result = self.inventory.calculate_order(
                    product_id, 
                    demand_result['predicted_demand']
                )
                
                # Trigger pricing
                print("\n→ Triggering dynamic pricing...")
                pricing_result = self.pricing.calculate_price(
                    product_id,
                    inventory_result['order_quantity']
                )
                
                return {
                    'demand': demand_result,
                    'inventory': inventory_result,
                    'pricing': pricing_result
                }
        
        return None

def main():
    """Main demo runner"""
    print("\n" + "="*60)
    print("EVENT-DRIVEN MULTI-AGENT ML SYSTEM - STANDALONE DEMO")
    print("="*60)
    
    # Initialize components
    redis_client = MockRedis()
    neo4j_client = MockNeo4j()
    db_path = 'ecommerce.db'
    
    # Initialize services
    training_service = TrainingService(db_path, redis_client)
    demand_agent = DemandAgent(redis_client, neo4j_client)
    inventory_agent = InventoryAgent(redis_client, neo4j_client, db_path)
    pricing_agent = PricingAgent(redis_client, neo4j_client)
    
    # Initialize orchestrator
    orchestrator = Orchestrator(training_service, demand_agent, inventory_agent, pricing_agent)
    
    # Get a sample product from those that will be trained
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT product_id 
        FROM sales_aggregated 
        GROUP BY product_id 
        HAVING COUNT(*) >= 5 
        ORDER BY COUNT(*) DESC
        LIMIT 1
    """)
    sample_product = cursor.fetchone()[0]
    conn.close()
    
    print(f"\nUsing sample product: {sample_product}")
    
    # Simulate CDC event
    print("\n" + "="*60)
    print("SIMULATING CDC EVENT: SalesCreated")
    print("="*60)
    
    result = orchestrator.handle_cdc_event('SalesCreated', sample_product)
    
    # Print summary
    print("\n" + "="*60)
    print("WORKFLOW SUMMARY")
    print("="*60)
    
    if result:
        print(f"\nProduct: {sample_product}")
        print(f"  Predicted Demand: {result['demand']['predicted_demand']} units")
        print(f"  Order Quantity: {result['inventory']['order_quantity']} units")
        print(f"  New Price: ${result['pricing']['new_price']}")
        print(f"  Current Stock: {result['inventory']['current_stock']}")
    
    # Show Neo4j traces
    print("\n" + "="*60)
    print("NEO4J REASONING TRACES")
    print("="*60)
    
    for i, trace in enumerate(neo4j_client.traces, 1):
        print(f"\n{i}. [{trace['agent']}] @ {trace['timestamp']}")
        print(f"   Product: {trace['product_id']}")
        print(f"   Reasoning: {trace['reasoning']}")
    
    print("\n" + "="*60)
    print("DEMO COMPLETED SUCCESSFULLY!")
    print("="*60)
    print("\nThis demonstrates the event-driven architecture where:")
    print("1. CDC events trigger model retraining")
    print("2. Trained models enable demand prediction")
    print("3. Predictions drive inventory decisions")
    print("4. Inventory levels trigger dynamic pricing")
    print("5. All reasoning is logged to Neo4j for auditability")
    print("\nTo run the full system with Docker:")
    print("  docker-compose up --build")
    print()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
