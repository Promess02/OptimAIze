import os
import json
import sqlite3
import math
import threading
from datetime import datetime
from uuid import uuid4
from confluent_kafka import Consumer, Producer
try:
    from crewai import Agent, Task, Crew, Process
except Exception:
    class Agent:
        def __init__(self, *args, **kwargs):
            pass

    class Task:
        def __init__(self, description="", expected_output="", agent=None):
            self.description = description
            self.expected_output = expected_output
            self.agent = agent

    class Crew:
        def __init__(self, agents=None, tasks=None, process=None):
            self.agents = agents or []
            self.tasks = tasks or []
            self.process = process

        def kickoff(self):
            if self.tasks:
                return f"Fallback planning: {self.tasks[0].description}"
            return "Fallback planning"

    class Process:
        sequential = "sequential"
try:
    from langchain_community.tools import tool
except Exception:
    try:
        from langchain.tools import tool
    except Exception:
        def tool(_name):
            def decorator(func):
                return func
            return decorator
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn
import redis
from neo4j import GraphDatabase

app = FastAPI(title="Inventory Agent API")

KAFKA_BROKER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
REDIS_HOST = os.getenv('REDIS_HOST', 'redis_inventory')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
NEO4J_URI = os.getenv('NEO4J_URI', 'bolt://neo4j:7687')
DB_PATH = os.getenv('DB_PATH', './ecommerce.db')

redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=("neo4j", os.getenv("NEO4J_PASSWORD", "password")))

class OrderRequest(BaseModel):
    product_id: str
    predicted_demand: int

@tool("aktualizuj_stan_magazynowy")
def aktualizuj_stan_magazynowy(sku: str, quantity_delta: int) -> str:
    """Narzędzie do aktualizacji stanu magazynowego produktu."""
    return f"Zaktualizowano stan dla {sku} o {quantity_delta} jednostek."

@tool("weryfikuj_dostawce")
def weryfikuj_dostawce(dostawca_id: str) -> str:
    """Weryfikuje dostawcę w łańcuchu dostaw."""
    return f"Dostawca {dostawca_id} zweryfikowany pozytywnie."

zaopatrzeniowiec = Agent(
    role="Zaopatrzeniowiec",
    goal="Optymalizacja stanów magazynowych i łańcucha dostaw.",
    backstory="Zarządzasz zaopatrzeniem. Analizujesz predykcje od analityków i podejmujesz decyzje o domówieniu towaru lub aktualizacji zapasów w magazynie głównym.",
    verbose=True,
    allow_delegation=False,
    tools=[aktualizuj_stan_magazynowy, weryfikuj_dostawce]
)

producer_conf = {'bootstrap.servers': KAFKA_BROKER}
producer = Producer(producer_conf)

consumer_conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'inventory_agent_group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumer_conf)
consumer.subscribe(['demand_predictions', 'database_cdc.sales'])

def delivery_report(err, msg):
    if err is not None:
        pass
    else:
        print(f"Zaktualizowano {msg.topic()} [{msg.partition()}]")

def log_to_neo4j(product_id, reasoning_trace, order_quantity):
    """Log reasoning trace to Neo4j"""
    try:
        with neo4j_driver.session() as session:
            session.run("""
                MERGE (p:Product {id: $product_id})
                CREATE (o:OrderDecision {
                    quantity: $quantity,
                    timestamp: datetime(),
                    reasoning: $reasoning
                })
                CREATE (p)-[:HAS_ORDER]->(o)
            """, product_id=product_id, quantity=order_quantity, reasoning=str(reasoning_trace))
    except Exception as e:
        print(f"Neo4j logging error: {e}")

def update_inventory_in_db(product_id, new_stock):
    """Update inventory stock level in SQLite database"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute("""
            UPDATE inventory
            SET current_stock = ?
            WHERE product_id = ?
        """, (int(new_stock), product_id))

        conn.commit()
        conn.close()

        print(f"[Inventory Agent] Updated stock for {product_id} to {new_stock} in database")
        return True
    except Exception as e:
        print(f"[Inventory Agent] Error updating inventory: {e}")
        return False

def get_current_inventory_db(product_id):
    """Get current inventory from database"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute("SELECT current_stock FROM inventory WHERE product_id = ?", (product_id,))
        row = cursor.fetchone()
        conn.close()

        return int(row[0]) if row else 100
    except Exception as e:
        print(f"[Inventory Agent] Error reading inventory: {e}")
        return 100

def calculate_order(product_id, predicted_demand):
    """Calculate order quantity based on prediction"""
    try:
        current_stock = get_current_inventory_db(product_id)

        order_quantity = max(0, predicted_demand - current_stock)

        projected_stock = current_stock + order_quantity

        if order_quantity > 0:
            update_inventory_in_db(product_id, projected_stock)

        redis_client.setex(f"order:{product_id}", 3600, json.dumps({
            "product_id": product_id,
            "order_quantity": order_quantity,
            "current_stock": current_stock,
            "projected_stock": projected_stock,
            "predicted_demand": predicted_demand
        }))

        return {
            "product_id": product_id,
            "order_quantity": order_quantity,
            "current_stock": current_stock,
            "projected_stock": projected_stock,
            "action": "ORDERED_STOCK"
        }
    except Exception as e:
        return {"error": str(e)}

@app.post("/order")
def order_endpoint(request: OrderRequest):
    """HTTP endpoint for inventory ordering"""
    result = calculate_order(request.product_id, request.predicted_demand)
    if "error" in result:
        raise HTTPException(status_code=500, detail=result["error"])
    return result

@app.get("/health")
def health():
    return {"status": "healthy", "agent": "inventory"}

def kafka_consumer_loop():
    """Kafka consumer running in background thread"""
    print("Agent Magazynowy (Zaopatrzeniowiec - CrewAI) uruchomiony...")
    
    try:
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None: continue
            if msg.error(): continue

            topic = msg.topic()
            data = json.loads(msg.value().decode('utf-8'))
            
            if topic == 'demand_predictions':
                print(f"[Zaopatrzeniowiec] Otrzymano predykcje by uzupełnić zapasy: {data}")
                product_id = data.get("product_id")
                pred = data.get("predicted_demand", 100)
                
                zamowienie_task = Task(
                    description=f"Otrzymałeś predykcję zapotrzebowania {pred} na produkt {product_id}. Zweryfikuj dostępnych dostawców i zleć aktualizację wymaganego stanu magazynowego na następny okres.",
                    expected_output="Decyzja o ilości zamówienia oraz potwierdzenie użycia narzędzi",
                    agent=zaopatrzeniowiec
                )

                crew = Crew(
                    agents=[zaopatrzeniowiec],
                    tasks=[zamowienie_task],
                    process=Process.sequential
                )
                wynik = crew.kickoff()
                
                order_result = calculate_order(product_id, pred)
                order_quantity = order_result.get('order_quantity', pred)
                
                inventory_update = {
                    "event_id": str(uuid4()),
                    "timestamp": datetime.utcnow().isoformat(),
                    "source_agent": "inventory_agent",
                    "product_id": product_id,
                    "action": "ORDERED_STOCK",
                    "quantity": order_quantity,
                    "predicted_demand": pred,
                    "workflow_trace": str(wynik)
                }
                
                log_to_neo4j(product_id, wynik, order_quantity)
                
                producer.produce(
                    'inventory_updates', 
                    key=product_id.encode('utf-8'),
                    value=json.dumps(inventory_update).encode('utf-8'),
                    callback=delivery_report
                )
                producer.poll(0)

            elif topic == 'database_cdc.sales':
                print(f"[Zaopatrzeniowiec] Zarejestrowano sprzedaż produktu {data.get('product_id')}, odejmuję z lokalnego zapasu.")

    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"Exception: {e}")
    finally:
        consumer.close()
        producer.flush()

if __name__ == "__main__":
    kafka_thread = threading.Thread(target=kafka_consumer_loop, daemon=True)
    kafka_thread.start()
    
    uvicorn.run(app, host="0.0.0.0", port=8002)
