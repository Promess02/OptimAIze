import os
import json
import sqlite3
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
                return f"Fallback pricing analysis: {self.tasks[0].description}"
            return "Fallback pricing analysis"

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

from guardrails.constraints import PricingConstraints
from rl_engine.inference import PricingState, RuleBasedRLPolicy
from simulators.demand_sandbox import DemandSandbox

app = FastAPI(title="Pricing Agent API")

KAFKA_BROKER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
REDIS_HOST = os.getenv('REDIS_HOST', 'redis_pricing')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
NEO4J_URI = os.getenv('NEO4J_URI', 'bolt://neo4j:7687')
DB_PATH = os.getenv('DB_PATH', './ecommerce.db')
MODELS_PATH = os.getenv('MODELS_PATH', '/models')
PRICING_MODE = os.getenv('PRICING_MODE', 'rl').lower()

redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=("neo4j", os.getenv("NEO4J_PASSWORD", "password")))

pricing_constraints = PricingConstraints(
    min_margin_pct=float(os.getenv("MIN_MARGIN_PCT", "0.10")),
    max_competitor_gap_pct=float(os.getenv("MAX_COMPETITOR_GAP_PCT", "0.20")),
    max_step_pct=float(os.getenv("MAX_STEP_PCT", "0.15")),
)
pricing_policy = RuleBasedRLPolicy()
demand_sandbox = DemandSandbox(DB_PATH, MODELS_PATH)

class PriceRequest(BaseModel):
    product_id: str
    inventory_level: int


class PriceRLRequest(BaseModel):
    product_id: str
    inventory_level: int
    competitor_price: float | None = None
    season: int | None = None
    unit_cost: float | None = None

@tool("pobierz_polityke_rabatowa")
def pobierz_polityke_rabatowa() -> str:
    """Pobiera aktualną politykę rabatową i minimalne marże."""
    return "Maksymalny rabat to 15%. Minimalna marża: 20%."

@tool("kalkulator_marzy_brutto")
def kalkulator_marzy_brutto(koszt: float, cena_proponowana: float) -> str:
    """Kalkulator obliczający marżę brutto."""
    marza = (cena_proponowana - koszt) / cena_proponowana
    return f"Marża przy koszcie {koszt} i cenie {cena_proponowana} wynosi: {marza * 100:.2f}%"

strateg_cenowy = Agent(
    role="Strateg Cenowy",
    goal="Dynamiczna optymalizacja marży w oparciu o politykę firmy.",
    backstory="Zarządzasz wyceną produktów. Wykorzystujesz mechanizm autokrytyki (self-reflection). Unikaj historycznych, nieudanych strategii rabatowych.",
    verbose=True,
    allow_delegation=False,
    tools=[pobierz_polityke_rabatowa, kalkulator_marzy_brutto]
)

producer_conf = {'bootstrap.servers': KAFKA_BROKER}
producer = Producer(producer_conf)

consumer_conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'pricing_agent_group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumer_conf)
consumer.subscribe(['inventory_updates', 'demand_predictions'])

def delivery_report(err, msg):
    if err is not None:
        pass
    else:
        print(f"Opublikowano do {msg.topic()} [{msg.partition()}]")

def log_to_neo4j(product_id, reasoning_trace, new_price):
    """Log reasoning trace to Neo4j"""
    try:
        with neo4j_driver.session() as session:
            session.run("""
                MERGE (p:Product {id: $product_id})
                CREATE (pr:PriceDecision {
                    price: $price,
                    timestamp: datetime(),
                    reasoning: $reasoning
                })
                CREATE (p)-[:HAS_PRICE]->(pr)
            """, product_id=product_id, price=new_price, reasoning=str(reasoning_trace))
    except Exception as e:
        print(f"Neo4j logging error: {e}")

def write_price_to_db(product_id, new_price):
    """Write the new price to SQLite database"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute("""
            UPDATE inventory
            SET price = ?
            WHERE product_id = ?
        """, (round(new_price, 2), product_id))

        conn.commit()
        conn.close()

        print(f"[Pricing Agent] Updated price for {product_id} to {new_price:.2f} in database")
        return True
    except Exception as e:
        print(f"[Pricing Agent] Error writing price to database: {e}")
        return False

def get_current_inventory(product_id):
    """Read current inventory from database"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute("SELECT current_stock FROM inventory WHERE product_id = ?", (product_id,))
        row = cursor.fetchone()
        conn.close()

        return int(row[0]) if row else 100
    except Exception as e:
        print(f"[Pricing Agent] Error reading inventory: {e}")
        return 100


def get_product_snapshot(product_id):
    """Read stock, price and optional unit_cost from database."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute("SELECT current_stock, price FROM inventory WHERE product_id = ?", (product_id,))
        row = cursor.fetchone()

        if row:
            current_stock = int(row[0])
            current_price = float(row[1]) if row[1] is not None else 99.99
        else:
            current_stock = 100
            current_price = 99.99

        unit_cost = current_price * 0.60
        try:
            cursor.execute("SELECT cogs FROM inventory WHERE product_id = ?", (product_id,))
            cogs_row = cursor.fetchone()
            if cogs_row and cogs_row[0] is not None:
                unit_cost = float(cogs_row[0])
        except Exception:
            pass

        conn.close()
        return {
            "current_stock": current_stock,
            "current_price": current_price,
            "unit_cost": unit_cost,
        }
    except Exception as e:
        print(f"[Pricing Agent] Error reading product snapshot: {e}")
        return {
            "current_stock": 100,
            "current_price": 99.99,
            "unit_cost": 59.99,
        }


def _infer_season():
    month = datetime.utcnow().month
    if month in [12, 1, 2]:
        return 1
    if month in [3, 4, 5]:
        return 2
    if month in [6, 7, 8]:
        return 3
    return 4


def calculate_dynamic_price_rl(product_id, inventory_level, competitor_price=None, season=None, unit_cost=None):
    """MVP RL-like pricing strategy with sandbox demand model and hard guardrails."""
    try:
        snapshot = get_product_snapshot(product_id)
        current_price = float(snapshot["current_price"])
        current_stock = int(snapshot["current_stock"])

        effective_inventory = int(inventory_level) if int(inventory_level) > 0 else current_stock
        effective_competitor = float(competitor_price) if competitor_price and competitor_price > 0 else current_price
        effective_season = int(season) if season in [1, 2, 3, 4] else _infer_season()
        effective_cost = float(unit_cost) if unit_cost is not None else float(snapshot["unit_cost"])

        state = PricingState(
            product_id=product_id,
            current_price=current_price,
            competitor_price=effective_competitor,
            inventory_level=effective_inventory,
            season=effective_season,
            unit_cost=effective_cost,
        )

        action, diagnostics = pricing_policy.choose_action(state, demand_sandbox)
        candidate_price = current_price * (1.0 + action)
        final_price, violations = pricing_constraints.apply(
            candidate_price=candidate_price,
            current_price=current_price,
            unit_cost=effective_cost,
            competitor_price=effective_competitor,
        )

        reason = "RL policy with sandbox demand"
        if violations:
            reason += f" + guardrails({','.join(violations)})"

        redis_client.setex(
            f"price:{product_id}",
            3600,
            json.dumps(
                {
                    "product_id": product_id,
                    "price": final_price,
                    "inventory_level": effective_inventory,
                    "competitor_price": round(effective_competitor, 2),
                    "season": effective_season,
                    "unit_cost": round(effective_cost, 2),
                    "reason": reason,
                    "policy_diagnostics": diagnostics,
                }
            ),
        )

        write_price_to_db(product_id, final_price)
        return {
            "product_id": product_id,
            "new_price": final_price,
            "reason": reason,
            "inventory_level": effective_inventory,
            "competitor_price": round(effective_competitor, 2),
            "season": effective_season,
            "unit_cost": round(effective_cost, 2),
            "violations": violations,
            "policy_diagnostics": diagnostics,
        }
    except Exception as e:
        return {"error": str(e)}

def calculate_dynamic_price(product_id, inventory_level):
    """Calculate dynamic price based on inventory and predicted demand"""
    try:
        current_inventory = get_current_inventory(product_id)
        effective_level = inventory_level if inventory_level > 0 else current_inventory

        base_price = 99.99

        if effective_level > 1000:
            new_price = base_price * 0.85
            reason = "High inventory level - applying 15% discount"
        elif effective_level < 100:
            new_price = base_price * 1.15
            reason = "Low inventory level - applying 15% premium"
        elif effective_level < 300:
            new_price = base_price * 1.05
            reason = "Medium-low inventory - applying 5% increase"
        else:
            new_price = base_price
            reason = "Normal inventory level"

        min_price = base_price * 0.7 
        max_price = base_price * 1.3 
        new_price = max(min_price, min(max_price, new_price))

        redis_client.setex(f"price:{product_id}", 3600, json.dumps({
            "product_id": product_id,
            "price": round(new_price, 2),
            "inventory_level": effective_level,
            "reason": reason
        }))

        write_price_to_db(product_id, new_price)

        return {
            "product_id": product_id,
            "new_price": round(new_price, 2),
            "reason": reason,
            "inventory_level": effective_level
        }
    except Exception as e:
        return {"error": str(e)}

@app.post("/price")
def price_endpoint(request: PriceRequest):
    """HTTP endpoint for dynamic pricing"""
    if PRICING_MODE == 'rl':
        result = calculate_dynamic_price_rl(request.product_id, request.inventory_level)
    else:
        result = calculate_dynamic_price(request.product_id, request.inventory_level)
    if "error" in result:
        raise HTTPException(status_code=500, detail=result["error"])
    return result


@app.post("/price/rl")
def price_rl_endpoint(request: PriceRLRequest):
    """HTTP endpoint for RL-based dynamic pricing."""
    result = calculate_dynamic_price_rl(
        product_id=request.product_id,
        inventory_level=request.inventory_level,
        competitor_price=request.competitor_price,
        season=request.season,
        unit_cost=request.unit_cost,
    )
    if "error" in result:
        raise HTTPException(status_code=500, detail=result["error"])
    return result

@app.get("/health")
def health():
    return {"status": "healthy", "agent": "pricing"}

def kafka_consumer_loop():
    """Kafka consumer running in background thread"""
    print("Strateg Cenowy (CrewAI) uruchomiony (dynamicznie reaguje).")
    
    try:
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None: continue
            if msg.error(): continue

            topic = msg.topic()
            data = json.loads(msg.value().decode('utf-8'))
            
            if topic == 'inventory_updates':
                print(f"[Strateg Cenowy] Zmiana w zapasach, aktualizacja polityki cennika dla: {data}")
                product_id = data.get("product_id")
                
                wycena_task = Task(
                    description=f"Zaktualizowano rezerwy magazynowe w skutek prognozowanego popytu dla {product_id}. Sprawdź zgodność sugerowanej ceny z polityką firmy i oblicz optymalną cenę maksymalizującą zysk z zachowaniem minimalnej marży.",
                    expected_output="Proponowana, nowa cena wyliczona jako float (np. 99.99).",
                    agent=strateg_cenowy
                )

                crew = Crew(
                    agents=[strateg_cenowy],
                    tasks=[wycena_task],
                    process=Process.sequential
                )
                
                wynik = crew.kickoff()

                if PRICING_MODE == 'rl':
                    price_result = calculate_dynamic_price_rl(
                        product_id,
                        data.get('quantity', 100),
                    )
                else:
                    price_result = calculate_dynamic_price(product_id, data.get('quantity', 100))
                new_price = price_result.get('new_price', 99.99)
                
                pricing_update = {
                    "event_id": str(uuid4()),
                    "timestamp": datetime.utcnow().isoformat(),
                    "source_agent": "pricing_agent",
                    "product_id": product_id,
                    "new_price": new_price,
                    "reason": "Dynamic inventory trigger",
                    "workflow_trace": str(wynik)
                }
                
                # Log to Neo4j
                log_to_neo4j(product_id, wynik, new_price)
                
                producer.produce(
                    'price_updates', 
                    key=product_id.encode('utf-8'),
                    value=json.dumps(pricing_update).encode('utf-8'),
                    callback=delivery_report
                )
                producer.poll(0)
                
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"Blad: {e}")
    finally:
        consumer.close()
        producer.flush()

if __name__ == "__main__":
    kafka_thread = threading.Thread(target=kafka_consumer_loop, daemon=True)
    kafka_thread.start()
    
    uvicorn.run(app, host="0.0.0.0", port=8003)
