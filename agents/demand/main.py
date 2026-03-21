import os
import json
import pickle
import threading
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
                return f"Fallback analysis: {self.tasks[0].description}"
            return "Fallback analysis"

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
import pandas as pd
from datetime import timedelta
from neo4j import GraphDatabase

app = FastAPI(title="Demand Agent API")

KAFKA_BROKER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
NEO4J_URI = os.getenv('NEO4J_URI', 'bolt://neo4j:7687')
REDIS_HOST = os.getenv('REDIS_HOST', 'redis_demand')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))

redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=False)
neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=("neo4j", os.getenv("NEO4J_PASSWORD", "password")))

class PredictRequest(BaseModel):
    product_id: str
    horizon_days: int = 60

@tool("pobierz_ceny_konkurencji")
def pobierz_ceny_konkurencji(product_id: str, competitor_list: str) -> str:
    """Mock narzędzia: Pobiera ceny konkurencji dla danego produktu."""
    return f"Cena dla {product_id} wynosi 100 PLN."

@tool("analiza_sentymentu_rynkowego")
def analiza_sentymentu_rynkowego(query: str) -> str:
    """Mock narzędzia: Sprawdza sentyment rynkowy na podstawie trendów."""
    return "Sentyment jest wysoce pozytywny, spodziewany duży popyt."

analityk_rynkowy = Agent(
    role="Analityk Rynkowy",
    goal="Monitorowanie trendów i korelacji popytowych. Przewidywanie popytu na bazie danych wejściowych.",
    backstory="Jesteś zaawansowanym asystentem AI. Twoim zadaniem jest przewidzenie trendu sprzedaży i zasugerowanie skali popytu w oparciu o informacje sprzedażowe i sentyment.",
    verbose=True,
    allow_delegation=False,
    tools=[pobierz_ceny_konkurencji, analiza_sentymentu_rynkowego]
)

producer_conf = {'bootstrap.servers': KAFKA_BROKER}
producer = Producer(producer_conf)

consumer_conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'demand_agent_group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumer_conf)
consumer.subscribe(['database_cdc.sales'])

def delivery_report(err, msg):
    if err is not None:
        print(f"Błąd wysyłania wiadomości: {err}")
    else:
        print(f"Opublikowano do {msg.topic()} [{msg.partition()}]")

def log_to_neo4j(product_id, reasoning_trace, prediction):
    """Log reasoning trace to Neo4j"""
    try:
        with neo4j_driver.session() as session:
            session.run("""
                MERGE (p:Product {id: $product_id})
                CREATE (d:DemandPrediction {
                    prediction: $prediction,
                    timestamp: datetime(),
                    reasoning: $reasoning
                })
                CREATE (p)-[:HAS_PREDICTION]->(d)
            """, product_id=product_id, prediction=prediction, reasoning=str(reasoning_trace))
    except Exception as e:
        print(f"Neo4j logging error: {e}")

def predict_demand(product_id, horizon_days=60):
    """Make demand prediction using trained model from Redis"""
    try:
        model_bytes = redis_client.get(f"model:{product_id}")
        if not model_bytes:
            return {"error": "Model not found", "product_id": product_id}
        
        model_data = pickle.loads(model_bytes)
        model = model_data['model']
        min_date = model_data['min_date']
        feature_cols = model_data.get('feature_cols', ['dayofweek', 'month', 'year', 'day', 'days_since_start'])
        historical_sales_mean = float(model_data.get('historical_sales_mean', 0.0))
        historical_sales_std = float(model_data.get('historical_sales_std', 0.0))
        
        # Generate future dates
        from datetime import datetime
        last_date = datetime.now()
        future_dates = [last_date + timedelta(days=i) for i in range(1, horizon_days + 1)]
        df_future = pd.DataFrame({'date': pd.to_datetime(future_dates)})
        
        # Extract notebook-aligned features
        df_future['dayofweek'] = df_future['date'].dt.dayofweek
        df_future['month'] = df_future['date'].dt.month
        df_future['day'] = df_future['date'].dt.day
        df_future['quarter'] = df_future['date'].dt.quarter
        df_future['year'] = df_future['date'].dt.year
        df_future['day_of_year'] = df_future['date'].dt.dayofyear
        df_future['is_month_end'] = df_future['date'].dt.is_month_end.astype(int)
        df_future['is_quarter_end'] = df_future['date'].dt.is_quarter_end.astype(int)
        df_future['days_since_start'] = (df_future['date'] - min_date).dt.days

        for lag in [1, 7, 14, 30]:
            df_future[f'sales_lag_{lag}'] = historical_sales_mean

        for window in [7, 14, 30]:
            df_future[f'sales_rolling_mean_{window}'] = historical_sales_mean
            df_future[f'sales_rolling_std_{window}'] = historical_sales_std

        for col in feature_cols:
            if col not in df_future.columns:
                df_future[col] = 0
        
        X_future = df_future[feature_cols]
        
        import numpy as np
        preds = model.predict(X_future)
        preds = np.maximum(0, preds)
        
        predicted_demand = int(np.sum(preds))
        
        # Cache result
        cache_key = f"prediction:{product_id}"
        redis_client.setex(cache_key, 3600, json.dumps({
            "product_id": product_id,
            "predicted_demand": predicted_demand,
            "horizon_days": horizon_days
        }))
        
        return {
            "product_id": product_id,
            "predicted_demand": predicted_demand,
            "confidence_score": 0.85
        }
    except Exception as e:
        return {"error": str(e), "product_id": product_id}

@app.post("/predict")
def predict_endpoint(request: PredictRequest):
    """HTTP endpoint for demand prediction"""
    result = predict_demand(request.product_id, request.horizon_days)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result

@app.get("/health")
def health():
    return {"status": "healthy", "agent": "demand"}

def kafka_consumer_loop():
    """Kafka consumer running in background thread"""
    print("Agent Popytu (Analityk Rynkowy - CrewAI) uruchomiony i oczekuje na zdarzenia sprzedaży...")
    
    try:
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
            if msg.error():
                print(f"Błąd Consumer: {msg.error()}")
                continue

            data = json.loads(msg.value().decode('utf-8'))
            print(f"[Analityk] Odebrano: {data}")
            product_id = data.get("product_id", "Unknown")


            analiza_task = Task(
                description=f"Przeanalizuj produkt o ID: {product_id}. Pobierz ceny konkurencji i zbadaj sentyment. Zwróć estymowaną wartość popytu jako liczbę całkowitą.",
                expected_output="Liczba całkowita reprezentująca prognozowany popyt (np. 150).",
                agent=analityk_rynkowy
            )

            crew = Crew(
                agents=[analityk_rynkowy],
                tasks=[analiza_task],
                process=Process.sequential
            )

            wynik = crew.kickoff()
            print(f"[Analityk] Wynik analizy: {wynik}")
            
            prediction_result = predict_demand(product_id)
            predicted_demand_value = prediction_result.get('predicted_demand', 150)
            
            predicted_demand = {
                "product_id": product_id,
                "predicted_demand": predicted_demand_value,
                "confidence_score": 0.85,
                "reasoning_trace": str(wynik)
            }
            
            log_to_neo4j(product_id, wynik, predicted_demand_value)

            producer.produce(
                'demand_predictions', 
                key=product_id.encode('utf-8'),
                value=json.dumps(predicted_demand).encode('utf-8'),
                callback=delivery_report
            )
            producer.poll(0)

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
    
    uvicorn.run(app, host="0.0.0.0", port=8001)