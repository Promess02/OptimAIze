import os
import json
import pickle
import threading
from datetime import datetime, timedelta
from confluent_kafka import Consumer, Producer
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn
import redis
import pandas as pd
import numpy as np
from neo4j import GraphDatabase

try:
    from crewai import Agent, Task, Crew, Process
except Exception:
    class Agent:
        def __init__(self, *args, **kwargs): pass
    class Task:
        def __init__(self, *args, **kwargs):
            self.description = kwargs.get('description', '')
            self.expected_output = kwargs.get('expected_output', '')
            self.agent = kwargs.get('agent')
    class Crew:
        def __init__(self, *args, **kwargs):
            self.tasks = kwargs.get('tasks', [])
        def kickoff(self):
            return f"Fallback demand analysis: {self.tasks[0].description if self.tasks else ''}"
    class Process:
        sequential = "sequential"

app = FastAPI(title="Demand Agent API")

KAFKA_BROKER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
REDIS_HOST = os.getenv('REDIS_HOST', 'redis_demand')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
NEO4J_URI = os.getenv('NEO4J_URI', 'bolt://neo4j:7687')

redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=False)
redis_client_decoded = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=("neo4j", os.getenv("NEO4J_PASSWORD", "password")))

class PredictRequest(BaseModel):
    product_id: str
    horizon_days: int = 60
    months: int | None = None

analityk_popytu = Agent(
    role="Analityk Popytu",
    goal="Dokładne prognozowanie przyszłego wolumenu sprzedaży.",
    backstory="Analizujesz dane historyczne i wyniki modeli uczenia maszynowego, aby dostarczyć wiarygodne probabilistyczne prognozy popytu.",
    verbose=True,
    allow_delegation=False
)

producer = Producer({'bootstrap.servers': KAFKA_BROKER})
consumer = Consumer({
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'demand_agent_group',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe(['training_completed'])

def extract_time_features(df, min_date):
    df = df.copy()
    df['dayofweek'] = df['date'].dt.dayofweek
    df['month'] = df['date'].dt.month
    df['day'] = df['date'].dt.day
    df['year'] = df['date'].dt.year
    return df

def predict_demand(product_id: str, horizon_days: int = 60):
    model_bytes = redis_client.get(f"model:{product_id}")
    if not model_bytes:
        return {"error": f"Model not found for {product_id}"}
    
    try:
        model_data = pickle.loads(model_bytes)
    except Exception as e:
        return {"error": f"Failed to load model: {e}"}

    model = model_data.get('model')
    min_date = model_data.get('min_date', pd.Timestamp(datetime.utcnow().date()))
    if isinstance(min_date, str):
        min_date = pd.to_datetime(min_date)
        
    historical_mean = float(model_data.get('historical_sales_mean', 10.0))
    historical_std = float(model_data.get('historical_sales_std', 0.0))
    historical_price = float(model_data.get('historical_price', 99.99))
    strategy = model_data.get('selected_strategy', 'xgb')
    
    predicted_demand = 0.0
    
    if strategy == 'xgb' and model is not None:
        start_date = datetime.utcnow().date() + timedelta(days=1)
        future_dates = [start_date + timedelta(days=i) for i in range(horizon_days)]
        df_future = pd.DataFrame({'date': pd.to_datetime(future_dates)})
        df_future = extract_time_features(df_future, min_date)
        
        for lag in [1, 7, 14, 30]:
            df_future[f'sales_lag_{lag}'] = historical_mean
            df_future[f'revenue_lag_{lag}'] = historical_mean * historical_price
        for window in [7, 14]:
            df_future[f'sales_rolling_mean_{window}'] = historical_mean
            df_future[f'sales_rolling_std_{window}'] = historical_std
            
        df_future['is_out_of_stock'] = 0

        feature_cols = model_data.get('feature_cols', [])
        for col in feature_cols:
            if col not in df_future.columns:
                df_future[col] = 0
        
        X_future = df_future[feature_cols]
        preds = model.predict(X_future)
        preds = np.clip(preds, 0, None)
        predicted_demand = float(np.sum(preds))
    else:
        fallback_config = model_data.get('fallback_config', {})
        fallback_type = fallback_config.get('type', 'mean')
        if fallback_type in ['lag1', 'lag1_or_mean'] and 'last_sales' in fallback_config:
            daily_rate = float(fallback_config['last_sales'])
        else:
            daily_rate = float(fallback_config.get('mean_sales', historical_mean))
        
        predicted_demand = float(daily_rate * horizon_days)

    predicted_demand = max(0, int(round(predicted_demand)))

    result = {
        "product_id": product_id,
        "predicted_demand": predicted_demand,
        "confidence_score": 0.85 if strategy == 'xgb' else 0.50,
        "horizon_days": horizon_days
    }
    
    redis_client_decoded.setex(f"prediction:{product_id}", 3600, json.dumps(result))
    return result

@app.post("/predict")
def predict_endpoint(request: PredictRequest):
    horizon = request.horizon_days
    if request.months is not None:
        horizon = int(request.months * 30.5)
        
    result = predict_demand(request.product_id, horizon)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    
    if request.months is not None:
        result["months"] = request.months
        
    return result

@app.get("/health")
def health():
    return {"status": "healthy", "agent": "demand"}

def log_to_neo4j(product_id, reasoning, result):
    try:
        with neo4j_driver.session() as session:
            session.run('''
                MERGE (p:Product {id: $product_id})
                CREATE (d:DemandPrediction {
                    predicted_demand: $demand,
                    horizon_days: $horizon,
                    confidence: $confidence,
                    timestamp: datetime(),
                    reasoning: $reasoning
                })
                CREATE (p)-[:HAS_PREDICTION]->(d)
            ''', product_id=product_id, demand=result['predicted_demand'], 
                 horizon=result['horizon_days'], confidence=result['confidence_score'],
                 reasoning=str(reasoning))
    except Exception as e:
        print(f"Neo4j logging error: {e}")

def kafka_consumer_loop():
    print("Agent Popytu (Demand Agent) uruchomiony...")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg.error(): continue
            
            topic = msg.topic()
            data = json.loads(msg.value().decode('utf-8'))
            
            if topic == 'training_completed':
                product_id = data.get('product_id', 'ALL')
                pred_task = Task(
                    description=f"Pobrano nowy model z Kafki. Opracuj predykcję popytu używając zaktualizowanych modeli dla {product_id}.",
                    expected_output="Zgłoszenie pomyślnej aktualizacji popytu i wysłanie na kolejkę.",
                    agent=analityk_popytu
                )
                crew = Crew(agents=[analityk_popytu], tasks=[pred_task], process=Process.sequential)
                reasoning = crew.kickoff()
                
                if product_id != 'ALL':
                    result = predict_demand(product_id, 60)
                    if "error" not in result:
                        log_to_neo4j(product_id, reasoning, result)
                        producer.produce(
                            'demand_predictions',
                            key=product_id.encode('utf-8'),
                            value=json.dumps(result).encode('utf-8')
                        )
                        producer.poll(0)
    except Exception as e:
        print(f"Błąd Agenta Popytu: {e}")
    finally:
        consumer.close()
        producer.flush()

if __name__ == "__main__":
    threading.Thread(target=kafka_consumer_loop, daemon=True).start()
    uvicorn.run(app, host="0.0.0.0", port=8001)