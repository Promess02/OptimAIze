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
from datetime import datetime
from neo4j import GraphDatabase
import numpy as np

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
    months: int | None = None

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
        model = model_data.get('model')
        min_date = model_data.get('min_date', datetime.now())
        feature_cols = model_data.get('feature_cols', ['dayofweek', 'month', 'year', 'day', 'days_since_start'])
        historical_sales_mean = float(model_data.get('historical_sales_mean', 0.0))
        historical_sales_std = float(model_data.get('historical_sales_std', 0.0))
        model_type = model_data.get('model_type', 'xgboost')
        recent_sales = [float(v) for v in model_data.get('recent_sales', [])]

        if isinstance(min_date, str):
            min_date = pd.to_datetime(min_date)

        horizon_days = int(max(1, horizon_days))

        # Generate future dates
        last_date = datetime.now()
        future_dates = [last_date + timedelta(days=i) for i in range(1, horizon_days + 1)]
        df_future = pd.DataFrame({'date': pd.to_datetime(future_dates)})

        def apply_temporal_features(frame: pd.DataFrame) -> pd.DataFrame:
            frame = frame.copy()
            frame['dayofweek'] = frame['date'].dt.dayofweek
            frame['month'] = frame['date'].dt.month
            frame['day'] = frame['date'].dt.day
            frame['quarter'] = frame['date'].dt.quarter
            frame['year'] = frame['date'].dt.year
            frame['day_of_year'] = frame['date'].dt.dayofyear
            frame['is_month_end'] = frame['date'].dt.is_month_end.astype(int)
            frame['is_quarter_end'] = frame['date'].dt.is_quarter_end.astype(int)
            frame['days_since_start'] = (frame['date'] - min_date).dt.days
            return frame

        df_future = apply_temporal_features(df_future)

        fallback_cfg = model_data.get('fallback_config', {})
        default_last = float(fallback_cfg.get('last_sales', recent_sales[-1] if recent_sales else historical_sales_mean))
        default_mean = float(fallback_cfg.get('mean_sales', historical_sales_mean))

        def infer_lag_value(history: list[float], lag: int) -> float:
            if len(history) >= lag:
                return float(history[-lag])
            if history:
                return float(history[-1])
            return default_mean

        simulated_history = list(recent_sales)
        if not simulated_history:
            simulated_history = [default_last]

        lag_cols = [1, 7, 14, 30]
        window_cols = [7, 14, 30]

        if model_type == 'naive_fallback' or model is None:
            preds = []
            strategy = model_data.get('selected_strategy', 'lag1_or_mean')
            for _ in range(horizon_days):
                if strategy == 'lag1_or_mean':
                    next_pred = float(simulated_history[-1]) if simulated_history else default_mean
                else:
                    next_pred = default_mean
                next_pred = max(0.0, next_pred)
                preds.append(next_pred)
                simulated_history.append(next_pred)
        else:
            preds = []
            rows = []
            for idx in range(horizon_days):
                row = dict(df_future.iloc[idx])
                for lag in lag_cols:
                    row[f'sales_lag_{lag}'] = infer_lag_value(simulated_history, lag)
                for window in window_cols:
                    values = simulated_history[-window:] if len(simulated_history) >= window else simulated_history
                    if values:
                        row[f'sales_rolling_mean_{window}'] = float(np.mean(values))
                        row[f'sales_rolling_std_{window}'] = float(np.std(values))
                    else:
                        row[f'sales_rolling_mean_{window}'] = default_mean
                        row[f'sales_rolling_std_{window}'] = historical_sales_std
                rows.append(row)

            df_future_full = pd.DataFrame(rows)
            for col in feature_cols:
                if col not in df_future_full.columns:
                    df_future_full[col] = 0

            for idx in range(horizon_days):
                current = df_future_full.iloc[[idx]].copy()
                for lag in lag_cols:
                    current[f'sales_lag_{lag}'] = infer_lag_value(simulated_history, lag)
                for window in window_cols:
                    values = simulated_history[-window:] if len(simulated_history) >= window else simulated_history
                    if values:
                        current[f'sales_rolling_mean_{window}'] = float(np.mean(values))
                        current[f'sales_rolling_std_{window}'] = float(np.std(values))
                    else:
                        current[f'sales_rolling_mean_{window}'] = default_mean
                        current[f'sales_rolling_std_{window}'] = historical_sales_std

                pred = float(model.predict(current[feature_cols])[0])
                pred = max(0.0, pred)
                preds.append(pred)
                simulated_history.append(pred)

        preds = np.maximum(0, np.asarray(preds, dtype=float))
        df_future['pred'] = preds
        df_future['month_bucket'] = df_future['date'].dt.strftime('%Y-%m')
        monthly = (
            df_future.groupby('month_bucket', as_index=False)['pred']
            .sum()
            .rename(columns={'pred': 'forecast_demand'})
        )
        monthly['forecast_demand'] = monthly['forecast_demand'].round().astype(int)
        
        predicted_demand = int(np.sum(preds))
        
        # Cache result
        cache_key = f"prediction:{product_id}"
        redis_client.setex(cache_key, 3600, json.dumps({
            "product_id": product_id,
            "predicted_demand": predicted_demand,
            "horizon_days": horizon_days,
            "monthly_forecast": monthly.to_dict(orient='records'),
        }))
        
        return {
            "product_id": product_id,
            "predicted_demand": predicted_demand,
            "confidence_score": 0.85,
            "horizon_days": horizon_days,
            "monthly_forecast": monthly.to_dict(orient='records'),
            "strategy": model_data.get('selected_strategy', 'xgboost_default'),
        }
    except Exception as e:
        return {"error": str(e), "product_id": product_id}

@app.post("/predict")
def predict_endpoint(request: PredictRequest):
    """HTTP endpoint for demand prediction"""
    if request.months is not None:
        if request.months not in (1, 2, 3):
            raise HTTPException(status_code=422, detail="months must be one of: 1, 2, 3")
        resolved_horizon_days = int(round(request.months * 30.5))
    else:
        resolved_horizon_days = request.horizon_days

    result = predict_demand(request.product_id, resolved_horizon_days)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    if request.months is not None:
        result["months"] = request.months
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