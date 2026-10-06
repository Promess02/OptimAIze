# Event-Driven Multi-Agent ML System

This system implements an event-driven architecture with multiple AI agents for demand forecasting, inventory management, and dynamic pricing.

## Architecture Overview

The system consists of:

### Core Services
1. **Training Service** - Asynchronous XGBoost model training for all products
2. **Orchestrator** - Coordinates agent workflows and CDC event distribution
3. **Demand Agent** - Forecasts product demand using trained ML models
4. **Inventory Agent** - Optimizes stock levels based on demand predictions
5. **Pricing Agent** - Dynamically adjusts prices based on inventory levels

### Infrastructure
- **Kafka** - Message broker for event streaming
- **Redis** (4 instances) - Separate caches for each service/agent
- **Neo4j** - Graph database for reasoning traces and data provenance
- **SQLite** - Main transactional database

## Getting Started

### Prerequisites
- Docker & Docker Compose
- Python 3.11+

### Repository layout

```text
OptimAIze/
├── setup.sh / train_models.sh / run_tests.sh   # entrypoints
├── docker-compose.yml, README.md, requirements.txt
├── data/          # ecommerce.db and data artifacts
├── scripts/       # create_db, CDC simulators, migrations, demos
│   └── legacy/    # older standalone prototypes
├── test/          # unit + integration tests
├── notebooks/     # exploratory Jupyter notebooks
├── docs/          # project docs + mermaid/ diagrams
├── agents/        # demand, inventory, pricing, procurement (+ domain notebooks)
├── services/      # training, orchestrator
├── shared/        # shared.paths + CDC helpers
├── playground/    # demo UI
└── models/        # trained model cache
```

Paths resolve via `shared/paths.py` (override with `DB_PATH`, `DATA_DIR`, `REPO_ROOT`).

### Setup

Trzy skrypty uruchomieniowe:

```bash
./setup.sh          # baza (data/ecommerce.db) + kontenery
./train_models.sh   # trening modeli popytu
./run_tests.sh      # testy jednostkowe + integracyjne
```

`setup.sh` uruchamia m.in.:
- Zookeeper (port 2181)
- Kafka (port 9092)
- Neo4j (ports 7474, 7687)
- Redis instances (ports 6379-6382)
- Training Service
- Orchestrator
- Demand Agent (HTTP: 8001)
- Inventory Agent (HTTP: 8002)
- Pricing Agent (HTTP: 8003)
- Procurement Agent (HTTP: 8004)

Playground UI (opcjonalnie):
```bash
cd playground && pip install -r requirements.txt
uvicorn server:app --host 0.0.0.0 --port 8090
```
W UI (http://localhost:8090): zakładka **Operacje** — trening modeli i testy z postępem; zakładka **Zamówienia** — propozycje zakupu.

### Usage

#### HTTP Endpoints

**Demand Prediction**:
```bash
curl -X POST http://localhost:8001/predict \
  -H "Content-Type: application/json" \
  -d '{"product_id": "PROD001", "horizon_days": 60}'
```

**Inventory Order**:
```bash
curl -X POST http://localhost:8002/order \
  -H "Content-Type: application/json" \
  -d '{"product_id": "PROD001", "predicted_demand": 500}'
```

**Dynamic Pricing**:
```bash
curl -X POST http://localhost:8003/price \
  -H "Content-Type: application/json" \
  -d '{"product_id": "PROD001", "inventory_level": 250}'
```

**Health Checks**:
```bash
curl http://localhost:8001/health
curl http://localhost:8002/health
curl http://localhost:8003/health
```

#### Simulating CDC Events

```bash
pip install confluent-kafka
# lightweight simulator
python shared/cdc_simulator.py
# or richer simulator with progress / more events
python scripts/enhanced_cdc_simulator.py
```

Database bootstrap (if needed without `./setup.sh`):
```bash
python scripts/create_db.py
```

## Event Flow

```
Database Change (Sale) 
  → CDC Event (Kafka: database_changes)
  → Training Service retrains models
  → Training Completed Event
  → Orchestrator triggers agent workflows
  → Demand Agent makes prediction
  → Inventory Agent calculates order
  → Pricing Agent adjusts price
  → All decisions logged to Neo4j
```

## Kafka Topics

- `database_changes` - CDC events from database
- `training_completed` - Model training completion notifications
- `demand_predictions` - Demand forecasts from Demand Agent
- `inventory_updates` - Inventory decisions from Inventory Agent
- `price_updates` - Price changes from Pricing Agent

## Neo4j Reasoning Traces

Access Neo4j Browser at http://localhost:7474 (user: neo4j, password: password)

Query reasoning traces:
```cypher
// View all predictions for a product
MATCH (p:Product {id: "PROD001"})-[:HAS_PREDICTION]->(d:DemandPrediction)
RETURN p, d ORDER BY d.timestamp DESC

// View complete decision chain
MATCH path = (p:Product)-[:HAS_PREDICTION]->()-[:HAS_ORDER]->()-[:HAS_PRICE]->()
RETURN path
```

## Redis Cache Structure

- `model:{product_id}` - Trained XGBoost models
- `prediction:{product_id}` - Cached demand predictions
- `order:{product_id}` - Cached order decisions
- `price:{product_id}` - Cached price decisions
- `inventory:{product_id}` - Current inventory levels

## Configuration

Environment variables (set in docker-compose.yml):

- `KAFKA_BOOTSTRAP_SERVERS` - Kafka broker address
- `REDIS_HOST` - Redis instance hostname
- `NEO4J_URI` - Neo4j connection URI
- `DB_PATH` - Path to SQLite database
- `MODELS_PATH` - Directory for saved models

## Monitoring

View logs for each service:
```bash
docker-compose logs -f training_service
docker-compose logs -f orchestrator
docker-compose logs -f demand_agent
docker-compose logs -f inventory_agent
docker-compose logs -f pricing_agent
```

## Development

### Adding New Agents

1. Create agent directory in `agents/`
2. Implement FastAPI HTTP endpoints
3. Add Kafka consumer for event-driven triggers
4. Log reasoning to Neo4j
5. Cache state in Redis
6. Add to docker-compose.yml

### Model Retraining

Models are retrained automatically on CDC events. To trigger manual retraining:

```python
from confluent_kafka import Producer
import json

producer = Producer({'bootstrap.servers': 'localhost:9092'})
event = {
    "event_type": "SalesCreated",
    "timestamp": "2024-01-01T00:00:00",
    "data": {"product_id": "PROD001", "sales": 100}
}
producer.produce('database_changes', value=json.dumps(event).encode('utf-8'))
producer.flush()
```

## Technology Stack

- **Framework**: FastAPI (REST) + Kafka (async messaging)
- **ML**: XGBoost (per-product demand forecasting)
- **Agents**: CrewAI (multi-agent orchestration)
- **Cache**: Redis (state management)
- **Graph DB**: Neo4j (reasoning traces)
- **Database**: SQLite (transactions)
- **Container**: Docker + docker-compose

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                    KAFKA MESSAGE BROKER                          │
├─────────────────────────────────────────────────────────────────┤
│  Topics:                                                         │
│  - database_changes (CDC events)                                │
│  - demand_predictions (from Demand Agent)                       │
│  - inventory_updates (from Inventory Agent)                     │
│  - price_updates (from Pricing Agent)                           │
│  - training_completed (from Training Service)                   │
└─────────────────────────────────────────────────────────────────┘
         ↑                    ↑                    ↑
         │                    │                    │
    ┌────┴────┐          ┌────┴────┐          ┌────┴────┐
    │  Demand │          │Inventory│          │ Pricing │
    │  Agent  │          │  Agent  │          │  Agent  │
    │(FastAPI)│          │(FastAPI)│          │(FastAPI)│
    │  :8001  │          │  :8002  │          │  :8003  │
    └────┬────┘          └────┬────┘          └────┬────┘
         │                    │                    │
    ┌────┴────────────────────┴────────────────────┴────┐
    │              REDIS CACHE (4 instances)           │
    │  - Cached predictions                            │
    │  - Agent states                                  │
    │  - Model metadata                                │
    └────────────────────────────────────────────────────┘
         │
    ┌────┴──────────────────────────────────────────────┐
    │        TRAINING SERVICE (Async Workers)           │
    │  - Retrains XGBoost models on CDC events          │
    │  - Stores models in Redis + /models               │
    │  - Publishes training_completed events            │
    └────────────────────────────────────────────────────┘
         │
    ┌────┴──────────────────────────────────────────────┐
    │              NEO4J CONTEXT GRAPHS                 │
    │  - Agent reasoning traces                         │
    │  - Decision trails (Data Provenance)              │
    │  - Product relationships                          │
    └────────────────────────────────────────────────────┘
         │
    ┌────┴──────────────────────────────────────────────┐
    │            SQLITE3 MAIN DATABASE                  │
    │  - sales_aggregated (historical data)             │
    │  - inventory (product stock levels)               │
    └────────────────────────────────────────────────────┘
```

## License

MIT
