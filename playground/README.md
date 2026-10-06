# OptimAIze Playground

Interfejs do demonstracji i ręcznego sterowania systemem.

## Funkcje

1. **Operacje** — trening modeli oraz uruchamianie testów z paskiem postępu, listą kroków i logiem na żywo.
2. **Zamówienia** — lista produktów z raportu procurement i propozycja zamówień (30 / 60 dni) w popupie.
3. **Status agentów** — health-check demand / inventory / procurement.

## Wymagania

- Stack: `./setup.sh` (trening można odpalić z UI albo `./train_models.sh`)
- Docker dostępny na hoście (trening woła `docker-compose run training_service`)

## Uruchomienie

```bash
cd playground
pip install -r requirements.txt
uvicorn server:app --reload --host 0.0.0.0 --port 8090
```

Otwórz: **http://localhost:8090**

## API zadań

| Endpoint | Opis |
|----------|------|
| `POST /api/jobs/tests` | Start testów (async) |
| `POST /api/jobs/train` | Start treningu modeli (async) |
| `GET /api/jobs/{id}` | Status, postęp, kroki, log |

## Zmienne środowiskowe

| Zmienna | Domyślnie | Opis |
|---------|-----------|------|
| `PLAYGROUND_DEMAND_URL` | `http://localhost:8001` | Demand Agent |
| `PLAYGROUND_INVENTORY_URL` | `http://localhost:8002` | Inventory Agent |
| `PLAYGROUND_PROCUREMENT_URL` | `http://localhost:8004` | Procurement Agent |
| `PLAYGROUND_DB_PATH` | `data/ecommerce.db` (via `shared.paths`) | Fallback SQLite |
| `PLAYGROUND_TOP_PRODUCTS_LIMIT` | `10` | Limit produktów przy treningu |
| `PLAYGROUND_FORCE_RETRAIN` | `0` | Wymuś retrening |
