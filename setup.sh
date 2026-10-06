#!/bin/bash
# Uruchomienie całego systemu: baza + kontenery Docker + oczekiwanie na health-checki.

set -e
cd "$(dirname "$0")"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}OptimAIze — uruchomienie systemu${NC}"
echo -e "${BLUE}========================================${NC}\n"

echo -e "${YELLOW}[1/3]${NC} Sprawdzanie Docker Compose..."
if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
    echo -e "${RED}✗ Docker Compose nie znaleziony${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Docker Compose dostępny${NC}\n"

if [ -n "$CONDA_PREFIX" ] && [ -x "$CONDA_PREFIX/bin/python" ]; then
    PYTHON_BIN="$CONDA_PREFIX/bin/python"
else
    PYTHON_BIN="$(command -v python || command -v python3)"
fi

echo -e "${YELLOW}[2/3]${NC} Inicjalizacja bazy danych (data/ecommerce.db)..."
if "$PYTHON_BIN" scripts/create_db.py; then
    echo -e "${GREEN}✓ Baza gotowa${NC}\n"
else
    echo -e "${RED}✗ Inicjalizacja bazy nie powiodła się${NC}"
    exit 1
fi

echo -e "${YELLOW}[3/3]${NC} Uruchamianie kontenerów..."
echo "Usuwanie konfliktujących kontenerów (jeśli istnieją)..."
docker rm -f zookeeper kafka redis_training redis_demand redis_inventory redis_pricing redis_procurement neo4j training_service orchestrator demand_agent inventory_agent pricing_agent procurement_agent mongodb mongodb_init 2>/dev/null || true
docker-compose down -v --remove-orphans 2>/dev/null || true

if docker-compose up --build -d; then
    echo -e "${GREEN}✓ Kontenery uruchomione${NC}"
else
    echo -e "${RED}✗ Nie udało się uruchomić kontenerów${NC}"
    exit 1
fi

echo -e "\n${YELLOW}Oczekiwanie na gotowość usług (8001–8004)...${NC}"
MAX_RETRIES=60
RETRY_COUNT=0

while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    if curl -s http://localhost:8001/health > /dev/null 2>&1 && \
       curl -s http://localhost:8002/health > /dev/null 2>&1 && \
       curl -s http://localhost:8003/health > /dev/null 2>&1 && \
       curl -s http://localhost:8004/health > /dev/null 2>&1; then
        echo -e "\n${GREEN}✓ Wszystkie usługi gotowe${NC}\n"
        break
    fi
    RETRY_COUNT=$((RETRY_COUNT + 1))
    echo -n "."
    sleep 1
done

if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
    echo -e "\n${YELLOW}⚠ Usługi mogą się jeszcze uruchamiać. Logi: docker-compose logs -f${NC}"
else
    echo -e "${GREEN}Gotowe.${NC}"
fi

echo -e "\n${YELLOW}Następne kroki:${NC}"
echo "  ./train_models.sh     — trening modeli"
echo "  ./run_tests.sh        — testy"
echo "  cd playground && pip install -r requirements.txt && uvicorn server:app --host 0.0.0.0 --port 8090"
echo ""
