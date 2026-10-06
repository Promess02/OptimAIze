#!/bin/bash
# Trening modeli popytu (batch) i zapis do cache Redis /models.

set -e
cd "$(dirname "$0")"

GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}OptimAIze — trening modeli${NC}"
echo -e "${BLUE}========================================${NC}\n"

echo "Uruchamianie training_service w trybie batch..."
docker-compose run --rm \
    -e TOP_PRODUCTS_LIMIT=10 \
    -e FORCE_RETRAIN=0 \
    -e DB_PATH=/data/ecommerce.db \
    training_service python /app/main.py --batch-train

echo -e "\n${GREEN}✓ Modele wytrenowane i zapisane w cache${NC}"
echo -e "${GREEN}Następny krok: ./run_tests.sh${NC}\n"
