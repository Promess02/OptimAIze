#!/bin/bash
# Quick-start script for Event-Driven Multi-Agent System

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Event-Driven Multi-Agent System${NC}"
echo -e "${BLUE}Quick Start Setup${NC}"
echo -e "${BLUE}========================================${NC}\n"

# Check Docker
echo -e "${YELLOW}[1/4]${NC} Checking Docker..."
if ! command -v docker-compose &> /dev/null; then
    echo -e "${RED}✗ Docker Compose not found${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Docker Compose available${NC}\n"

# Initialize database
echo -e "${YELLOW}[2/4]${NC} Initializing database..."
if python create_db.py; then
    echo -e "${GREEN}✓ Database initialized${NC}\n"
else
    echo -e "${RED}✗ Database initialization failed${NC}"
    exit 1
fi

# Start containers
echo -e "${YELLOW}[3/4]${NC} Starting Docker containers..."
echo -e "${BLUE}This may take 1-2 minutes on first run...${NC}\n"

if docker-compose up --build -d; then
    echo -e "${GREEN}✓ Containers started${NC}"
else
    echo -e "${RED}✗ Failed to start containers${NC}"
    exit 1
fi

# Wait for services
echo -e "\n${YELLOW}[4/4]${NC} Waiting for services to be ready..."
echo -e "${YELLOW}This may take 30-60 seconds...${NC}\n"

MAX_RETRIES=60
RETRY_COUNT=0

while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    if curl -s http://localhost:8001/health > /dev/null 2>&1 && \
       curl -s http://localhost:8002/health > /dev/null 2>&1 && \
       curl -s http://localhost:8003/health > /dev/null 2>&1 && \
       curl -s http://localhost:8004/health > /dev/null 2>&1; then
        echo -e "${GREEN}✓ All services ready${NC}\n"
        break
    fi

    RETRY_COUNT=$((RETRY_COUNT + 1))
    echo -n "."
    sleep 1
done

if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
    echo -e "\n${YELLOW}⚠ Services may still be starting. Check logs with:${NC}"
    echo "  docker-compose logs -f"
fi

# Run tests
echo -e "\n${BLUE}========================================${NC}"
echo -e "${BLUE}Running Tests${NC}"
echo -e "${BLUE}========================================${NC}\n"

python setup_and_test.py

# Print next steps
echo -e "\n${BLUE}========================================${NC}"
echo -e "${BLUE}Setup Complete!${NC}"
echo -e "${BLUE}========================================${NC}\n"

echo -e "${GREEN}✓ System is ready${NC}\n"

echo -e "${YELLOW}Next Steps:${NC}\n"

echo "1. ${BLUE}View container logs:${NC}"
echo "   docker-compose logs -f [service]"
echo "   (Try: demand_agent, inventory_agent, pricing_agent, procurement_agent)\n"

echo "2. ${BLUE}Run CDC Simulator (generates sales events):${NC}"
echo "   python enhanced_cdc_simulator.py\n"

echo "3. ${BLUE}Access Neo4j Browser (reasoning traces):${NC}"
echo "   http://localhost:7474 (user: neo4j, password: password)\n"

echo "4. ${BLUE}Query the database:${NC}"
echo "   sqlite3 ecommerce.db \"SELECT product_id, current_stock, price FROM inventory LIMIT 5;\"\n"

echo "5. ${BLUE}Check Kafka topics:${NC}"
echo "   docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092\n"

echo "6. ${BLUE}Monitor price updates in real-time:${NC}"
echo "   docker-compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic price_updates --from-beginning\n"

echo -e "${YELLOW}Documentation:${NC}"
echo "  Read: SETUP_GUIDE.md for detailed explanation\n"
