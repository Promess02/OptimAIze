#!/bin/bash
set -e

echo "Cleaning up existing containers..."
docker rm -f zookeeper kafka redis_training redis_demand redis_inventory redis_pricing redis_procurement neo4j training_service orchestrator demand_agent inventory_agent pricing_agent procurement_agent || true

echo "Starting deployment..."

# Create network
docker network create agent_network || true

# Start Zookeeper
echo "Starting Zookeeper..."
docker run -d --name zookeeper \
  --network agent_network \
  -e ZOOKEEPER_CLIENT_PORT=2181 \
  -e ZOOKEEPER_TICK_TIME=2000 \
  confluentinc/cp-zookeeper:7.4.0

# Start Kafka
echo "Starting Kafka..."
docker run -d --name kafka \
  --network agent_network \
  -p 9092:9092 \
  -e KAFKA_BROKER_ID=1 \
  -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092 \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT \
  -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  confluentinc/cp-kafka:7.4.0

# Start Redis instances
echo "Starting Redis instances..."
docker run -d --name redis_training --network agent_network -p 16379:6379 redis:7-alpine
docker run -d --name redis_demand --network agent_network -p 16380:6379 redis:7-alpine
docker run -d --name redis_inventory --network agent_network -p 16381:6379 redis:7-alpine
docker run -d --name redis_pricing --network agent_network -p 16382:6379 redis:7-alpine
docker run -d --name redis_procurement --network agent_network -p 16383:6379 redis:7-alpine

# Start Neo4j
echo "Starting Neo4j..."
docker run -d --name neo4j \
  --network agent_network \
  -p 7474:7474 -p 7687:7687 \
  -e NEO4J_AUTH=neo4j/password \
  -e NEO4J_PLUGINS='["apoc"]' \
  -v neo4j_data:/data \
  neo4j:5.15

# Build and start services
echo "Building and starting Training Service..."
docker build -t training_service ./services/training
docker run -d --name training_service \
  --network agent_network \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 \
  -e REDIS_HOST=redis_training \
  -e REDIS_PORT=6379 \
  -e DB_PATH=/data/ecommerce.db \
  -e MODELS_PATH=/models \
  -v $(pwd):/data \
  -v $(pwd)/models:/models \
  training_service

echo "Building and starting Orchestrator..."
docker build -t orchestrator ./services/orchestrator
docker run -d --name orchestrator \
  --network agent_network \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 \
  orchestrator

echo "Building and starting Demand Agent..."
docker build -t demand_agent ./agents/demand
docker run -d --name demand_agent \
  --network agent_network \
  -p 8001:8001 \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 \
  -e REDIS_HOST=redis_demand \
  -e REDIS_PORT=6379 \
  -e NEO4J_URI=bolt://neo4j:7687 \
  -e NEO4J_PASSWORD=password \
  demand_agent

echo "Building and starting Inventory Agent..."
docker build -t inventory_agent ./agents/inventory
docker run -d --name inventory_agent \
  --network agent_network \
  -p 8002:8002 \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 \
  -e REDIS_HOST=redis_inventory \
  -e REDIS_PORT=6379 \
  -e NEO4J_URI=bolt://neo4j:7687 \
  -e NEO4J_PASSWORD=password \
  inventory_agent

echo "Building and starting Pricing Agent..."
docker build -t pricing_agent ./agents/pricing
docker run -d --name pricing_agent \
  --network agent_network \
  -p 8003:8003 \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 \
  -e REDIS_HOST=redis_pricing \
  -e REDIS_PORT=6379 \
  -e NEO4J_URI=bolt://neo4j:7687 \
  -e NEO4J_PASSWORD=password \
  pricing_agent

echo "Building and starting Procurement Agent..."
docker build -t procurement_agent ./agents/procurement
docker run -d --name procurement_agent \
  --network agent_network \
  -p 8004:8004 \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 \
  -e REDIS_HOST=redis_procurement \
  -e REDIS_PORT=6379 \
  -e DB_PATH=/data/ecommerce.db \
  -e MODELS_PATH=/models \
  -e SCHEDULE_MINUTES=30 \
  -v $(pwd):/data \
  -v $(pwd)/models:/models \
  procurement_agent

echo "All services started successfully!"
docker ps
