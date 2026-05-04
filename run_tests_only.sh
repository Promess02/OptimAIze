#!/bin/bash
# Script to run all tests without starting containers or running setup

set -e

echo "========================================"
echo "         Running All Unit Tests         "
echo "========================================"
python test_demand_inventory_scenarios.py
python test_pricing_rl_scenarios.py

echo ""
echo "========================================"
echo "      Running Integration Tests         "
echo " (Assumes containers are already running)"
echo "========================================"
python test_system.py
python test_procurement_agent.py
python test_kafka_procurement.py

echo ""
echo "========================================"
echo "           All Tests Passed!            "
echo "========================================"