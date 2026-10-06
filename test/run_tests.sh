#!/bin/bash
# Uruchomienie testów jednostkowych i (jeśli usługi żyją) integracyjnych.

set -e
cd "$(dirname "$0")"

GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}OptimAIze — testy${NC}"
echo -e "${BLUE}========================================${NC}\n"

if [ -n "$CONDA_PREFIX" ] && [ -x "$CONDA_PREFIX/bin/python" ]; then
    PYTHON_BIN="$CONDA_PREFIX/bin/python"
else
    PYTHON_BIN="$(command -v python || command -v python3)"
fi

if "$PYTHON_BIN" run_all_tests.py; then
    echo -e "\n${GREEN}✓ Wszystkie uruchomione testy zakończone sukcesem${NC}\n"
else
    echo -e "\n\033[0;31m✗ Część testów nie przeszła — szczegóły powyżej.\033[0m"
    exit 1
fi
