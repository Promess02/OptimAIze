#!/bin/bash
# Wrapper — uruchamia testy z katalogu test/.

set -e
cd "$(dirname "$0")"
exec ./test/run_tests.sh
