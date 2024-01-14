#!/usr/bin/env bash
clear

echo Run Unit Tests
python -m pytest -s -v tests/unit/

echo Run Integration Tests
python -m pytest -s -v tests/integration/