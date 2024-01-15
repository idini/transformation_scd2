#!/usr/bin/env bash
clear

echo Run Unit Tests
python -m pytest -s -W ignore::DeprecationWarning -v tests/unit/

echo Run Integration Tests
python -m pytest -s -W ignore::DeprecationWarning -v tests/integration/