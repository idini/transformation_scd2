#!/usr/bin/env bash
# example ./bin/exec.sh test_env bash
docker compose -f docker-compose.yml exec $1 $2