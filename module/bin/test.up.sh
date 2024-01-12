#!/usr/bin/env bash

docker-compose -f docker-compose.yml up --build --force-recreate -d test_env
docker compose -f docker-compose.yml exec test_env bash