#!/usr/bin/env bash

docker-compose -f docker-compose.yml up --build --force-recreate -d test_transformation_scd2
docker compose -f docker-compose.yml exec test_transformation_scd2 bash