#!/usr/bin/env bash

docker compose -f docker-compose-ci.yml down -v --remove-orphans
