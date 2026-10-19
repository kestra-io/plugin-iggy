#!/usr/bin/env bash
set -e

docker compose -f docker-compose-ci.yml up -d --wait
