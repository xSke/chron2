#!/bin/sh
docker run -d --rm -p 5432:5432 --name postgres -e POSTGRES_PASSWORD=password timescale/timescaledb:latest-pg15