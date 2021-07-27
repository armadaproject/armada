#!/bin/sh

# Ensure armada, armada-executor, and armada-lookout are not running

kind delete cluster --name demo-a

docker stop redis
docker stop nats
docker stop postgres

docker rm redis
docker rm nats
docker rm postgres
