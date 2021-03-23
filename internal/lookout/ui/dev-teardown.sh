#!/bin/sh

docker stop postgres && docker rm postgres
docker stop redis && docker rm redis
docker stop nats && docker rm nats

kind delete cluster --name demo-a
