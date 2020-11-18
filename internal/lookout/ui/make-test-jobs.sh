#!/bin/sh

n_jobs=500

go run ./cmd/armadactl/main.go create-queue test --priorityFactor 1

i=1
while [ "$i" -ne "$n_jobs" ]; do
    go run ./cmd/armadactl/main.go submit ./example/jobs.yaml
    i="$((i+1))"
done
