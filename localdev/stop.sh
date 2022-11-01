#!/bin/bash

# make the dir containing this file the CWD
cd "$(dirname "${0}")" || exit

# Stop services.
docker-compose down

# Get the configured cluster name.
read -ra cluster_name_line <<< "$(grep "^name:" ../e2e/setup/kind.yaml)"
cluster_name=${cluster_name_line[1]}

# stop the k8s cluster.
kind delete cluster -n "$cluster_name"
