#!/bin/bash

# this will update kind kube config copied from host to work inside docker
if [ -f /.dockerenv ]; then
    DOCKER_HOST=$(ping -c 1 host.docker.internal | grep -m 1 -o -E '[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}')
    echo "$DOCKER_HOST kubernetes.default" >> /etc/hosts
    cp -R /tmp/.kube ~
    sed -i s/127.0.0.1/kubernetes.default/ ~/.kube/config
fi
ARMADA_APPLICATION_CLUSTERID=demo-a ARMADA_METRIC_PORT=9001 go run ./cmd/executor/main.go --config localdev/config/executor/config.yaml
