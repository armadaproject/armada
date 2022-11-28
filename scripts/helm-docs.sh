#!/bin/bash
# Generate documentation for each helm chart inside deployment/

docker run --rm --volume "$(pwd):/helm-docs" -u $(id -u) jnorwood/helm-docs:latest
