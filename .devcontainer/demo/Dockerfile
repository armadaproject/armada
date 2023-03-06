# Commands to run
# docker build -t armada-demo -f .devcontainer/demo/Dockerfile .
# docker push armada-demo:latest

ARG BASE_IMAGE=alpine:3.17.0
FROM ${BASE_IMAGE}

RUN addgroup -S -g 2000 armada && adduser -S -u 1000 armada -G armada
USER armada

# Copy bin
COPY bin /app/bin

# Lookout UI
COPY internal/lookout/ui/build /app/internal/lookout/ui/build

# Copy configs
COPY config /app/config

# Copy localdev configs
COPY localdev/config /app/localdev/config

WORKDIR /app
