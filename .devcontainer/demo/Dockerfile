# Commands to run
# docker build -t armada-demo -f .devcontainer/demo/Dockerfile .
# docker push user/armada-demo:latest

ARG BASE_IMAGE=golang:1.18-buster
FROM ${BASE_IMAGE}

RUN addgroup -S -g 2000 armada && adduser -S -u 1000 armada -G armada
USER armada

# Server
COPY bin/server /app/
COPY localdev/config/armada/config.yaml /app/config/armada/config.yaml

# Executor
COPY bin/executor /app/
COPY localdev/config/executor/config.yaml /app/config/executor/config.yaml

# Binoculars
COPY bin/binoculars /app/
COPY localdev/config/binoculars/config.yaml /app/config/binoculars/config.yaml

# Lookout ingester
COPY bin/lookoutingester /app/
COPY localdev/config/lookoutingester/config.yaml /app/config/lookoutingester/config.yaml

# Event ingester
COPY bin/eventingester /app/
COPY localdev/config/eventingester/config.yaml /app/config/eventingester/config.yaml

# Lookout
COPY bin/lookout /app/
COPY localdev/config/lookout/config.yaml /app/config/lookout/config.yaml

# Lookout V2
COPY bin/lookoutv2 /app/
COPY localdev/config/lookoutv2/config.yaml /app/config/lookoutv2/config.yaml

# Jobservice
COPY bin/jobservice /app/
COPY localdev/config/jobservice/config.yaml /app/config/jobservice/config.yaml

# Lookout V2 Ingester
COPY bin/lookoutingesterv2 /app/
COPY localdev/config/lookoutingesterv2/config.yaml /app/config/lookoutingesterv2/config.yaml

# Lookout UI
COPY internal/lookout/ui/build /app/internal/lookout/ui/build

WORKDIR /app
