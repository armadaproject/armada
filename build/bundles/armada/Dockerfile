ARG BASE_IMAGE=alpine:3.21.0
FROM ${BASE_IMAGE}
LABEL org.opencontainers.image.title=armada
LABEL org.opencontainers.image.description="Armada Bundle"
LABEL org.opencontainers.image.url=https://hub.docker.com/r/gresearch/armada

RUN addgroup -S -g 2000 armada && adduser -S -u 1000 armada -G armada
USER armada

# Logging (share between all components)
COPY config/logging.yaml /app/config/logging.yaml

# Server
COPY config/server/config.yaml /app/config/server/config.yaml

# Executor
COPY config/executor/config.yaml /app/config/executor/config.yaml

# Binoculars
COPY config/binoculars/config.yaml /app/config/binoculars/config.yaml

# Scheduler
COPY config/scheduler/config.yaml /app/config/scheduler/config.yaml

# Scheduler Ingester
COPY config/scheduleringester/config.yaml /app/config/scheduleringester/config.yaml

# Event ingester
COPY config/eventingester/config.yaml /app/config/eventingester/config.yaml

COPY server executor binoculars eventingester scheduler scheduleringester /app/

WORKDIR /app
