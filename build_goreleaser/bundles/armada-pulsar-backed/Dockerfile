ARG BASE_IMAGE=alpine:3.17.0
FROM ${BASE_IMAGE}
LABEL org.opencontainers.image.title=armada
LABEL org.opencontainers.image.description="Armada Bundle"
LABEL org.opencontainers.image.url=https://hub.docker.com/r/gresearchdev/armada

RUN addgroup -S -g 2000 armada && adduser -S -u 1000 armada -G armada
USER armada

# Server
COPY config/armada/config-pulsar-localdev.yaml /app/config/armada/config.yaml

# Executor
COPY config/executor/config-pulsar-localdev.yaml /app/config/executor/config.yaml

# Binoculars
COPY config/binoculars/config.yaml /app/config/binoculars/config.yaml

# Scheduler
COPY config/scheduler/config-pulsar-localdev.yaml /app/config/scheduler/config.yaml

# Scheduler Ingester
COPY config/scheduleringester/config-pulsar-localdev.yaml /app/config/scheduleringester/config.yaml

# Event ingester
COPY config/eventingester/config.yaml /app/config/eventingester/config.yaml

COPY server executor binoculars eventingester scheduler scheduleringester /app/

WORKDIR /app
