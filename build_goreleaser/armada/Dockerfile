ARG BASE_IMAGE=alpine:3.17.0
FROM ${BASE_IMAGE}
RUN addgroup -S -g 2000 armada && adduser -S -u 1000 armada -G armada
USER armada

# Server
COPY server /app/
COPY config/armada/config.yaml /app/config/armada/config.yaml

# Executor
COPY executor /app/
COPY config/executor/config.yaml /app/config/executor/config.yaml

# Binoculars
COPY binoculars /app/
COPY config/binoculars/config.yaml /app/config/binoculars/config.yaml

# Lookout
COPY lookout /app/
COPY config/lookout/ /app/config/lookout

# Lookout ingester
COPY lookoutingester /app/
COPY config/lookoutingester/config.yaml /app/config/lookoutingester/config.yaml

# Event ingester
COPY eventingester /app/
COPY config/eventingester/config.yaml /app/config/eventingester/config.yaml

# LookoutV2 ingester
COPY lookoutingesterv2 /app/
COPY config/lookoutingesterv2/config.yaml /app/config/lookoutingesterv2/config.yaml

# Lookoutv2
COPY lookoutv2 /app/
COPY config/lookoutv2/config.yaml /app/config/lookoutv2/config.yaml

# Lookout w/ UI
COPY lookout /app/
COPY config/lookout/config.yaml /app/config/lookout/config.yaml

# Jobservice
COPY jobservice /app/
COPY config/jobservice/config.yaml /app/config/jobservice/config.yaml

WORKDIR /app
