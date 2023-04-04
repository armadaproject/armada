ARG BASE_IMAGE=alpine:3.17.0
FROM ${BASE_IMAGE}
RUN addgroup -S -g 2000 armada && adduser -S -u 1000 armada -G armada
USER armada

# Lookout
COPY lookout /app/
COPY config/lookout/ /app/config/lookout

# Lookout ingester
COPY lookoutingester /app/
COPY config/lookoutingester/config.yaml /app/config/lookoutingester/config.yaml

# LookoutV2 ingester
COPY lookoutingesterv2 /app/
COPY config/lookoutingesterv2/config.yaml /app/config/lookoutingesterv2/config.yaml

# Lookoutv2
COPY lookoutv2 /app/
COPY config/lookoutv2/config.yaml /app/config/lookoutv2/config.yaml

WORKDIR /app
