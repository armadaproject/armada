ARG BASE_IMAGE=alpine:3.17.0
FROM ${BASE_IMAGE}
RUN addgroup -S -g 2000 armada && adduser -S -u 1000 armada -G armada
USER armada

# Binaries
COPY server executor binoculars lookout lookoutingester lookoutingesterv2 eventingester lookout lookoutv2 jobservice /app/

# Config
COPY config/ app/config/

WORKDIR /app
