ARG BASE_IMAGE=alpine:3.21.0

FROM ${BASE_IMAGE}
LABEL org.opencontainers.image.title=armada-lookout-bundle
LABEL org.opencontainers.image.description="Armada Lookout Bundle"
LABEL org.opencontainers.image.url=https://hub.docker.com/r/gresearch/armada-lookout-bundle

RUN addgroup -S -g 2000 armada && adduser -S -u 1000 armada -G armada
USER armada

# Logging (shared between both components)
COPY config/logging.yaml /app/config/logging.yaml

COPY config/lookoutingesterv2/config.yaml /app/config/lookoutingesterv2/config.yaml

COPY config/lookout/config.yaml /app/config/lookout/config.yaml

COPY lookoutingesterv2 lookout /app/

WORKDIR /app
