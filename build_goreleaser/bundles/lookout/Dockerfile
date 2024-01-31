ARG BASE_IMAGE=alpine:3.18.3

FROM ${BASE_IMAGE}
LABEL org.opencontainers.image.title=armada-lookout-bundle
LABEL org.opencontainers.image.description="Armada Lookout Bundle"
LABEL org.opencontainers.image.url=https://hub.docker.com/r/gresearchdev/armada-lookout-bundle

RUN addgroup -S -g 2000 armada && adduser -S -u 1000 armada -G armada
USER armada

COPY config/lookoutingesterv2/config.yaml /app/config/lookoutingesterv2/config.yaml

COPY config/lookoutv2/config.yaml /app/config/lookoutv2/config.yaml

COPY lookoutingesterv2 lookoutv2 /app/

WORKDIR /app
