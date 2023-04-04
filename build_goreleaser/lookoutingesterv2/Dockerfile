ARG BASE_IMAGE=alpine:3.17.0
LABEL org.opencontainers.image.title=lookoutingesterv2
LABEL org.opencontainers.image.description="Lookout Ingester V2"
LABEL org.opencontainers.image.url=https://hub.docker.com/r/gresearchdev/lookoutingesterv2

FROM ${BASE_IMAGE}

RUN addgroup -S -g 2000 armada && adduser -S -u 1000 armada -G armada
USER armada

COPY lookoutingesterv2 /app/
COPY config/lookoutingesterv2/config.yaml /app/config/lookoutingesterv2/config.yaml

WORKDIR /app

ENTRYPOINT ["./lookoutingesterv2"]