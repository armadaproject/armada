ARG BASE_IMAGE=alpine:3.18.2.0

FROM ${BASE_IMAGE}
LABEL org.opencontainers.image.title=lookoutingester
LABEL org.opencontainers.image.description="Lookout Ingester"
LABEL org.opencontainers.image.url=https://hub.docker.com/r/gresearchdev/lookoutingester

RUN addgroup -S -g 2000 armada && adduser -S -u 1000 armada -G armada
USER armada

COPY lookoutingester /app/
COPY config/lookoutingester/config.yaml /app/config/lookoutingester/config.yaml

WORKDIR /app

ENTRYPOINT ["./lookoutingester"]
