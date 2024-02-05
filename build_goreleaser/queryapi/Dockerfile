ARG BASE_IMAGE=alpine:3.18.3
FROM ${BASE_IMAGE}
LABEL org.opencontainers.image.title=queryapi
LABEL org.opencontainers.image.description="queryapi"
LABEL org.opencontainers.image.url=https://hub.docker.com/r/gresearchdev/queryapi

RUN addgroup -S -g 2000 armada && adduser -S -u 1000 armada -G armada
USER armada

COPY queryapi /app/
COPY config/queriapi/config.yaml /app/config/queryapi/config.yaml

WORKDIR /app

ENTRYPOINT ["./queryapi"]
