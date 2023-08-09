ARG BASE_IMAGE=alpine:3.18.3
FROM ${BASE_IMAGE}

LABEL org.opencontainers.image.title=armadactl
LABEL org.opencontainers.image.description="ArmadaCTL"
LABEL org.opencontainers.image.url=https://hub.docker.com/r/gresearchdev/armadactl

RUN addgroup -S -g 2000 armada && adduser -S -u 1000 armada -G armada
USER armada

COPY armadactl /app/

WORKDIR /app

ENTRYPOINT ["./armadactl"]
