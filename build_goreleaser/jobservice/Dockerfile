ARG BASE_IMAGE=alpine:3.0

FROM ${BASE_IMAGE}
LABEL org.opencontainers.image.title=jobservice
LABEL org.opencontainers.image.description="Job Service"
LABEL org.opencontainers.image.url=https://hub.docker.com/r/gresearchdev/jobservice

RUN addgroup -S -g 2000 armada && adduser -S -u 1000 armada -G armada
USER armada

COPY jobservice /app/
COPY config/jobservice/config.yaml /app/config/jobservice/config.yaml

WORKDIR /app

ENTRYPOINT ["./jobservice", "run"]
