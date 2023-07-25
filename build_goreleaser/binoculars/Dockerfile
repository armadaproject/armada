ARG BASE_IMAGE=alpine:3.18.2.0
FROM ${BASE_IMAGE}
LABEL org.opencontainers.image.title=binoculars
LABEL org.opencontainers.image.description="binoculars"
LABEL org.opencontainers.image.url=https://hub.docker.com/r/gresearchdev/binoculars

RUN addgroup -S -g 2000 armada && adduser -S -u 1000 armada -G armada
USER armada

COPY binoculars /app/
COPY config/binoculars/config.yaml /app/config/binoculars/config.yaml

WORKDIR /app

ENTRYPOINT ["./binoculars"]
