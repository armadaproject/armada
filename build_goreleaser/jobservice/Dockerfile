FROM alpine:3.10

RUN addgroup -S -g 2000 armada && adduser -S -u 1000 armada -G armada

USER armada

COPY jobservice /app/

COPY config/jobservice/config.yaml /app/config/jobservice/config.yaml

WORKDIR /app

ENTRYPOINT ["./jobservice"]
