FROM alpine:3.14

RUN addgroup -S -g 2000 armada && adduser -S -u 1000 armada -G armada

USER armada

COPY ./scheduler /app/

COPY ./config/ /app/config/scheduler

WORKDIR /app

ENTRYPOINT ["./scheduler"]
