FROM alpine:3.10

RUN addgroup -S -g 2000 armada && adduser -S -u 1000 armada -G armada

USER armada

COPY ./scheduleringester /app/

COPY ./config/ /app/config/scheduleringester

WORKDIR /app

ENTRYPOINT ["./scheduleringester"]
