FROM alpine:3.14

RUN addgroup -S -g 2000 armada && adduser -S -u 1000 armada -G armada

USER armada

COPY ./bin/linux/lookout /app/
COPY ./internal/lookout/ui/build/ /app/internal/lookout/ui/build
COPY ./config/lookout/ /app/config/lookout

WORKDIR /app

ENTRYPOINT ["./lookout"]
