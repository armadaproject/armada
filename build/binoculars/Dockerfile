FROM alpine:3.16

RUN addgroup -S -g 2000 armada && adduser -S -u 1000 armada -G armada

USER armada

COPY ./binoculars /app/

COPY /config/ /app/config/binoculars

WORKDIR /app

ENTRYPOINT ["./binoculars"]
