FROM alpine:3.10

COPY ./armada /app/
COPY /config/armada/ /app/config/armada

WORKDIR /app

ENTRYPOINT ["./armada"]
