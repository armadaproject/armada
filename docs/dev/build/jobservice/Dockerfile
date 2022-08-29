FROM alpine:3.10

ARG APP_UID
ARG APP_GID

RUN addgroup -S -g ${APP_GID} armada && adduser -S -u ${APP_UID} armada -G armada

USER armada

COPY ./jobservice /app/

COPY ./config/ /app/config/jobservice


WORKDIR /app

ENTRYPOINT ["./jobservice"]
