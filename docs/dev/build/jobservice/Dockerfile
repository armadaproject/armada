
FROM alpine:3.10

ARG APP_UID
ARG APP_GID

# For macos, the user's GID likely collides with something already assigned in /etc/group
RUN $(grep -v ":${APP_GID}:" /etc/group > /tmp/group && mv /tmp/group /etc/group) \
    && addgroup -S -g ${APP_GID} armada \
    && adduser -S -u ${APP_UID} armada -g armada

USER armada

COPY ./jobservice /app/

COPY ./config/ /app/config/jobservice


WORKDIR /app

ENTRYPOINT ["./jobservice"]
