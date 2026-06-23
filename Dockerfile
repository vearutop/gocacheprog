FROM golang:alpine AS build
ARG TARGETOS TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -ldflags="-s -w" -o /gocacheprogd .

FROM alpine:3.21
RUN apk --no-cache add ca-certificates tzdata
COPY --from=build /gocacheprogd /usr/local/bin/gocacheprogd

# Entrypoint wrapper: always injects -cache-dir /data so callers only pass
# the remaining flags (e.g. -auth-token, -https-host, -max-disk-bytes …).
RUN printf '#!/bin/sh\nexec /usr/local/bin/gocacheprogd -cache-dir /data "$@"\n' \
    > /entrypoint.sh && chmod +x /entrypoint.sh

VOLUME ["/data"]
EXPOSE 80 443
ENTRYPOINT ["/entrypoint.sh"]
