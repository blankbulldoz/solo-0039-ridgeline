# syntax=docker/dockerfile:1

# Ridgeline service image.
# Stage 1 produces a fully static binary so the runtime image needs no libc
# toolchain; stage 2 runs as an unprivileged user with no shell requirements.
FROM golang:1.25-alpine AS build
WORKDIR /src
# Cache module downloads separately from source changes.
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# Static binary: no cgo, stripped, module proxy pinned by go.sum.
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath \
    -ldflags='-s -w -buildid=' \
    -o /out/ridgeline ./cmd/ridgeline

FROM alpine:3.20
# Ridgeline writes only to RIDGE_DB; everything else is read-only at runtime.
ARG RIDGELINE_UID=10001
RUN adduser -D -H -u "${RIDGELINE_UID}" ridgeline \
 && mkdir -p /var/lib/ridgeline \
 && chown -R ridgeline:ridgeline /var/lib/ridgeline \
 && chmod 0700 /var/lib/ridgeline
COPY --from=build /out/ridgeline /usr/local/bin/ridgeline
USER ridgeline
# Busybox wget is present in alpine and is enough for the loopback probe.
HEALTHCHECK --interval=10s --timeout=3s --start-period=3s --retries=3 \
  CMD wget -q -O - http://127.0.0.1:8399/ridge/v1/health || exit 1
# The binary binds 0.0.0.0 inside the container; Compose publishes the port
# on the host loopback address only, so nothing is exposed beyond this host.
ENV RIDGE_DB=/var/lib/ridgeline/ridgeline.db \
    RIDGE_ADDR=0.0.0.0:8399
VOLUME ["/var/lib/ridgeline"]
ENTRYPOINT ["/usr/local/bin/ridgeline"]
