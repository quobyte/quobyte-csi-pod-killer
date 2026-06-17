ARG TARGETOS
ARG TARGETARCH
ARG BUILD_MODE=release
ARG APP_NAME=pod_killer

FROM --platform=$BUILDPLATFORM golang:1.25-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .

ARG TARGETOS
ARG TARGETARCH
ARG BUILD_MODE
ARG APP_NAME

RUN if [ "$BUILD_MODE" = "debug" ]; then \
      CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
      go build -gcflags="all=-N -l" -o /app/${APP_NAME} . ; \
      readlink -f ${APP_NAME}; \
    else \
      CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
        go build -o /app/${APP_NAME}.unstripped . ; \
      # Go keeps sufficient info to recover obfusticated strace using abov
      # unstripped version.
      CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
        go build -ldflags="-s -w" -o /app/${APP_NAME} . ; \
    fi

FROM alpine:latest AS root-prep
ARG APP_NAME
RUN mkdir /final_root/; \
    cd /final_root; \
    mkdir -p var tmp run bin; \
    ln -s var/run run; \
    chmod -R 755 var tmp run bin

FROM scratch AS export
ARG APP_NAME
COPY --from=builder /app/${APP_NAME}* /

FROM scratch
ARG APP_NAME
COPY --from=root-prep /final_root/ /
COPY --from=builder /app/${APP_NAME} /bin/pod_killer
ENTRYPOINT ["/bin/pod_killer"]