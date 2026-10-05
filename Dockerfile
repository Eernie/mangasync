# Build on the runner's native platform and cross-compile for the target (no QEMU needed).
FROM --platform=$BUILDPLATFORM golang:1.27 AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w" -o /out/mangasync ./cmd/mangasync \
    && mkdir /out/data

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/mangasync /mangasync
# /data holds the SQLite state; owned by nonroot so a fresh volume is writable.
COPY --from=build --chown=65532:65532 /out/data /data
USER nonroot:nonroot
EXPOSE 8080
VOLUME /data
ENTRYPOINT ["/mangasync"]
