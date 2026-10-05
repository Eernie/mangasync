FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/mangasync ./cmd/mangasync

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/mangasync /mangasync
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/mangasync"]
