FROM golang:1.26.1-bookworm@sha256:ab3d6955bbc813a0f3fdf220c1d817dd89c0b3f283777db8ece4a32fe7858edd AS build
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -o /out/proxy ./cmd/proxy && \
    CGO_ENABLED=0 go build -trimpath -o /out/backend ./cmd/backend && \
    CGO_ENABLED=0 go build -trimpath -o /out/loadgen ./cmd/loadgen

FROM scratch
COPY --from=build /out/ /app/
USER 65532:65532
CMD ["/app/proxy"]
