# syntax=docker/dockerfile:1
ARG RUST_VERSION
FROM ghcr.io/wallaroolabs/rust:${RUST_VERSION} AS base

FROM base AS build
ARG BUILD_COMMIT
ENV BUILD_COMMIT=$BUILD_COMMIT
ARG TARGETARCH
WORKDIR /usr/src/plateau
COPY . .
RUN \
    if [ "${TARGETARCH}" = "amd64" ]; then ARCH=x86_64; elif [ "${TARGETARCH}" = "arm64" ]; then ARCH=aarch64; else exit 1; fi && \
    cargo build --release --target ${ARCH}-unknown-linux-musl -p plateau && \
    cp target/${ARCH}-unknown-linux-musl/release/plateau target/release/plateau

FROM us-docker.pkg.dev/wallaroo-dev-253816/docker-hub-us/library/debian:bullseye-slim

LABEL org.opencontainers.image.vendor="Wallaroo Labs"
LABEL org.opencontainers.image.source="https://github.com/WallarooLabs/plateau/Dockerfile"
LABEL org.opencontainers.image.title="plateau"

COPY --from=build /usr/src/plateau/target/release/plateau .
CMD ["./plateau"]
