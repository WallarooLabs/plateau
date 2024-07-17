# syntax=docker/dockerfile:1
ARG RUST_VERSION
FROM ghcr.io/wallaroolabs/rust:${RUST_VERSION} AS base

FROM base AS build
ARG BUILD_COMMIT
ENV BUILD_COMMIT=$BUILD_COMMIT
WORKDIR /usr/src/plateau
COPY . .
RUN \
    cargo build --release --target x86_64-unknown-linux-musl -p plateau

FROM us-docker.pkg.dev/wallaroo-dev-253816/docker-hub-us/library/debian:bullseye-slim

LABEL org.opencontainers.image.vendor="Wallaroo Labs"
LABEL org.opencontainers.image.source="https://github.com/WallarooLabs/plateau/Dockerfile"
LABEL org.opencontainers.image.title="plateau"

COPY --from=build --link /usr/src/plateau/target/x86_64-unknown-linux-musl/release/plateau .
CMD ["./plateau"]
