ARG rust_version=1.75
ARG base_image=debian

FROM rust:${rust_version} as builder

RUN apt-get update && apt-get install -y protobuf-compiler=3.21.12-3 --no-install-recommends  && rm -rf /var/lib/apt/lists/*
RUN GRPC_HEALTH_PROBE_VERSION=v0.4.13 && \
    wget -qO/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64 && \
    chmod +x /bin/grpc_health_probe

WORKDIR /app

RUN --mount=type=bind,source=rust/src,target=rust/src \
    --mount=type=bind,source=rust/build.rs,target=rust/build.rs \
    --mount=type=bind,source=rust/Cargo.toml,target=rust/Cargo.toml \
    --mount=type=bind,source=rust/Cargo.lock,target=rust/Cargo.lock \
    --mount=type=cache,target=/app/rust/target/ \
    --mount=type=cache,target=/usr/local/cargo/registry/ \
    --mount=type=bind,source=proto,target=proto \
    <<EOF
set -e
cd rust
cargo build --locked --release
cp ./target/release/superlink /bin/superlink
EOF

FROM debian:12-slim as debian
RUN apt-get update && apt-get install -y openssl --no-install-recommends  && rm -rf /var/lib/apt/lists/*
COPY --from=builder /bin/grpc_health_probe /bin/grpc_health_probe

FROM gcr.io/distroless/cc-debian12:nonroot as distroless

FROM ${base_image}

WORKDIR /app

COPY --from=builder /bin/superlink /bin/
ENTRYPOINT ["superlink"]
