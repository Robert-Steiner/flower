ARG rust_version=1.75
ARG base_image=debian

FROM rust:${rust_version} as builder

RUN apt-get update && apt-get install -y protobuf-compiler=3.21.12-3 --no-install-recommends  && rm -rf /var/lib/apt/lists/*

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
cp ./target/release/migration /bin/migration
EOF

FROM gcr.io/distroless/cc-debian12:nonroot as distroless

WORKDIR /app

COPY --from=builder /bin/migration /bin/
ENTRYPOINT ["migration"]
