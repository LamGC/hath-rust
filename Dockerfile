# syntax=docker/dockerfile:1
FROM rust:trixie AS builder
ARG TARGETARCH
ARG HATH_BUILD_COMMIT
ARG CARGO_BUILD_JOBS
ENV HATH_BUILD_COMMIT=${HATH_BUILD_COMMIT}
WORKDIR /usr/src/hath
COPY Cargo.toml Cargo.lock build.rs ./
COPY src/ ./src/
RUN test -n "$HATH_BUILD_COMMIT"
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/usr/src/hath/target,id=hath-target-${TARGETARCH} \
    if [ "$TARGETARCH" = "arm64" ]; then export JEMALLOC_SYS_WITH_LG_PAGE=16; fi; \
    cargo install --locked --path . --root /out

FROM debian:trixie-slim
ARG HATH_BUILD_COMMIT
LABEL org.opencontainers.image.source="https://github.com/LamGC/hath-rust" \
      org.opencontainers.image.revision=${HATH_BUILD_COMMIT} \
      org.opencontainers.image.licenses="GPL-3.0-or-later"
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libgcc-s1 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 hath \
    && useradd --uid 10001 --gid 10001 --no-create-home --home-dir /hath --shell /usr/sbin/nologin hath \
    && install -d -o 10001 -g 10001 -m 0700 /hath
WORKDIR /hath
COPY --from=builder /out/bin/hath-rust /usr/local/bin/hath-rust
USER 10001:10001
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/hath-rust"]
