# Copied from kannika-platform/core/Dockerfile
ARG RUST_VERSION=1.87
ARG ALPINE_VERSION=3.21
ARG KNK_CORE_BUILD_MODE=release

FROM rust:$RUST_VERSION-alpine$ALPINE_VERSION AS base
ENV RUSTFLAGS="-Ctarget-feature=-crt-static -Clink-arg=-fuse-ld=mold"
RUN apk add --no-cache bash build-base sccache mold musl-dev openssl-dev cyrus-sasl-dev zlib-dev zstd-dev librdkafka-dev protoc yq
WORKDIR /usr/src/kannika-bridge
ENV RUSTC_WRAPPER=sccache SCCACHE_DIR=/sccache

# Run if KNK_CORE_BUILD_MODE is 'debug'
FROM base AS core-builder-debug
COPY . .
RUN --mount=type=ssh \
    --mount=type=cache,target=$SCCACHE_DIR,sharing=locked \
    cargo -v build
RUN echo "⚠️  NOTE: This is the **DEBUG** version of the core image ⚠️"

# Run if KNK_CORE_BUILD_MODE is 'release'
FROM base AS core-builder-release
COPY . .
RUN --mount=type=ssh \
    --mount=type=cache,target=$SCCACHE_DIR,sharing=locked \
    cargo -v build --release

# Alias to core-builder-{release,debug} that trips docker up
FROM core-builder-${KNK_CORE_BUILD_MODE} AS core-builder

# Bundle Stage
FROM alpine:$ALPINE_VERSION
ARG KNK_CORE_BUILD_MODE
RUN apk add --no-cache ca-certificates librdkafka cyrus-sasl libgcc zstd zlib jemalloc bash
## Override the default allocator with jemalloc for the whole container.
ENV LD_PRELOAD="/usr/lib/libjemalloc.so.2"
COPY --from=core-builder /usr/src/kannika-bridge/target/${KNK_CORE_BUILD_MODE}/kbridge /usr/bin/bridge-cli

ENTRYPOINT ["/bin/bash"]
