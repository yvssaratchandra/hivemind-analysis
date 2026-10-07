# Build context is the repository root; compose lives in research/exo/two-peer.
FROM python:3.13-slim-bookworm

COPY --from=ghcr.io/astral-sh/uv:0.12.19 /uv /uvx /bin/

ARG EXO_REVISION=unknown
LABEL org.opencontainers.image.title="Exo two-peer research image" \
      org.opencontainers.image.revision="${EXO_REVISION}"

ENV CARGO_HOME=/usr/local/cargo \
    RUSTUP_HOME=/usr/local/rustup \
    CARGO_BUILD_JOBS=2 \
    RUSTUP_TOOLCHAIN=nightly \
    PATH="/opt/exo/.venv/bin:/usr/local/cargo/bin:${PATH}" \
    UV_PROJECT_ENVIRONMENT=/opt/exo/.venv \
    UV_LINK_MODE=copy \
    EXO_HOME=/var/lib/exo \
    EXO_RESOURCES_DIR=/opt/exo/resources \
    EXO_DASHBOARD_DIR=/opt/exo/dashboard-assets \
    PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install --no-install-recommends -y \
        build-essential \
        ca-certificates \
        curl \
        git \
        libssl-dev \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- -y --default-toolchain nightly --profile minimal

WORKDIR /opt/exo
COPY exo/ /opt/exo/

RUN mkdir -p "$EXO_HOME" "$EXO_DASHBOARD_DIR" \
    && uv sync --locked --no-dev --extra mlx-none

ENTRYPOINT ["/opt/exo/.venv/bin/exo"]
