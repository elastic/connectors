FROM cgr.dev/chainguard/wolfi-base:latest@sha256:d7d42af987333417272165a51dd7aed9cfd47067ac701ea927263364b12d64ad
ARG python_version=3.11

USER root
RUN apk add --no-cache python3=~${python_version} make git

COPY --chown=nonroot:nonroot . /app

USER nonroot
WORKDIR /app
RUN make clean install
RUN ln -s .venv/bin /app/bin

USER root
RUN apk del make git

USER nonroot
ENTRYPOINT []
