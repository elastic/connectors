FROM cgr.dev/chainguard/wolfi-base:latest
ARG python_version=3.11

USER root
RUN apk add --no-cache python3=~${python_version} make git

COPY --chown=nonroot:nonroot . /app

USER nonroot
WORKDIR /app
RUN make clean install-package
RUN ln -s app/connectors_service/.venv/bin /app/bin

USER root
RUN apk del make git

USER nonroot
ENTRYPOINT []
