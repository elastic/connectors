FROM python:3.11-slim-bookworm
RUN apt -y update && apt -y upgrade && apt -y install make git
COPY . /app
WORKDIR /app
RUN make clean install
RUN ln -s .venv/bin /app/bin
ENTRYPOINT []
