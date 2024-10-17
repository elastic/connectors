FROM python:3.11-slim-bookworm
RUN apt update && apt upgrade && apt install make -y
COPY . /app
WORKDIR /app
RUN make clean install
RUN ln -s .venv/bin /app/bin
ENTRYPOINT []
