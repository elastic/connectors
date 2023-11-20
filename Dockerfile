FROM python:3.11.6-slim
COPY . /app
WORKDIR /app
RUN apt update && \
    apt upgrade -y && \
    apt install -y make build-essential && \
    rm -rf /var/lib/apt/lists/*
RUN make clean install PYTHON=python3.11
