FROM python:3.10
COPY . /app
WORKDIR /app
RUN make clean install
