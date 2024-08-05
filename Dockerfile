FROM python:3.12-alpine
# RUN apt update && apt install make
RUN apk update && apk upgrade && apk add --no-cache bash make gcc libc-dev linux-headers libffi-dev
COPY . /app
WORKDIR /app
RUN make clean install
RUN apk del gcc libc-dev linux-headers libffi-dev
