FROM python:3.10
ARG BRANCH

RUN git clone https://github.com/elastic/connectors-python /app/
WORKDIR /app
RUN git checkout $BRANCH
RUN make clean install
