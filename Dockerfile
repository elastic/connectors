FROM python:3.10


RUN git clone https://github.com/elastic/connectors-python /app/
WORKDIR /app
RUN make clean install
