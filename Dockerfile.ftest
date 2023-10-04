FROM python:3.10
COPY . /connectors
WORKDIR /connectors
RUN make clean install
RUN pip install -r requirements/ftest.txt
