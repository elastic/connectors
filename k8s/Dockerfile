FROM python:3.10


RUN pip install --upgrade pip
RUN pip install supervisor
RUN pip install git+https://github.com/elastic/connectors-python.git@main

CMD ["supervisord", "-c", "/app/config/supervisord.conf"]
