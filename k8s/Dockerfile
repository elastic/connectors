FROM python:3.10


RUN pip install --upgrade pip
RUN pip install supervisor
RUN pip install git+https://2062f518d36dc2526d51772d864d1053dd79451b@github.com/elastic/connectors-python.git@main

CMD ["supervisord", "-c", "/app/config/supervisord.conf"]
