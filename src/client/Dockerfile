FROM python:3-slim

RUN pip3 install pika pyarrow

COPY common ./common
COPY client ./client

ENTRYPOINT ["python3", "-m", "client"]
