FROM python:3-alpine

RUN pip3 install pika

COPY common ./common
COPY gateway ./gateway

ENTRYPOINT ["python3", "-m", "gateway"]
