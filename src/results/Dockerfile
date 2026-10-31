FROM python:3-alpine

RUN pip3 install pika

COPY common ./common
COPY results .

ENTRYPOINT ["python3", "joiner.py"]
