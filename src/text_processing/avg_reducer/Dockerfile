FROM python:3-alpine

RUN pip3 install pika

COPY common ./common
COPY text_processing/avg_reducer .

ENTRYPOINT ["python3", "avg_reducer.py"]
