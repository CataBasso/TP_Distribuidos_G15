FROM python:3-alpine

RUN pip3 install pika

COPY common ./common
COPY text_processing/top_merger .

ENTRYPOINT ["python3", "top_merger.py"]
