FROM python:3-alpine

RUN pip3 install pika

COPY common ./common
COPY filters/q2_filter .

ENTRYPOINT ["python3", "query2.py"]