FROM python:3-alpine

RUN pip3 install pika

COPY common ./common
COPY filters/year_filter .

ENTRYPOINT ["python3", "year_filter.py"]
