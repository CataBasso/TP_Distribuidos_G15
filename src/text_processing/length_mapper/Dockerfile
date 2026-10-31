FROM python:3-alpine

RUN pip3 install pika

COPY common ./common
COPY text_processing/length_mapper .

ENTRYPOINT ["python3", "length_mapper.py"]
