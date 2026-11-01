FROM python:3-alpine

RUN pip3 install pika

COPY common ./common
COPY text_processing/abstract_cleaner .

ENTRYPOINT ["python3", "abstract_cleaner.py"]
