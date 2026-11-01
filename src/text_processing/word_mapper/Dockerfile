FROM python:3-alpine

RUN pip3 install pika

COPY common ./common
COPY text_processing/word_mapper .

ENTRYPOINT ["python3", "word_mapper.py"]
