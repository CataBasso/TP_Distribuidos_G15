FROM python:3-alpine

RUN pip3 install pika

COPY common ./common
COPY filters/__init__.py ./filters/__init__.py
COPY filters/year_filter ./filters/year_filter

ENTRYPOINT ["python3", "-m", "filters.year_filter.year_filter"]
