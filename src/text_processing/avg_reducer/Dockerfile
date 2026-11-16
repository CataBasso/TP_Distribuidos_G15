FROM tpdist/base:latest


COPY common ./common
COPY text_processing/avg_reducer .

ENTRYPOINT ["python3", "avg_reducer.py"]
