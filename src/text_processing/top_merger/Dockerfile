FROM tpdist/base:latest


COPY common ./common
COPY text_processing/top_merger .

ENTRYPOINT ["python3", "top_merger.py"]
