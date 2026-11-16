FROM tpdist/base:latest


COPY common ./common
COPY text_processing/abstract_cleaner .

ENTRYPOINT ["python3", "abstract_cleaner.py"]
