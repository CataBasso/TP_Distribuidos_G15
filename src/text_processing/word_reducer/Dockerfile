FROM tpdist/base:latest


COPY common ./common
COPY text_processing/word_reducer .

ENTRYPOINT ["python3", "word_reducer.py"]
