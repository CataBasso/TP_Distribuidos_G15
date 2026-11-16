FROM tpdist/base:latest


COPY common ./common
COPY text_processing/word_mapper .

ENTRYPOINT ["python3", "word_mapper.py"]
