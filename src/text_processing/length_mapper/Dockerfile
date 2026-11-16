FROM tpdist/base:latest


COPY common ./common
COPY text_processing/length_mapper .

ENTRYPOINT ["python3", "length_mapper.py"]
