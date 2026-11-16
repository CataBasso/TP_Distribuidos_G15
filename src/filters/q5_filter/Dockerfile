FROM tpdist/base:latest


COPY common ./common
COPY filters/q5_filter .

ENTRYPOINT ["python3", "q5_filter.py"]
