FROM tpdist/base:latest

RUN pip3 install --no-cache-dir pyarrow

COPY common ./common
COPY client ./client

ENTRYPOINT ["python3", "-m", "client"]
