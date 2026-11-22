FROM tpdist/base:latest
RUN apt-get update && apt-get install -y --no-install-recommends docker-cli && rm -rf /var/lib/apt/lists/*
COPY common ./common
COPY supervisor ./supervisor
ENTRYPOINT ["python3", "-m", "supervisor"]
