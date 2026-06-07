FROM python:3.11-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jre-headless \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf "$(dirname $(dirname $(readlink -f $(which java))))" /opt/java

ENV JAVA_HOME=/opt/java

RUN pip install uv

WORKDIR /app

COPY pyproject.toml .
RUN python3 -c "\
import tomllib, subprocess; \
deps = tomllib.load(open('pyproject.toml', 'rb'))['project']['dependencies']; \
subprocess.check_call(['uv', 'pip', 'install', '--system'] + deps)"
