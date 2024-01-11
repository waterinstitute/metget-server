FROM ubuntu:23.04

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y ca-certificates python3 python3-pip openssl postgresql libpq-dev

COPY containers/download/requirements.txt /tmp/

RUN pip install --break-system-packages --requirement /tmp/requirements.txt

COPY libraries/metbuild /tmp/metbuild/.
RUN pip install --break-system-packages /tmp/metbuild/. && rm -rf /tmp/metbuild

WORKDIR /app

COPY containers/download/metgetlib /app/metgetlib
COPY containers/rebuild/rebuild_database.py /app/rebuild_database.py
