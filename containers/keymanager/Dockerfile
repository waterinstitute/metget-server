FROM ubuntu:22.04

RUN apt-get update && apt-get install -y ca-certificates python3 python3-pip \
    openssl libpq-dev && rm -rf /var/lib/apt/lists/*

COPY containers/keymanager/requirements.txt /tmp/
RUN pip install --requirement /tmp/requirements.txt

WORKDIR /app

COPY libraries/metbuild /tmp/metbuild/.
RUN pip install /tmp/metbuild/. && rm -rf /tmp/metbuild

COPY containers/keymanager/metget-key-manager /usr/bin/.
