FROM ubuntu:23.04

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y ca-certificates python3 python3-pip openssl libpq-dev

COPY libraries/metdownload /tmp/metdownload/.
RUN pip install --break-system-packages /tmp/metdownload/. && rm -rf /tmp/metdownload

COPY libraries/metbuild /tmp/metbuild/.
RUN pip install --break-system-packages /tmp/metbuild/. && rm -rf /tmp/metbuild

WORKDIR /app

COPY containers/rebuild/metget-rebuild /usr/bin/metget-rebuild
