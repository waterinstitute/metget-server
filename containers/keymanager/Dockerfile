FROM ubuntu:23.10

RUN apt-get update && apt-get install -y ca-certificates python3 python3-pip \
    openssl libpq-dev && rm -rf /var/lib/apt/lists/*

COPY libraries/triangle/src /tmp/triangle/.
RUN pip install --break-system-packages /tmp/triangle/. && rm -rf /tmp/triangle

COPY libraries/metbuild /tmp/metbuild/.
RUN pip install --break-system-packages /tmp/metbuild/. && rm -rf /tmp/metbuild

COPY containers/keymanager /tmp/keymanager/.
RUN pip install --break-system-packages /tmp/keymanager/. && rm -rf /tmp/keymanager

WORKDIR /app
