FROM ubuntu:23.10

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y ca-certificates python3 python3-pip openssl \
    libgdal-dev libpq-dev libeccodes-dev vim && rm -rf /var/lib/apt/lists/*

COPY src/libraries/triangle/src /tmp/triangle/.
RUN pip install --break-system-packages /tmp/triangle/. && rm -rf /tmp/triangle

COPY src/libraries/libmetget/ /tmp/libmetget/.
RUN pip install --break-system-packages /tmp/libmetget/. && rm -rf /tmp/libmetget

COPY src/executables/download /tmp/metbuild_download/.
RUN pip install --break-system-packages /tmp/metbuild_download/. && rm -rf /tmp/metbuild_download

WORKDIR /app
