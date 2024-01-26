FROM ubuntu:23.10

RUN apt-get update && apt-get install -y ca-certificates python3 python3-pip openssl \
    libgdal-dev libpq-dev libeccodes-dev vim && rm -rf /var/lib/apt/lists/*

COPY libraries/metdownload /tmp/metdownload/.
RUN pip install --break-system-packages /tmp/metdownload/. && rm -rf /tmp/metdownload

COPY libraries/triangle/src /tmp/triangle/.
RUN pip install --break-system-packages /tmp/triangle/. && rm -rf /tmp/triangle

COPY libraries/metbuild /tmp/metbuild/.
RUN pip install --break-system-packages /tmp/metbuild/. && rm -rf /tmp/metbuild

COPY containers/rebuild /tmp/metbuild_rebuild/.
RUN pip install --break-system-packages /tmp/metbuild_rebuild/. && rm -rf /tmp/metbuild_rebuild

WORKDIR /app
