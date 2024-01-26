FROM ubuntu:23.10

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y ca-certificates python3 python3-pip openssl libpq-dev

COPY libraries/metdownload /tmp/metdownload/.
RUN pip install --break-system-packages /tmp/metdownload/. && rm -rf /tmp/metdownload

COPY libraries/triangle/src /tmp/triangle/.
RUN pip install --break-system-packages /tmp/triangle/. && rm -rf /tmp/triangle

COPY libraries/metbuild /tmp/metbuild/.
RUN pip install --break-system-packages /tmp/metbuild/. && rm -rf /tmp/metbuild

COPY containers/rebuild /tmp/metbuild_run/.
RUN pip install --break-system-packages /tmp/metbuild_run/. && rm -rf /tmp/metbuild_run

WORKDIR /app
