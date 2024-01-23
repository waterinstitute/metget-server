FROM ubuntu:23.10

WORKDIR /app

#...Basic software installation
RUN apt-get update && apt-get install -y ca-certificates python3 python3-pip openssl \
    libgdal-dev libpq-dev libeccodes-dev vim && rm -rf /var/lib/apt/lists/*

COPY libraries/metdownload /tmp/metdownload/.
RUN pip install --break-system-packages /tmp/metdownload/. && rm -rf /tmp/metdownload

COPY libraries/metbuild /tmp/metbuild/.
RUN pip install --break-system-packages /tmp/metbuild/. && rm -rf /tmp/metbuild

#...Install the metget-build entrypoint script(s)
COPY containers/build /tmp/metbuild_run/.
RUN pip install --break-system-packages /tmp/metbuild_run/. && rm -rf /tmp/metbuild_run
