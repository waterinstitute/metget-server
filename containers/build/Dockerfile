FROM ubuntu:22.04

WORKDIR /app

#...Basic software installation
RUN apt-get update && apt-get install -y ca-certificates python3 python3-pip openssl \
    libgdal-dev libpq-dev libeccodes-dev vim && rm -rf /var/lib/apt/lists/*

#...Install the python dependencies from the requirements.txt files
COPY containers/build/requirements.txt /tmp/
COPY libraries/metbuild/requirements.txt /app/
RUN pip install --requirement /tmp/requirements.txt && pip install --requirement /app/requirements.txt && rm -rf /tmp/requirements.txt /app/requirements.txt

#...Install the metbuild python library
COPY libraries/metbuild /tmp/metbuild
RUN pip install /tmp/metbuild/. && rm -rf /tmp/metbuild

#...Install the metget-build entrypoint script(s)
COPY containers/build/*.py /app/

COPY request.json /app/
