FROM condaforge/miniforge3 AS conda

ARG DEBIAN_FRONTEND=noninteractive
ARG PYTHON_VERSION=3.13

# Use Azure mirrors for faster package downloads (supports both legacy sources.list and deb822 format)
RUN if [ -f /etc/apt/sources.list.d/ubuntu.sources ]; then \
        sed -i 's|http://archive.ubuntu.com|http://azure.archive.ubuntu.com|g' /etc/apt/sources.list.d/ubuntu.sources && \
        sed -i 's|http://security.ubuntu.com|http://azure.archive.ubuntu.com|g' /etc/apt/sources.list.d/ubuntu.sources && \
        sed -i 's|http://ports.ubuntu.com|http://azure.ports.ubuntu.com|g' /etc/apt/sources.list.d/ubuntu.sources; \
    fi && \
    if [ -f /etc/apt/sources.list ]; then \
        sed -i 's|http://archive.ubuntu.com|http://azure.archive.ubuntu.com|g' /etc/apt/sources.list && \
        sed -i 's|http://security.ubuntu.com|http://azure.archive.ubuntu.com|g' /etc/apt/sources.list && \
        sed -i 's|http://ports.ubuntu.com|http://azure.ports.ubuntu.com|g' /etc/apt/sources.list; \
    fi

RUN apt-get update && apt-get install -y --no-install-recommends build-essential libboost-dev libgmp-dev libmpfr-dev libcgal-dev

RUN conda create -y -p /env python=${PYTHON_VERSION} boto3 bs4 certifi cfgrib feedparser geojson \
                                     geopandas h5py loguru netCDF4 numba numpy psycopg2 \
                                     python-dateutil requests schema scipy shapely sqlalchemy \
                                     xarray

COPY src/libraries/fasttri /tmp/fasttri/.
COPY src/libraries/libmetget/ /tmp/libmetget/.
COPY src/executables/download /tmp/metbuild_download/.

RUN /env/bin/pip install /tmp/fasttri/.
RUN /env/bin/pip install /tmp/libmetget/.
RUN /env/bin/pip install /tmp/metbuild_download/.

FROM debian:stable-slim AS metget

LABEL maintainer="Zach Cobell <zcobell@thewaterinstitute.org>"
LABEL version="0.8.2"
LABEL description="METGET Download"
LABEL io.k8s.description="METGET Download"
LABEL io.k8s.display-name="METGET Download"

COPY --from=conda /env /env

# Link the conda python to the system python
RUN ln -s /env/bin/python /usr/bin/python && ln -s /env/bin/pip /usr/bin/pip && \
    ln -s /env/bin/python3 /usr/bin/python3 && ln -s /env/bin/pip3 /usr/bin/pip3

ENV PATH="/env/bin:${PATH}"
ENV LD_LIBRARY_PATH="/env/lib"

# Create a user to run the application
RUN useradd -ms /bin/bash metget && mkdir /app && chown -R metget:metget /app

# Set the root password
RUN echo "root:metget" | chpasswd

USER metget

# Set the working directory and change permissions
WORKDIR /app
