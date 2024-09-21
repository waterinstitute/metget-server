FROM condaforge/miniforge3 AS conda

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends build-essential

RUN conda create -y -p /env python=3 boto3 bs4 certifi cfgrib feedparser geojson \
                                     geopandas h5py netCDF4 numba numpy psycopg2 \
                                     python-dateutil requests schema scipy shapely sqlalchemy \
                                     xarray

COPY src/libraries/triangle/src /tmp/triangle/.
COPY src/libraries/libmetget/ /tmp/libmetget/.
COPY src/executables/download /tmp/metbuild_download/.

RUN CFLAGS="-O3" /env/bin/pip install /tmp/triangle/.
RUN /env/bin/pip install /tmp/libmetget/.
RUN /env/bin/pip install /tmp/metbuild_download/.

FROM debian:bookworm-20240904-slim AS metget

LABEL maintainer="Zach Cobell <zcobell@thewaterinstitute.org>"
LABEL version="0.1.7"
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
USER metget

# Set the working directory and change permissions
WORKDIR /app
