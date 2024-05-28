FROM ubuntu:23.10

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y ca-certificates python3 python3-pip openssl \
    libgdal-dev libpq-dev libeccodes-dev vim && rm -rf /var/lib/apt/lists/*

RUN pip install --break-system-packages boto3 bs4 certifi cfgrib feedparser geojson \
                                        geopandas h5py netCDF4 numba numpy psycopg2 \
                                        python-dateutil requests schema scipy shapely sqlalchemy \
                                        xarray

COPY src/libraries/triangle/src /tmp/triangle/.
RUN pip install --break-system-packages /tmp/triangle/. && rm -rf /tmp/triangle

COPY src/libraries/libmetget/ /tmp/libmetget/.
RUN pip install --break-system-packages /tmp/libmetget/. && rm -rf /tmp/libmetget

COPY src/executables/download /tmp/metbuild_download/.
RUN pip install --break-system-packages /tmp/metbuild_download/. && rm -rf /tmp/metbuild_download

WORKDIR /app
