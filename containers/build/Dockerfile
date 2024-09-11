FROM condaforge/miniforge3:latest as conda

RUN conda create -y -p /env python=3 boto3 bs4 certifi cfgrib feedparser geojson \
                                     geopandas h5py netCDF4 numba numpy psycopg2 \
                                     python-dateutil requests schema scipy shapely sqlalchemy \
                                     xarray eccodes

FROM debian:bookworm-20240904-slim as metget-build
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y gcc vim && rm -rf /var/lib/apt/lists/*

COPY --from=conda /env /env
ENV PATH="/env/bin:${PATH}"
ENV LD_LIBRARY_PATH="/env/lib:${LD_LIBRARY_PATH}"

# Link the conda python to the system python
RUN ln -s /env/bin/python /usr/bin/python && ln -s /env/bin/pip /usr/bin/pip && \
    ln -s /env/bin/python3 /usr/bin/python3 && ln -s /env/bin/pip3 /usr/bin/pip3

COPY src/libraries/triangle/src /tmp/triangle/.
RUN pip install /tmp/triangle/. && rm -rf /tmp/triangle

COPY src/libraries/libmetget/ /tmp/libmetget/.
RUN pip install /tmp/libmetget/. && rm -rf /tmp/libmetget

COPY src/executables/build /tmp/metbuild_run/.
RUN pip install /tmp/metbuild_run/. && rm -rf /tmp/metbuild_run

# Create a user to run the application
RUN useradd -ms /bin/bash metget
USER metget

# Set the working directory and change permissions
WORKDIR /app
RUN chown -R metget:metget /app
