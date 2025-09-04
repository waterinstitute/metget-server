FROM condaforge/miniforge3:latest AS conda

ARG DEBIAN_FRONTEND=noninteractive
ARG PYTHON_VERSION=3.13
ARG TARGETARCH
ARG TARGETPLATFORM

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN apt-get update && apt-get install -y --no-install-recommends build-essential libboost-dev libgmp-dev libmpfr-dev libcgal-dev

RUN conda create -y -p /env python=${PYTHON_VERSION} boto3 bs4 certifi cfgrib feedparser geojson \
                                     geopandas h5py loguru netCDF4 numba numpy psycopg2 \
                                     python-dateutil requests schema scipy shapely sqlalchemy \
                                     xarray eccodes

COPY src/libraries/fasttri /tmp/fasttri/.
COPY src/libraries/libmetget/ /tmp/libmetget/.
COPY src/executables/build /tmp/metbuild_run/.

RUN if [[ "x${TARGETARCH}" == "xamd64" ]] ; then \
      # Provide a generic set of x86_64 optimization flags
      COPT="-march=core-avx2"; \
    elif [[ "x${TARGETARCH}" == "xarm64" ]] ; then \
      # Provide a generic set of ARM optimization flags for Graviton
      COPT="-mcpu=neoverse-v1"; \
    fi; \
    CFLAGS="-DNDEBUG -O3 -funroll-loops $COPT" /env/bin/pip install -v /tmp/fasttri/.
RUN /env/bin/pip install /tmp/libmetget/.
RUN /env/bin/pip install /tmp/metbuild_run/.

FROM debian:stable-slim AS metget

LABEL maintainer="Zach Cobell <zcobell@thewaterinstitute.org>"
LABEL version="0.5.1"
LABEL description="METGET Build"
LABEL io.k8s.description="METGET Build"
LABEL io.k8s.display-name="METGET Build"

COPY --from=conda /env /env

ENV PATH="/env/bin:${PATH}"
ENV LD_LIBRARY_PATH="/env/lib"

# Link the conda python to the system python
RUN ln -s /env/bin/python /usr/bin/python && ln -s /env/bin/pip /usr/bin/pip && \
    ln -s /env/bin/python3 /usr/bin/python3 && ln -s /env/bin/pip3 /usr/bin/pip3

# Create a user to run the application
RUN useradd -ms /bin/bash metget && mkdir /app && chown -R metget:metget /app

# Set the root password
RUN echo "root:metget" | chpasswd

USER metget

# Set the working directory and change permissions
WORKDIR /app
