FROM condaforge/miniforge3 AS conda

ARG DEBIAN_FRONTEND=noninteractive
ARG PYTHON_VERSION=3.12

RUN apt-get update && apt-get install -y --no-install-recommends build-essential

RUN conda create -y -p /env python=${PYTHON_VERSION} boto3 bs4 certifi cfgrib feedparser geojson \
                                     geopandas h5py netCDF4 numba numpy psycopg2 \
                                     python-dateutil requests schema scipy shapely sqlalchemy \
                                     xarray flask flask-limiter flask-restful \
                                     flask-cors pika && \
    /env/bin/pip install flask-healthz && \
    conda clean -afy && \
    find /env -follow -type f -name '*.a' -delete && \
    find /env -follow -type f -name '*.pyc' -delete && \
    find /env -follow -type f -name '*.js.map' -delete

COPY src/libraries/triangle/src /tmp/triangle/.
COPY src/libraries/libmetget/ /tmp/libmetget/.
COPY src/executables/api /tmp/api/.

RUN /env/bin/pip install /tmp/triangle/.
RUN /env/bin/pip install /tmp/libmetget/.
RUN /env/bin/pip install /tmp/api/.

FROM debian:stable-slim AS metget

LABEL maintainer="Zach Cobell <zcobell@thewaterinstitute.org>"
LABEL version="0.3.1"
LABEL description="METGET API"
LABEL io.k8s.description="METGET API"
LABEL io.k8s.display-name="METGET API"

COPY --from=conda /env /env

RUN ln -s /env/bin/python /usr/bin/python && \
    ln -s /env/bin/flask /usr/bin/flask && \
    ln -s /env/bin/gunicorn /usr/bin/gunicorn

ENV PATH="/env/bin:${PATH}"
ENV LD_LIBRARY_PATH="/env/lib"

RUN useradd -m metget && chown -R metget:metget /home/metget

# Set the root password
RUN echo "root:metget" | chpasswd

USER metget
WORKDIR /home/metget

ENTRYPOINT ["gunicorn"]
CMD ["-w","4","metget_api.api:application","--bind","0.0.0.0:8080","--timeout","120"]

#ENTRYPOINT ["python3"]
#CMD [ "-m", "flask", "--app", "metget_api.api:application", "run", "--port", "8080", "--host", "0.0.0.0" ]
