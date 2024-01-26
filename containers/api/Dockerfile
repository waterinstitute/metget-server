FROM ubuntu:23.10

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y ca-certificates python3 python3-pip openssl \
    libgdal-dev libpq-dev libeccodes-dev vim && rm -rf /var/lib/apt/lists/*

COPY src/libraries/triangle/src /tmp/triangle/.
RUN pip install --break-system-packages /tmp/triangle/. && rm -rf /tmp/triangle

COPY src/libraries/libmetget/ /tmp/libmetget/.
RUN pip install --break-system-packages /tmp/libmetget/. && rm -rf /tmp/libmetget

COPY src/executables/api /tmp/api/.
RUN pip install --break-system-packages /tmp/api/. && rm -rf /tmp/metget_api

ENTRYPOINT ["gunicorn"]
CMD ["-w","4","metget_api.api:application","--bind","0.0.0.0:8080","--timeout","120"]

#ENTRYPOINT ["python3"]
#CMD [ "-m", "flask", "--app", "metget_api.api:application", "run", "--port", "8080", "--host", "0.0.0.0" ]
