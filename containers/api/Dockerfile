FROM ubuntu:23.10

RUN apt-get update && apt-get install -y ca-certificates python3 python3-pip openssl \
    libgdal-dev libpq-dev libeccodes-dev vim && rm -rf /var/lib/apt/lists/*

COPY libraries/metdownload /tmp/metdownload/.
RUN pip install --break-system-packages /tmp/metdownload/. && rm -rf /tmp/metdownload

COPY libraries/triangle/src /tmp/triangle/.
RUN pip install --break-system-packages /tmp/triangle/. && rm -rf /tmp/triangle

COPY libraries/metbuild /tmp/metbuild/.
RUN pip install --break-system-packages /tmp/metbuild/. && rm -rf /tmp/metbuild
WORKDIR /app

COPY containers/api /tmp/api/.
RUN pip install --break-system-packages /tmp/api/. && rm -rf /tmp/metget_api

ENTRYPOINT ["gunicorn"]
CMD ["-w","4","metget_api.api:application","--bind","0.0.0.0:8080","--timeout","120"]

#ENTRYPOINT ["python3"]
#CMD [ "-m", "flask", "--app", "metget_api.api:application", "run", "--port", "8080", "--host", "0.0.0.0" ]
