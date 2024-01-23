FROM ubuntu:23.10

RUN apt-get update && apt-get install -y ca-certificates python3 python3-pip openssl libpq-dev && rm -rf /var/lib/apt/lists/*

COPY libraries/metdownload /tmp/metdownload/.
RUN pip install --break-system-packages /tmp/metdownload/. && rm -rf /tmp/metdownload

COPY libraries/metbuild /tmp/metbuild/.
RUN pip install --break-system-packages /tmp/metbuild/. && rm -rf /tmp/metbuild
WORKDIR /app

COPY containers/api /tmp/api/.
RUN pip install --break-system-packages /tmp/api/. && rm -rf /tmp/metget_api

ENTRYPOINT ["gunicorn"]
CMD ["-w","4","metget_api.api:application","--bind","0.0.0.0:8080","--timeout","120"]

# ENTRYPOINT ["python3"]
# CMD [ "-m", "flask", "--app", "metget_api.api:application", "run" ]
