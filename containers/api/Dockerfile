FROM ubuntu:23.10

RUN apt-get update && apt-get install -y ca-certificates python3 python3-pip openssl libpq-dev && rm -rf /var/lib/apt/lists/*

COPY containers/api/requirements.txt /tmp/
COPY libraries/metbuild/requirements.txt /app/
RUN pip install --break-system-packages --requirement /tmp/requirements.txt && pip install --break-system-packages --requirement /app/requirements.txt && rm -rf /tmp/requirements.txt /app/requirements.txt

WORKDIR /app

COPY libraries/metbuild /tmp/metbuild/.
RUN pip install --break-system-packages /tmp/metbuild/. && rm -rf /tmp/metbuild

COPY containers/api/metget_api /app/metget_api/.
COPY containers/api/metget.py /app/.

#ENTRYPOINT ["gunicorn"]
#CMD ["-w","4","metget:application","--bind","0.0.0.0:8080","--timeout","120"]

ENTRYPOINT ["python3"]
CMD [ "metget.py" ]
