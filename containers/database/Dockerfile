FROM postgres:15.8-bookworm

LABEL maintainer="Zach Cobell <zcobell@thewaterinstitute.org>"
LABEL version="0.1.7"
LABEL description="METGET Database"
LABEL io.k8s.description="METGET Database"
LABEL io.k8s.display-name="METGET Database"

ADD containers/database/schema.sql /docker-entrypoint-initdb.d
EXPOSE 5432
RUN echo "root:metget" | chpasswd
USER postgres
