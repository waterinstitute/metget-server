FROM postgres:16-bookworm
ADD containers/database/schema.sql /docker-entrypoint-initdb.d
EXPOSE 5432

RUN useradd -ms /bin/bash metget && mkdir /app && chown -R metget:metget /app
USER metget
