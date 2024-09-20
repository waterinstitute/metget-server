FROM postgres:16-bookworm
ADD containers/database/schema.sql /docker-entrypoint-initdb.d
EXPOSE 5432
