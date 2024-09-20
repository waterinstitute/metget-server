FROM postgres:15.8-bookworm
ADD containers/database/schema.sql /docker-entrypoint-initdb.d
EXPOSE 5432
RUN echo "root:metget" | chpasswd
USER postgres
