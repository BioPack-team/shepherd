FROM postgres:bookworm

ARG POSTGRES_PASSWORD=supersecretpassw0rd
ENV POSTGRES_PASSWORD=$POSTGRES_PASSWORD

COPY shepherd_db/*.sql /docker-entrypoint-initdb.d/
