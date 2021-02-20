FROM postgres

ADD create_tables.sql /docker-entrypoint-initdb.d/

# Copy postgres config file into container
COPY postgresql.conf /etc/postgresql

# Override default postgres config file
CMD ["postgres", "-c", "config_file=/etc/postgresql/postgresql.conf"]
