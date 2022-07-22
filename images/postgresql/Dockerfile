FROM ubuntu

EXPOSE 5432

ENV TZ=Asia/Yekaterinburg
ENV SERVICEUSER onecversion
ENV SERVICEPASSWORD onecversion
ENV PGDATA /var/lib/postgresql/14/main

STOPSIGNAL SIGINT

RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && \
    echo $TZ > /etc/timezone && \
    apt-get update && apt-get install -y postgresql-14 vim

COPY initdb.sh /tmp/
COPY create_tables.sql /tmp/

RUN /tmp/initdb.sh && rm -rf  /tmp/*

USER postgres

COPY conf/pg_hba.conf /var/lib/postgresql/14/main/pg_hba.conf
COPY conf/postgresql.conf /var/lib/postgresql/14/main/postgresql.conf
COPY conf/pg_hba.conf /etc/postgresql/14/main/pg_hba.conf

ENTRYPOINT [ "/usr/lib/postgresql/14/bin/postgres" ]
