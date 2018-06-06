FROM cassandra:3
COPY ./db.cql initdb/db.cql
COPY ./keyspace.cql initdb/keyspace.cql
COPY init.sh /init.sh
ENTRYPOINT ["/init.sh"]
CMD ["cassandra", "-f"]