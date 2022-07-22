#!/bin/bash
PGBIN=/etc/init.d/postgresql

$PGBIN start

echo ' * Create user $SERVICEUSER:'

su postgres -c 'psql --command "create user $SERVICEUSER WITH PASSWORD '\''$SERVICEPASSWORD'\'';"'
su postgres -c 'psql --command "alter role $SERVICEUSER WITH LOGIN CREATEDB;"'
export PGUSER=$SERVICEUSER
export PGPASSWORD=$SERVICEPASSWORD

echo ' * Create user $PGUSER:'

psql --host localhost --dbname postgres --command "CREATE DATABASE $PGUSER;"

echo ' * Create tables:'
psql --host localhost -a -f /tmp/create_tables.sql

$PGBIN stop
