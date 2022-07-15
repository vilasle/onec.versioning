#!/bin/bash
PGBIN=/etc/init.d/postgresql

$PGBIN start

echo ' * Create user $ONECUSER:'

su postgres -c 'psql --command "create user $ONECUSER WITH PASSWORD '\''$ONECPASSWORD'\'';"'
su postgres -c 'psql --command "alter role $ONECUSER WITH LOGIN CREATEDB;"'
export PGUSER=$ONECUSER
export PGPASSWORD=$ONECPASSWORD

echo ' * Create user $PGUSER:'

psql --host localhost --dbname postgres --command "CREATE DATABASE $PGUSER;"

echo ' * Create tables:'
psql --host localhost -a -f /tmp/create_tables.sql

$PGBIN stop