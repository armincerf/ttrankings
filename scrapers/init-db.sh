#!/bin/bash

# Load environment variables from the specified .env file
export $(grep -v '^#' mkttl_match_card_scraper/.env | xargs)

# Parse DATABASE_URL
PROTO="$(echo $DATABASE_URL | grep :// | sed -e's,^\(.*://\).*,\1,g')"
URL="$(echo ${DATABASE_URL/$PROTO/})"
USERPASS="$(echo $URL | grep @ | cut -d@ -f1)"
USER="$(echo $USERPASS | cut -d: -f1)"
PASSWORD="$(echo $USERPASS | cut -d: -f2)"
HOSTPORT="$(echo ${URL/$USERPASS@/} | cut -d/ -f1)"
HOST="$(echo $HOSTPORT | sed -e 's,:.*,,g')"
PORT="$(echo $HOSTPORT | sed -e 's,.*:,,g')"
DBNAME="$(echo $URL | grep / | cut -d/ -f2-)"

# Run the Docker container
docker run --name pgduckdb-container \
  -e POSTGRES_USER=$USER \
  -e POSTGRES_PASSWORD=$PASSWORD \
  -e POSTGRES_DB=$DBNAME \
  -p $PORT:5432 \
  -d pgduckdb/pgduckdb:16-main

