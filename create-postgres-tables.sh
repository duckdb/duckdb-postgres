#!/bin/bash
set -e

DUCKDB_PATH=duckdb
if test -f build/release/duckdb; then
  DUCKDB_PATH=build/release/duckdb
elif test -f build/reldebug/duckdb; then
  DUCKDB_PATH=build/reldebug/duckdb
elif test -f build/debug/duckdb; then
  DUCKDB_PATH=build/debug/duckdb
fi

echo "
CREATE SCHEMA tpch; 
CREATE SCHEMA tpcds;
CALL dbgen(sf=0.01, schema='tpch');
CALL dsdgen(sf=0.01, schema='tpcds');
EXPORT DATABASE '/tmp/postgresscannertmp';
" | \
$DUCKDB_PATH

dropdb --if-exists postgresscanner
createdb postgresscanner

psql -d postgresscanner < /tmp/postgresscannertmp/schema.sql
psql -d postgresscanner < /tmp/postgresscannertmp/load.sql
rm -rf /tmp/postgresscannertmp

psql -d postgresscanner < test/all_pg_types.sql
psql -d postgresscanner < test/decimals.sql
psql -d postgresscanner < test/legacy_text_protocol.sql
psql -d postgresscanner < test/other.sql


psql -d postgresscanner -c "CHECKPOINT"
psql -d postgresscanner -c "VACUUM"
