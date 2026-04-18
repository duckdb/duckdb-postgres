#!/bin/bash
set -e
set -x

DUCKDB_PATH=duckdb
if test -f build/release/duckdb; then
  DUCKDB_PATH=build/release/duckdb
elif test -f build/reldebug/duckdb; then
  DUCKDB_PATH=build/reldebug/duckdb
elif test -f build/debug/duckdb; then
  DUCKDB_PATH=build/debug/duckdb
fi

if [ -z "${PGSCANNERTMP_ABS_DIR_PREFIX}" ]; then
  ABS_DIR_PREFIX="/tmp"
else
  ABS_DIR_PREFIX="${PGSCANNERTMP_ABS_DIR_PREFIX//\\//}"
fi

echo "
CREATE SCHEMA tpch; 
CREATE SCHEMA tpcds;
CALL dbgen(sf=0.01, schema='tpch');
CALL dsdgen(sf=0.01, schema='tpcds');
EXPORT DATABASE '${ABS_DIR_PREFIX}/postgresscannertmp';
" | \
$DUCKDB_PATH

dropdb --if-exists postgresscanner
createdb postgresscanner

psql -d postgresscanner < ${ABS_DIR_PREFIX}/postgresscannertmp/schema.sql
psql -d postgresscanner < ${ABS_DIR_PREFIX}/postgresscannertmp/load.sql
rm -rf ${ABS_DIR_PREFIX}/postgresscannertmp

psql -d postgresscanner < test/all_pg_types.sql
psql -d postgresscanner < test/decimals.sql
psql -d postgresscanner < test/other.sql


psql -d postgresscanner -c "CHECKPOINT"
psql -d postgresscanner -c "VACUUM"
