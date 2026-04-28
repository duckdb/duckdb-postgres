#!/bin/bash
set -e
set -x

# Create test tables on YugabyteDB for integration tests.
# Expects YSQL connection via PGHOST/PGPORT/PGUSER/PGPASSWORD env vars.
#
# YugabyteDB can be slow to accept connections after startup.
# Retry with backoff before giving up.

MAX_RETRIES=30
RETRY_DELAY=5

echo "Waiting for YugabyteDB to accept connections..."
for i in $(seq 1 $MAX_RETRIES); do
  if psql -d yugabyte -c "SELECT 1" >/dev/null 2>&1; then
    echo "YugabyteDB is ready (attempt $i)"
    break
  fi
  if [ "$i" -eq "$MAX_RETRIES" ]; then
    echo "ERROR: YugabyteDB did not become ready after $((MAX_RETRIES * RETRY_DELAY)) seconds"
    exit 1
  fi
  echo "Attempt $i/$MAX_RETRIES failed, retrying in ${RETRY_DELAY}s..."
  sleep $RETRY_DELAY
done

dropdb --if-exists postgresscanner || true
createdb postgresscanner

# Hash-partitioned table (default for YugabyteDB) — 100k rows to exercise parallel scan
psql -d postgresscanner -c "
CREATE TABLE hash_test (
  id INTEGER PRIMARY KEY,
  name TEXT,
  value INTEGER
);
INSERT INTO hash_test SELECT g, 'row_' || g, g * 10 FROM generate_series(1, 100000) g;
ANALYZE hash_test;
"

# Wide table with various types to stress the COPY path
psql -d postgresscanner -c "
CREATE TABLE wide_test (
  id INTEGER PRIMARY KEY,
  col_text TEXT,
  col_int BIGINT,
  col_float DOUBLE PRECISION,
  col_bool BOOLEAN,
  col_ts TIMESTAMP,
  col_date DATE
);
INSERT INTO wide_test
SELECT g,
       'text_' || g,
       g * 100000::BIGINT,
       g * 3.14159,
       (g % 2 = 0),
       '2024-01-01'::TIMESTAMP + (g || ' seconds')::INTERVAL,
       '2024-01-01'::DATE + g
FROM generate_series(1, 50000) g;
ANALYZE wide_test;
"

# Simple test table for attach/detach cycles
psql -d postgresscanner -c "
CREATE TABLE test (i INTEGER);
INSERT INTO test VALUES (1), (2), (3), (NULL);
"

# Null test table
psql -d postgresscanner -c "
CREATE TABLE nulltest (
  c1 INTEGER, c2 INTEGER, c3 INTEGER, c4 INTEGER, c5 INTEGER,
  c6 INTEGER, c7 INTEGER, c8 INTEGER, c9 INTEGER, c10 INTEGER
);
INSERT INTO nulltest VALUES (1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
INSERT INTO nulltest VALUES (1, NULL, 3, 4, NULL, 6, 7, 8, NULL, 10);
INSERT INTO nulltest VALUES (NULL, NULL, 3, 4, 5, 6, 7, NULL, NULL, NULL);
INSERT INTO nulltest VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
"

# Multi-column hash key table — tests compound partition key parallel scan
psql -d postgresscanner -c "
CREATE TABLE multi_hash (
  region TEXT,
  id INTEGER,
  data TEXT,
  PRIMARY KEY (region, id)
);
INSERT INTO multi_hash SELECT 'region_' || (g % 5), g, 'data_' || g FROM generate_series(1, 10000) g;
ANALYZE multi_hash;
"

# Range-partitioned tables (no hash key — yb_num_hash_key_columns = 0)
# These must fall back to single-threaded scan, NOT yb_hash_code() ranges.

# Single-column range key
psql -d postgresscanner -c "
CREATE TABLE range_single (
  id INTEGER,
  name TEXT,
  value INTEGER,
  PRIMARY KEY (id ASC)
) SPLIT AT VALUES ((2500), (5000), (7500));
INSERT INTO range_single SELECT g, 'range_' || g, g * 10 FROM generate_series(1, 10000) g;
ANALYZE range_single;
"

# Compound range key (timeseries pattern)
psql -d postgresscanner -c "
CREATE TABLE range_ts (
  ts TIMESTAMP,
  sensor_id INTEGER,
  reading DOUBLE PRECISION,
  PRIMARY KEY (ts ASC, sensor_id ASC)
);
INSERT INTO range_ts
SELECT '2024-01-01'::TIMESTAMP + (g || ' seconds')::INTERVAL,
       g % 100,
       random() * 1000
FROM generate_series(1, 20000) g;
ANALYZE range_ts;
"

# Range key with DESC ordering
psql -d postgresscanner -c "
CREATE TABLE range_desc (
  created_at TIMESTAMP,
  id INTEGER,
  payload TEXT,
  PRIMARY KEY (created_at DESC, id DESC)
);
INSERT INTO range_desc
SELECT '2024-06-01'::TIMESTAMP - (g || ' seconds')::INTERVAL,
       g,
       'payload_' || g
FROM generate_series(1, 15000) g;
ANALYZE range_desc;
"

# Colocated database — all tables share a single tablet (no hash partitioning)
# This exercises the non-parallel scan fallback and tests that yb_table_properties
# returns 0 tablets for colocated tables.
# YugabyteDB colocated databases must be created with colocation=true at CREATE DATABASE time.
dropdb --if-exists postgresscanner_colocated || true
psql -d yugabyte -c "CREATE DATABASE postgresscanner_colocated WITH colocation = true"
psql -d postgresscanner_colocated -c "
CREATE TABLE coloc_test (
  id INTEGER PRIMARY KEY,
  name TEXT,
  value INTEGER
);
INSERT INTO coloc_test SELECT g, 'coloc_' || g, g * 10 FROM generate_series(1, 10000) g;
ANALYZE coloc_test;
"

psql -d postgresscanner_colocated -c "
CREATE TABLE coloc_wide (
  id INTEGER PRIMARY KEY,
  col_text TEXT,
  col_int BIGINT,
  col_float DOUBLE PRECISION,
  col_bool BOOLEAN
);
INSERT INTO coloc_wide
SELECT g, 'text_' || g, g * 100000::BIGINT, g * 3.14, (g % 2 = 0)
FROM generate_series(1, 5000) g;
ANALYZE coloc_wide;
"

# Non-colocated table in the same database for contrast
psql -d postgresscanner_colocated -c "
CREATE TABLE non_coloc_test (
  id INTEGER PRIMARY KEY,
  name TEXT,
  value INTEGER
) WITH (colocation = false);
INSERT INTO non_coloc_test SELECT g, 'nocoloc_' || g, g * 10 FROM generate_series(1, 10000) g;
ANALYZE non_coloc_test;
"

echo "YugabyteDB test tables created successfully"
echo ""
echo "  Database: postgresscanner (non-colocated, default)"
echo "    hash_test:     100,000 rows (hash-partitioned, single key)"
echo "    wide_test:      50,000 rows (hash-partitioned, multiple types)"
echo "    multi_hash:     10,000 rows (hash-partitioned, compound key)"
echo "    range_single:   10,000 rows (range ASC, SPLIT AT VALUES)"
echo "    range_ts:       20,000 rows (range ASC compound key, timeseries)"
echo "    range_desc:     15,000 rows (range DESC compound key)"
echo "    test:                4 rows (simple)"
echo "    nulltest:            4 rows (null patterns)"
echo ""
echo "  Database: postgresscanner_colocated"
echo "    coloc_test:     10,000 rows (colocated, single tablet)"
echo "    coloc_wide:      5,000 rows (colocated, multiple types)"
echo "    non_coloc_test: 10,000 rows (non-colocated in colocated db)"
