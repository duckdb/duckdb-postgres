# YugabyteDB Integration for duckdb-postgres

**Date:** 2026-04-27
**Branch:** feat/secret-options (fork of duckdb/postgres_scanner)
**Target:** YugabyteDB v2025.2+
**Approach:** Fork-first, no upstream ceremony

## Overview

Make the duckdb-postgres extension YugabyteDB-aware with correct behavior, native parallelism, and optimized COPY. Three phases, each building on the last.

## Phase A: Correctness

### A1. YugabyteDB Detection

Add `YUGABYTE` to the `PostgresInstanceType` enum in `postgres_version.hpp`.

Detect via version string in `PostgresConnection::GetPostgresVersion()` (`postgres_connection.cpp:160-176`). YugabyteDB version strings contain `-YB-`:

```
PostgreSQL 11.2-YB-2025.2.0.0 on x86_64-...
```

Detection:
```cpp
if (StringUtil::Contains(pg_version_string, "-YB-")) {
    version.type_v = PostgresInstanceType::YUGABYTE;
}
```

Extract the YB version string (e.g., `2025.2.0.0`) into a new `yb_version` field on `PostgresVersion` for future feature gating.

**Files:**
- `src/include/postgres_version.hpp` -- add `YUGABYTE` to enum, add `string yb_version` field
- `src/postgres_connection.cpp` -- add detection in `GetPostgresVersion()`
- `src/postgres_utils.cpp` -- parse YB version substring

### A2. Disable CTID Scan

YugabyteDB uses LSM storage, not heap pages. CTID ranges are meaningless.

In `PostgresScanFunction::PrepareBind()` (`postgres_scanner.cpp:118-139`), add after the `version.major_v < 14` check:

```cpp
if (version.type_v == PostgresInstanceType::YUGABYTE) {
    use_ctid_scan = false;
}
```

This forces single-threaded scan as a safe baseline until Phase B adds YB-native parallelism.

**Files:**
- `src/postgres_scanner.cpp`

### A3. Skip pg_export_snapshot()

YugabyteDB uses hybrid logical clocks (HLC) for MVCC. `pg_export_snapshot()` is unnecessary and may not behave correctly.

In `PostgresGetSnapshot()` (`postgres_scanner.cpp:67-107`), add early return alongside the existing Aurora check:

```cpp
if (version.type_v == PostgresInstanceType::YUGABYTE) {
    return;
}
```

**Files:**
- `src/postgres_scanner.cpp`

### A4. Replace DISCARD ALL in Connection Reset

`DISCARD ALL` clobbers session GUCs (`statement_timeout`, `search_path`, etc.). For YugabyteDB connections, replace with targeted cleanup.

In `PostgresConnection::Reset()` (`postgres_connection.cpp:205-226`):

```cpp
if (instance_type == PostgresInstanceType::YUGABYTE) {
    PGresult *res = PQexec(conn, "RESET ALL; DEALLOCATE ALL; CLOSE ALL; UNLISTEN *");
} else {
    PGresult *res = PQexec(conn, "DISCARD ALL");
}
```

Add an `instance_type` field to `OwnedPostgresConnection`. Since `Open()` doesn't perform a version query today, the instance type is set lazily: the first `GetPostgresVersion()` call (which happens at bind time) stores the detected type back on the connection. The pool's `ResetConnection` passes through to the type-aware `Reset`. Connections that haven't been typed yet default to `POSTGRES` (standard `DISCARD ALL` behavior).

**Files:**
- `src/postgres_connection.cpp` -- type-aware Reset
- `src/include/postgres_connection.hpp` -- `PostgresInstanceType instance_type` field on `OwnedPostgresConnection`

### A5. Fix Cardinality Estimation

`relpages` from `pg_class` is meaningless on YugabyteDB (returns 0 or stale values).

For YugabyteDB tables, query `yb_table_properties()` to get tablet count and hash key column info:

```sql
SELECT num_tablets, num_hash_key_columns
FROM yb_table_properties('schema.table'::regclass)
```

Use `num_tablets` as the parallelism hint (replacing `pages_approx`). Store `num_hash_key_columns` for Phase B sharding strategy selection.

In `postgres_table_set.cpp`, add a YugabyteDB-specific metadata query alongside or after `GetInitializeQuery()`.

**Files:**
- `src/storage/postgres_table_set.cpp` -- tablet count query
- `src/postgres_scanner.cpp` -- use tablet count for cardinality in `PostgresScanCardinality()`
- `src/include/postgres_scanner.hpp` -- add `idx_t yb_num_tablets`, `idx_t yb_num_hash_key_columns` to `PostgresBindData`

## Phase B: Parallelism

### B1. Tserver Discovery

At ATTACH time, if instance type is YUGABYTE, query the cluster topology:

```sql
SELECT host, port, node_type, cloud, region, zone, ip_address
FROM yb_servers()
```

Store as `YugabyteTopology` on the `PostgresCatalog`:

```cpp
struct YugabyteTserver {
    string host;
    int32_t port;
    string cloud, region, zone;
    string ip_address;
};

struct YugabyteTopology {
    vector<YugabyteTserver> tservers;
    bool direct_connect_available = false;
};
```

**Connectivity probe:** After discovery, attempt a lightweight `PQconnectdb` + `PQstatus` check to each tserver's postgres port (2-second timeout). If any succeed, `direct_connect_available = true`. Failed tservers marked unavailable but retained for retry on cache refresh.

**Cache lifetime:** Topology cached for the ATTACH lifetime. Cleared by `pg_clear_postgres_cache()`.

**Files:**
- New: `src/include/yugabyte_topology.hpp` -- structs
- `src/storage/postgres_catalog.cpp` -- query `yb_servers()`, run connectivity probe
- `src/include/storage/postgres_catalog.hpp` -- add `YugabyteTopology` member
- `src/storage/postgres_connection_pool.cpp` -- overload `CreateNewConnection` for specific host:port

### B2. Hash-Code Parallel Scanning

For hash-sharded tables (`yb_num_hash_key_columns > 0`), split YugabyteDB's hash space (0-65535) into N ranges where N = min(num_tablets, pool_size).

Each parallel task gets:
```sql
WHERE yb_hash_code(pk_col1, pk_col2) BETWEEN range_min AND range_max
```

**Discovering hash partition columns:** Query the first `num_hash_key_columns` columns of the primary key:

```sql
SELECT a.attname
FROM pg_index i
JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
WHERE i.indrelid = 'schema.table'::regclass AND i.indisprimary
ORDER BY array_position(i.indkey, a.attnum)
LIMIT num_hash_key_columns
```

**Connection routing:** If `direct_connect_available`, distribute hash ranges to tserver connections round-robin. Not tablet-leader-aware (avoids tablet metadata lookup complexity) but still provides data locality in most cases.

**Fallback:** If tservers not directly reachable, run hash-code splitting over the existing connection pool pointing at the load balancer.

New fields on `PostgresBindData`:
```cpp
idx_t yb_num_tablets = 0;
idx_t yb_num_hash_key_columns = 0;
vector<string> yb_hash_partition_columns;
```

New field on `PostgresGlobalState`:
```cpp
idx_t yb_hash_idx = 0;  // next hash range to assign
```

`PostgresParallelStateNext` gains a YugabyteDB path that assigns hash ranges instead of page ranges. `PostgresInitInternal` builds the query with `yb_hash_code() BETWEEN` instead of `ctid BETWEEN`.

**Files:**
- `src/postgres_scanner.cpp` -- parallel state next, init internal, global state
- `src/include/postgres_scanner.hpp` -- new fields on bind data
- `src/storage/postgres_table_set.cpp` -- hash partition column query
- `src/storage/postgres_catalog.cpp` -- pass topology for connection routing

### B3. Non-Hash Table Fallback

For range-sharded or no-PK tables (`yb_num_hash_key_columns == 0`), fall back to single-threaded scan. Future optimization: `ybctid` range splitting via `yb_get_range_split_clause`.

### B4. Snapshot Handling for Parallel Scans

All YugabyteDB parallel scan connections use REPEATABLE READ explicitly, regardless of the catalog's isolation level setting:

```
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
```

No `SET TRANSACTION SNAPSHOT` needed. YugabyteDB's HLC-based MVCC ensures all REPEATABLE READ transactions see consistent data without explicit snapshot export/import.

In `PostgresScanConnect()` (`postgres_scanner.cpp:315-331`), when instance type is YUGABYTE, force REPEATABLE READ and skip the snapshot SET.

**Files:**
- `src/postgres_scanner.cpp` -- `PostgresScanConnect` YugabyteDB path

## Phase C: COPY Optimization

### C1. Tserver-Routed COPY for Reads

When `direct_connect_available` and table is hash-sharded, parallel scan threads connect directly to tservers (from Phase B). The COPY query inherits the `yb_hash_code() BETWEEN` filter, so each tserver ships only local data -- no cross-tserver shuffling.

No additional COPY changes needed. The binary reader works unchanged since it receives standard PostgreSQL binary COPY format.

### C2. Batched Transactions for COPY FROM

YugabyteDB performs better with bounded transaction sizes on distributed writes.

New setting `pg_yb_rows_per_transaction` (default 10000). After each batch, commit and begin a new transaction:

```
BEGIN;
COPY table FROM STDIN (FORMAT binary)  -- send N rows
COMMIT;
BEGIN;
COPY table FROM STDIN (FORMAT binary)  -- next N rows
...
```

**Files:**
- `src/postgres_copy_from.cpp` -- batch commit logic

### C3. Bulk Load Optimization

New setting `pg_yb_disable_transactional_writes` (default false). When enabled, push `SET yb_disable_transactional_writes = true` before COPY FROM operations. This bypasses YugabyteDB transaction overhead for bulk loads.

Opt-in only -- disabling transactional writes means no rollback on failure.

**Files:**
- `src/postgres_copy_from.cpp` -- set GUC before COPY

### C4. Progress Reporting

Replace page-based progress (`page_idx / pages_approx`) with hash-range progress (`yb_hash_idx / num_tasks`) for YugabyteDB scans in `PostgresScanProgress()`.

**Files:**
- `src/postgres_scanner.cpp` -- progress calculation

## New Settings

| Setting | Type | Default | Purpose |
|---------|------|---------|---------|
| `pg_yb_rows_per_transaction` | UBIGINT | 10000 | COPY FROM batch commit size |
| `pg_yb_disable_transactional_writes` | BOOLEAN | false | Opt-in bulk load (no rollback) |

Registered in `postgres_extension.cpp`.

## Files Summary

**Modified:**
- `src/include/postgres_version.hpp`
- `src/include/postgres_connection.hpp`
- `src/include/postgres_scanner.hpp`
- `src/include/storage/postgres_catalog.hpp`
- `src/postgres_connection.cpp`
- `src/postgres_scanner.cpp`
- `src/postgres_utils.cpp`
- `src/storage/postgres_catalog.cpp`
- `src/storage/postgres_connection_pool.cpp`
- `src/storage/postgres_table_set.cpp`
- `src/postgres_copy_from.cpp`
- `src/postgres_extension.cpp`

**New:**
- `src/include/yugabyte_topology.hpp`
