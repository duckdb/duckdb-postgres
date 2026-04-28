# YugabyteDB Integration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the duckdb-postgres extension YugabyteDB-aware with correct scan behavior, tablet-aware hash-code parallelism, tserver discovery, and optimized COPY.

**Architecture:** Add YUGABYTE to the existing PostgresInstanceType enum and gate all YB-specific behavior behind instance type checks, following the established Aurora/Redshift pattern. Three phases: correctness fixes, native parallelism, COPY optimization.

**Tech Stack:** C++, libpq, DuckDB extension API, YugabyteDB system functions (yb_servers, yb_hash_code, yb_table_properties)

**Spec:** `docs/superpowers/specs/2026-04-27-yugabyte-integration-design.md`

---

## File Structure

**Modified files:**
- `src/include/postgres_version.hpp` -- add YUGABYTE enum value, yb_version field
- `src/include/postgres_connection.hpp` -- add instance_type to OwnedPostgresConnection
- `src/include/postgres_scanner.hpp` -- add YB fields to PostgresBindData
- `src/include/storage/postgres_catalog.hpp` -- add YugabyteTopology member, accessor
- `src/include/storage/postgres_table_entry.hpp` -- add YB metadata fields to PostgresTableInfo and PostgresTableEntry
- `src/postgres_connection.cpp` -- YB detection in GetPostgresVersion, type-aware Reset
- `src/postgres_scanner.cpp` -- disable CTID, skip snapshot, hash-code parallel scan, progress
- `src/storage/postgres_catalog.cpp` -- query yb_servers at ATTACH, store topology
- `src/storage/postgres_connection_pool.cpp` -- tserver-targeted connection creation
- `src/storage/postgres_table_set.cpp` -- query yb_table_properties, hash partition columns
- `src/storage/postgres_table_entry.cpp` -- pass YB metadata through PrepareBind
- `src/postgres_copy_to.cpp` -- bulk load GUC, batch commit helper
- `src/postgres_extension.cpp` -- register new settings

**New files:**
- `src/include/yugabyte_topology.hpp` -- YugabyteTserver and YugabyteTopology structs

---

## Phase A: Correctness

### Task 1: Add YUGABYTE Instance Type and Version Detection

**Files:**
- Modify: `src/include/postgres_version.hpp:15` (enum), `src/include/postgres_version.hpp:17-27` (struct fields)
- Modify: `src/postgres_connection.cpp:160-176` (GetPostgresVersion)

- [x] **Step 1: Add YUGABYTE to PostgresInstanceType enum**

In `src/include/postgres_version.hpp`, change line 15:

```cpp
enum class PostgresInstanceType { UNKNOWN, POSTGRES, AURORA, REDSHIFT, YUGABYTE };
```

And add a `yb_version` field to `PostgresVersion` after the `type_v` field:

```cpp
struct PostgresVersion {
	PostgresVersion() {
	}
	PostgresVersion(idx_t major_v, idx_t minor_v, idx_t patch_v = 0)
	    : major_v(major_v), minor_v(minor_v), patch_v(patch_v) {
	}

	idx_t major_v = 0;
	idx_t minor_v = 0;
	idx_t patch_v = 0;
	PostgresInstanceType type_v = PostgresInstanceType::POSTGRES;
	string yb_version;

	// existing operator overloads unchanged
};
```

- [x] **Step 2: Add YugabyteDB detection in GetPostgresVersion**

In `src/postgres_connection.cpp`, replace lines 160-176 with:

```cpp
PostgresVersion PostgresConnection::GetPostgresVersion(ClientContext &context) {
	auto result = TryQuery(context, "SELECT version(), (SELECT COUNT(*) FROM pg_settings WHERE name LIKE 'rds%')");
	if (!result) {
		PostgresVersion version;
		version.type_v = PostgresInstanceType::UNKNOWN;
		return version;
	}
	auto pg_version_string = result->GetString(0, 0);
	auto version = PostgresUtils::ExtractPostgresVersion(pg_version_string);
	if (result->GetInt64(0, 1) > 0) {
		version.type_v = PostgresInstanceType::AURORA;
	}
	if (StringUtil::Contains(pg_version_string, "Redshift")) {
		version.type_v = PostgresInstanceType::REDSHIFT;
	}
	if (StringUtil::Contains(pg_version_string, "-YB-")) {
		version.type_v = PostgresInstanceType::YUGABYTE;
		auto yb_start = pg_version_string.find("-YB-");
		if (yb_start != string::npos) {
			yb_start += 4;
			auto yb_end = pg_version_string.find(' ', yb_start);
			if (yb_end == string::npos) {
				yb_end = pg_version_string.size();
			}
			version.yb_version = pg_version_string.substr(yb_start, yb_end - yb_start);
		}
	}
	if (connection) {
		connection->instance_type = version.type_v;
	}
	return version;
}
```

- [x] **Step 3: Build and verify**

```bash
make -j$(nproc) -C build/release 2>&1 | tail -20
```
Expected: Clean build.

- [x] **Step 4: Commit**

```bash
git add src/include/postgres_version.hpp src/postgres_connection.cpp
git commit -m "feat: add YUGABYTE instance type and version detection

Detect YugabyteDB via '-YB-' in the PostgreSQL version string.
Extract the YB version (e.g., 2025.2.0.0) for future feature gating."
```

### Task 2: Disable CTID Scan for YugabyteDB

**Files:**
- Modify: `src/postgres_scanner.cpp:118-139` (PrepareBind)

- [x] **Step 1: Add YUGABYTE check to disable CTID scan**

In `src/postgres_scanner.cpp`, in `PostgresScanFunction::PrepareBind`, add after the `version.major_v < 14` block (around line 136):

```cpp
	if (version.type_v == PostgresInstanceType::YUGABYTE) {
		use_ctid_scan = false;
	}
```

- [x] **Step 2: Build and verify**

```bash
make -j$(nproc) -C build/release 2>&1 | tail -20
```

- [x] **Step 3: Commit**

```bash
git add src/postgres_scanner.cpp
git commit -m "fix: disable CTID scan for YugabyteDB

YugabyteDB uses LSM storage, not heap pages. CTID page ranges
are meaningless and would produce incorrect parallel scan plans."
```

### Task 3: Skip pg_export_snapshot() for YugabyteDB

**Files:**
- Modify: `src/postgres_scanner.cpp:67-107` (PostgresGetSnapshot)

- [x] **Step 1: Add early return for YUGABYTE**

In `src/postgres_scanner.cpp`, in `PostgresGetSnapshot`, add after the Aurora check at line 76:

```cpp
	if (version.type_v == PostgresInstanceType::YUGABYTE) {
		return;
	}
```

- [x] **Step 2: Build and verify**

```bash
make -j$(nproc) -C build/release 2>&1 | tail -20
```

- [x] **Step 3: Commit**

```bash
git add src/postgres_scanner.cpp
git commit -m "fix: skip pg_export_snapshot for YugabyteDB

YugabyteDB uses hybrid logical clocks for MVCC. Snapshot
export/import is unnecessary and may not behave correctly."
```

### Task 4: Type-Aware Connection Reset (Replace DISCARD ALL)

**Files:**
- Modify: `src/include/postgres_connection.hpp:26-34` (OwnedPostgresConnection)
- Modify: `src/postgres_connection.cpp:205-226` (Reset)

- [x] **Step 1: Add instance_type to OwnedPostgresConnection**

In `src/include/postgres_connection.hpp`, add the include and field:

After `#include "duckdb/common/shared_ptr.hpp"` add:
```cpp
#include "postgres_version.hpp"
```

Update the struct:
```cpp
struct OwnedPostgresConnection {
	explicit OwnedPostgresConnection(PGconn *conn = nullptr);
	OwnedPostgresConnection(const OwnedPostgresConnection &) = delete;
	OwnedPostgresConnection &operator=(const OwnedPostgresConnection &) = delete;
	~OwnedPostgresConnection();

	PGconn *connection;
	mutex connection_lock;
	PostgresInstanceType instance_type = PostgresInstanceType::POSTGRES;
};
```

- [x] **Step 2: Make Reset instance-type-aware**

In `src/postgres_connection.cpp`, replace the `Reset` method (lines 205-226):

```cpp
void PostgresConnection::Reset(const std::string &health_check_query) {
	if (!IsOpen()) {
		throw InternalException("Cannot reset a connection that is not open");
	}
	PGconn *conn = GetConn();
	auto tx_status = PQtransactionStatus(conn);
	if (tx_status == PQTRANS_INTRANS || tx_status == PQTRANS_INERROR) {
		PGresult *res = PQexec(conn, "ROLLBACK");
		PostgresResult res_holder(res);
	}
	if (connection->instance_type == PostgresInstanceType::YUGABYTE) {
		PGresult *res = PQexec(conn, "RESET ALL; DEALLOCATE ALL; CLOSE ALL; UNLISTEN *");
		PostgresResult res_holder(res);
		if (PQresultStatus(res) == PGRES_COMMAND_OK) {
			return;
		}
	} else {
		PGresult *res = PQexec(conn, "DISCARD ALL");
		PostgresResult res_holder(res);
		if (PQresultStatus(res) == PGRES_COMMAND_OK) {
			return;
		}
	}
	PQreset(conn);
	if (!PingServer(health_check_query)) {
		throw InternalException("Connection reset failure");
	}
}
```

- [x] **Step 3: Build and verify**

```bash
make -j$(nproc) -C build/release 2>&1 | tail -20
```

- [x] **Step 4: Commit**

```bash
git add src/include/postgres_connection.hpp src/postgres_connection.cpp
git commit -m "fix: replace DISCARD ALL with targeted reset for YugabyteDB

DISCARD ALL clobbers session GUCs like statement_timeout and
search_path. For YugabyteDB, use RESET ALL + DEALLOCATE ALL +
CLOSE ALL + UNLISTEN * instead."
```

### Task 5: Fix Cardinality Estimation with yb_table_properties

**Files:**
- Modify: `src/include/storage/postgres_table_entry.hpp:17-39` (PostgresTableInfo), `src/include/storage/postgres_table_entry.hpp:41-68` (PostgresTableEntry)
- Modify: `src/include/postgres_scanner.hpp:23-75` (PostgresBindData)
- Modify: `src/storage/postgres_table_set.cpp:121-143` (CreateEntries)
- Modify: `src/storage/postgres_table_entry.cpp:24-29` (constructor), `src/storage/postgres_table_entry.cpp:39-67` (GetScanFunction)
- Modify: `src/postgres_scanner.cpp:109-145` (PrepareBind)

- [x] **Step 1: Add YB metadata fields to PostgresTableInfo**

In `src/include/storage/postgres_table_entry.hpp`, add to `PostgresTableInfo` after `int64_t approx_num_pages`:

```cpp
	int64_t approx_num_pages = 0;
	idx_t yb_num_tablets = 0;
	idx_t yb_num_hash_key_columns = 0;
	vector<string> yb_hash_partition_columns;
```

Add corresponding fields to `PostgresTableEntry` after `std::atomic<int64_t> approx_num_pages`:

```cpp
	std::atomic<int64_t> approx_num_pages;
	idx_t yb_num_tablets = 0;
	idx_t yb_num_hash_key_columns = 0;
	vector<string> yb_hash_partition_columns;
```

- [x] **Step 2: Add YB fields to PostgresBindData**

In `src/include/postgres_scanner.hpp`, add after `idx_t max_threads = 1;`:

```cpp
	idx_t max_threads = 1;

	idx_t yb_num_tablets = 0;
	idx_t yb_num_hash_key_columns = 0;
	vector<string> yb_hash_partition_columns;
```

- [x] **Step 3: Add YB property loading helpers in postgres_table_set.cpp**

In `src/storage/postgres_table_set.cpp`, add before `CreateEntries`:

```cpp
static void LoadYugabyteTableProperties(PostgresTransaction &transaction, PostgresTableInfo &table_info,
                                        const string &schema_name) {
	string qualified = KeywordHelper::WriteQuoted(schema_name, '"') + "." +
	                   KeywordHelper::WriteQuoted(table_info.GetTableName(), '"');

	string props_query = StringUtil::Format(
	    "SELECT num_tablets, num_hash_key_columns FROM yb_table_properties('%s'::regclass)", qualified);
	try {
		auto result = transaction.Query(props_query);
		if (result && result->Count() > 0) {
			table_info.yb_num_tablets = result->IsNull(0, 0) ? 0 : result->GetInt64(0, 0);
			table_info.yb_num_hash_key_columns = result->IsNull(0, 1) ? 0 : result->GetInt64(0, 1);
		}
	} catch (...) {
		return;
	}

	if (table_info.yb_num_hash_key_columns > 0) {
		string pk_query = StringUtil::Format(
		    "SELECT a.attname "
		    "FROM pg_index i "
		    "JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) "
		    "WHERE i.indrelid = '%s'::regclass AND i.indisprimary "
		    "ORDER BY array_position(i.indkey, a.attnum) "
		    "LIMIT %d",
		    qualified, table_info.yb_num_hash_key_columns);
		try {
			auto result = transaction.Query(pk_query);
			if (result) {
				for (idx_t r = 0; r < result->Count(); r++) {
					table_info.yb_hash_partition_columns.push_back(result->GetString(r, 0));
				}
			}
		} catch (...) {
			table_info.yb_hash_partition_columns.clear();
			table_info.yb_num_hash_key_columns = 0;
		}
	}
}
```

- [x] **Step 4: Call YB property loader from CreateEntries**

In `CreateEntries`, at the top get the version, and call the loader:

```cpp
void PostgresTableSet::CreateEntries(PostgresTransaction &transaction, PostgresResult &result, idx_t start, idx_t end) {
	auto &pg_catalog = catalog.Cast<PostgresCatalog>();
	bool is_yugabyte = pg_catalog.GetPostgresVersion().type_v == PostgresInstanceType::YUGABYTE;

	vector<unique_ptr<PostgresTableInfo>> tables;
	unique_ptr<PostgresTableInfo> info;

	for (idx_t row = start; row < end; row++) {
		auto table_name = result.GetString(row, 1);
		if (!info || info->GetTableName() != table_name) {
			if (info) {
				tables.push_back(std::move(info));
			}
			info = make_uniq<PostgresTableInfo>(schema, table_name);
			info->approx_num_pages = result.IsNull(row, 2) ? 0 : result.GetInt64(row, 2);
		}
		AddColumnOrConstraint(&transaction, &schema, result, row, *info);
	}
	if (info) {
		tables.push_back(std::move(info));
	}
	for (auto &tbl_info : tables) {
		if (is_yugabyte) {
			LoadYugabyteTableProperties(transaction, *tbl_info, schema.name);
		}
		auto table_entry = make_shared_ptr<PostgresTableEntry>(catalog, schema, *tbl_info);
		CreateEntry(transaction, std::move(table_entry));
	}
}
```

Add the include for the catalog at the top of the file:
```cpp
#include "storage/postgres_catalog.hpp"
```

- [x] **Step 5: Store YB metadata in PostgresTableEntry constructor**

In `src/storage/postgres_table_entry.cpp`, update the PostgresTableInfo constructor:

```cpp
PostgresTableEntry::PostgresTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, PostgresTableInfo &info)
    : TableCatalogEntry(catalog, schema, *info.create_info), postgres_types(std::move(info.postgres_types)),
      postgres_names(std::move(info.postgres_names)), yb_num_tablets(info.yb_num_tablets),
      yb_num_hash_key_columns(info.yb_num_hash_key_columns),
      yb_hash_partition_columns(std::move(info.yb_hash_partition_columns)) {
	D_ASSERT(postgres_types.size() == columns.LogicalColumnCount());
	approx_num_pages.store(info.approx_num_pages, std::memory_order_release);
}
```

- [x] **Step 6: Pass YB metadata through GetScanFunction**

In `src/storage/postgres_table_entry.cpp`, in `GetScanFunction`, add before the PrepareBind call:

```cpp
	result->yb_num_tablets = yb_num_tablets;
	result->yb_num_hash_key_columns = yb_num_hash_key_columns;
	result->yb_hash_partition_columns = yb_hash_partition_columns;
	PostgresScanFunction::PrepareBind(pg_catalog.GetPostgresVersion(), context, *result,
	                                  approx_num_pages.load(std::memory_order_acquire));
```

- [x] **Step 7: Update PrepareBind to use tablet count for thread count**

In `src/postgres_scanner.cpp`, at the end of `PrepareBind`, add:

```cpp
	if (version.type_v == PostgresInstanceType::YUGABYTE && bind_data.yb_num_tablets > 0) {
		if (!bind_data.read_only || bind_data.use_text_protocol) {
			bind_data.max_threads = 1;
		} else {
			bind_data.max_threads = bind_data.yb_num_tablets;
		}
	}
```

- [x] **Step 8: Build and verify**

```bash
make -j$(nproc) -C build/release 2>&1 | tail -20
```

- [x] **Step 9: Commit**

```bash
git add src/include/storage/postgres_table_entry.hpp src/include/postgres_scanner.hpp \
        src/storage/postgres_table_set.cpp src/storage/postgres_table_entry.cpp \
        src/postgres_scanner.cpp
git commit -m "feat: use yb_table_properties for cardinality and hash column discovery

Query num_tablets, num_hash_key_columns, and PK column names from
YugabyteDB. Use tablet count for parallel thread count instead of
meaningless relpages."
```

---

## Phase B: Parallelism

### Task 6: Create YugabyteTopology Header

**Files:**
- Create: `src/include/yugabyte_topology.hpp`

- [x] **Step 1: Create the topology header**

Create `src/include/yugabyte_topology.hpp`:

```cpp
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// yugabyte_topology.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

struct YugabyteTserver {
	string host;
	int32_t port = 5433;
	string cloud;
	string region;
	string zone;
	string ip_address;
	bool reachable = false;
};

struct YugabyteTopology {
	vector<YugabyteTserver> tservers;
	bool direct_connect_available = false;

	bool HasTopology() const {
		return !tservers.empty();
	}

	idx_t ReachableCount() const {
		idx_t count = 0;
		for (auto &ts : tservers) {
			if (ts.reachable) {
				count++;
			}
		}
		return count;
	}
};

} // namespace duckdb
```

- [x] **Step 2: Commit**

```bash
git add src/include/yugabyte_topology.hpp
git commit -m "feat: add YugabyteTserver and YugabyteTopology structs"
```

### Task 7: Tserver Discovery at ATTACH Time

**Files:**
- Modify: `src/include/storage/postgres_catalog.hpp:9-10,82-114` (includes, member, accessor)
- Modify: `src/storage/postgres_catalog.cpp:1-25` (includes, constructor)

- [x] **Step 1: Add topology to PostgresCatalog header**

In `src/include/storage/postgres_catalog.hpp`:

Add after `#include "storage/postgres_connection_pool.hpp"`:
```cpp
#include "yugabyte_topology.hpp"
```

Add in the public section after `GetConnectionPoolPtr()`:
```cpp
	const YugabyteTopology &GetYugabyteTopology() const {
		return yb_topology;
	}
```

Add in the private section after `string default_schema;`:
```cpp
	YugabyteTopology yb_topology;
```

- [x] **Step 2: Add discovery function and call from constructor**

In `src/storage/postgres_catalog.cpp`, add a static helper before the constructor:

```cpp
static void DiscoverYugabyteTopology(ClientContext &context, PostgresConnection &conn,
                                     const string &connection_string, YugabyteTopology &topology) {
	auto result = conn.TryQuery(context,
	    "SELECT host, port, node_type, cloud, region, zone, public_ip FROM yb_servers()");
	if (!result) {
		return;
	}
	auto rows = result->Count();
	for (idx_t r = 0; r < rows; r++) {
		YugabyteTserver ts;
		ts.host = result->GetString(r, 0);
		ts.port = result->IsNull(r, 1) ? 5433 : static_cast<int32_t>(result->GetInt64(r, 1));
		ts.cloud = result->IsNull(r, 3) ? "" : result->GetString(r, 3);
		ts.region = result->IsNull(r, 4) ? "" : result->GetString(r, 4);
		ts.zone = result->IsNull(r, 5) ? "" : result->GetString(r, 5);
		ts.ip_address = result->IsNull(r, 6) ? ts.host : result->GetString(r, 6);
		topology.tservers.push_back(std::move(ts));
	}

	for (auto &ts : topology.tservers) {
		string probe_dsn = StringUtil::Format(
		    "host='%s' port=%d connect_timeout=2", ts.ip_address, ts.port);
		PGconn *probe = PQconnectdb(probe_dsn.c_str());
		if (probe && PQstatus(probe) == CONNECTION_OK) {
			ts.reachable = true;
			topology.direct_connect_available = true;
		}
		if (probe) {
			PQfinish(probe);
		}
	}
}
```

Add `#include <libpq-fe.h>` at top if not already present.

Update the constructor -- add after the `GetPostgresVersion` call:

```cpp
	auto connection = connection_pool->GetConnection();
	this->version = connection.GetConnection().GetPostgresVersion(context);

	if (version.type_v == PostgresInstanceType::YUGABYTE) {
		DiscoverYugabyteTopology(context, connection.GetConnection(), connection_string, yb_topology);
	}
```

- [x] **Step 3: Build and verify**

```bash
make -j$(nproc) -C build/release 2>&1 | tail -20
```

- [x] **Step 4: Commit**

```bash
git add src/include/storage/postgres_catalog.hpp src/storage/postgres_catalog.cpp
git commit -m "feat: discover YugabyteDB tserver topology at ATTACH time

Query yb_servers() to get all tservers with host/port/region/zone.
Probe each tserver for direct connectivity (2s timeout).
Cache topology for the ATTACH lifetime."
```

### Task 8: Hash-Code Parallel Scanning

**Files:**
- Modify: `src/postgres_scanner.cpp` (PostgresGlobalState, PostgresParallelStateNext, PostgresInitInternal, PostgresScanConnect, PostgresScanProgress, GetLocalState, PostgresInitGlobalState)

- [x] **Step 1: Add YB fields to PostgresGlobalState**

In `src/postgres_scanner.cpp`, add to `PostgresGlobalState` after `string snapshot;`:

```cpp
	string snapshot;

	idx_t yb_hash_idx = 0;
	idx_t yb_num_tasks = 0;
```

- [x] **Step 2: Add YugabyteDB hash-code WHERE in PostgresInitInternal**

In `PostgresInitInternal`, replace the filter construction block (around lines 268-283):

```cpp
	string filter;

	lstate.exec = false;
	lstate.done = false;
	if (bind_data->pages_approx > 0) {
		filter = StringUtil::Format("WHERE ctid BETWEEN '(%d,0)'::tid AND '(%d,0)'::tid", task_min, task_max);
	} else if (bind_data->version.type_v == PostgresInstanceType::YUGABYTE &&
	           bind_data->yb_num_hash_key_columns > 0 && !bind_data->yb_hash_partition_columns.empty()) {
		string hash_cols;
		for (idx_t i = 0; i < bind_data->yb_hash_partition_columns.size(); i++) {
			if (i > 0) {
				hash_cols += ", ";
			}
			hash_cols += KeywordHelper::WriteQuoted(bind_data->yb_hash_partition_columns[i], '"');
		}
		filter = StringUtil::Format("WHERE yb_hash_code(%s) BETWEEN %d AND %d",
		                            hash_cols, task_min, task_max);
	}
```

- [x] **Step 3: Add YugabyteDB path to PostgresParallelStateNext**

Replace `PostgresParallelStateNext`:

```cpp
static bool PostgresParallelStateNext(ClientContext &context, const FunctionData *bind_data_p,
                                      PostgresLocalState &lstate, PostgresGlobalState &gstate) {
	D_ASSERT(bind_data_p);
	auto bind_data = (const PostgresBindData *)bind_data_p;

	lock_guard<mutex> parallel_lock(gstate.lock);
	lstate.batch_idx = gstate.batch_idx++;

	if (bind_data->version.type_v == PostgresInstanceType::YUGABYTE &&
	    bind_data->yb_num_hash_key_columns > 0 && gstate.yb_num_tasks > 0) {
		if (gstate.yb_hash_idx >= gstate.yb_num_tasks) {
			lstate.done = true;
			return false;
		}
		idx_t range_size = 65536 / gstate.yb_num_tasks;
		idx_t range_min = gstate.yb_hash_idx * range_size;
		idx_t range_max = (gstate.yb_hash_idx == gstate.yb_num_tasks - 1)
		                      ? 65535
		                      : range_min + range_size - 1;
		gstate.yb_hash_idx++;
		PostgresInitInternal(context, bind_data, lstate, range_min, range_max);
		return true;
	}

	if (gstate.page_idx < bind_data->pages_approx) {
		auto page_max = gstate.page_idx + bind_data->pages_per_task;
		if (page_max >= bind_data->pages_approx || page_max > POSTGRES_TID_MAX) {
			page_max = POSTGRES_TID_MAX;
		}
		PostgresInitInternal(context, bind_data, lstate, gstate.page_idx, page_max);
		gstate.page_idx = page_max;
		return true;
	}
	lstate.done = true;
	return false;
}
```

- [x] **Step 4: Initialize yb_num_tasks in PostgresInitGlobalState**

In `PostgresInitGlobalState`, add after the `PostgresGetSnapshot` call (around line 376):

```cpp
		PostgresGetSnapshot(context, bind_data.version, bind_data, *result);
	}

	if (bind_data.version.type_v == PostgresInstanceType::YUGABYTE &&
	    bind_data.yb_num_hash_key_columns > 0 && bind_data.max_threads > 1) {
		result->yb_num_tasks = bind_data.max_threads;
	}

	return std::move(result);
```

- [x] **Step 5: Handle YB path in GetLocalState**

In `GetLocalState`, replace the condition block (lines 452-458):

```cpp
	if ((bind_data.pages_approx == 0 && gstate.yb_num_tasks == 0) || bind_data.requires_materialization) {
		PostgresInitInternal(context, &bind_data, *local_state, 0, POSTGRES_TID_MAX);
		lock_guard<mutex> parallel_lock(gstate.lock);
		gstate.page_idx = POSTGRES_TID_MAX;
	} else if (!PostgresParallelStateNext(context, input.bind_data.get(), *local_state, gstate)) {
		local_state->done = true;
	}
```

- [x] **Step 6: Force REPEATABLE READ for YugabyteDB parallel scans**

Update `PostgresScanConnect` to accept instance type:

```cpp
static void PostgresScanConnect(ClientContext &context, PostgresConnection &conn, const string &snapshot,
                                AccessMode access_mode, PostgresIsolationLevel isolation_level,
                                PostgresInstanceType instance_type = PostgresInstanceType::POSTGRES) {
	if (instance_type == PostgresInstanceType::YUGABYTE) {
		conn.Execute(context, PostgresTransaction::GetBeginTransactionQuery(
		    PostgresIsolationLevel::REPEATABLE_READ, AccessMode::READ_ONLY));
	} else {
		conn.Execute(context, PostgresTransaction::GetBeginTransactionQuery(isolation_level, access_mode));
		if (!snapshot.empty()) {
			D_ASSERT(isolation_level != PostgresIsolationLevel::READ_COMMITTED);
			conn.Query(context, StringUtil::Format("SET TRANSACTION SNAPSHOT '%s'", snapshot));
		}
	}
	Value statement_timeout;
	if (context.TryGetCurrentSetting("pg_statement_timeout_millis", statement_timeout) &&
	    !statement_timeout.IsNull()) {
		conn.Execute(context, StringUtil::Format("SET statement_timeout=%u",
		                                         UIntegerValue::Get(statement_timeout)));
	}
	Value idle_timeout;
	if (context.TryGetCurrentSetting("pg_idle_in_transaction_timeout_millis", idle_timeout) &&
	    !idle_timeout.IsNull()) {
		conn.Execute(context, StringUtil::Format("SET idle_in_transaction_session_timeout=%u",
		                                         UIntegerValue::Get(idle_timeout)));
	}
}
```

Update both call sites in `TryOpenNewConnection` (around lines 427 and 430) to pass the instance type:

```cpp
		PostgresScanConnect(context, lstate.connection, snapshot, pg_catalog->access_mode,
		                    pg_catalog->isolation_level, bind_data.version.type_v);
```

and:

```cpp
		PostgresScanConnect(context, lstate.connection, snapshot, AccessMode::READ_ONLY,
		                    PostgresIsolationLevel::REPEATABLE_READ, bind_data.version.type_v);
```

- [x] **Step 7: Update progress reporting for YugabyteDB**

Replace `PostgresScanProgress`:

```cpp
double PostgresScanProgress(ClientContext &context, const FunctionData *bind_data_p,
                            const GlobalTableFunctionState *global_state) {
	auto &bind_data = bind_data_p->Cast<PostgresBindData>();
	auto &gstate = global_state->Cast<PostgresGlobalState>();

	lock_guard<mutex> parallel_lock(gstate.lock);
	if (gstate.yb_num_tasks > 0) {
		return MinValue<double>(100, 100.0 * double(gstate.yb_hash_idx) / double(gstate.yb_num_tasks));
	}
	double progress = 100 * double(gstate.page_idx) / double(bind_data.pages_approx);
	return MinValue<double>(100, progress);
}
```

- [x] **Step 8: Build and verify**

```bash
make -j$(nproc) -C build/release 2>&1 | tail -20
```

- [x] **Step 9: Commit**

```bash
git add src/postgres_scanner.cpp
git commit -m "feat: hash-code parallel scanning for YugabyteDB

Split YugabyteDB hash space (0-65535) into N ranges and push
yb_hash_code(pk_cols) BETWEEN X AND Y as parallel scan tasks.
Force REPEATABLE READ on all parallel connections for consistent
reads via HLC-based MVCC."
```

### Task 9: Tserver-Targeted Connection Routing

**Files:**
- Modify: `src/include/storage/postgres_connection_pool.hpp` (add method declaration)
- Modify: `src/storage/postgres_connection_pool.cpp` (add tserver connection method)
- Modify: `src/postgres_scanner.cpp` (TryOpenNewConnection -- route to tservers)

- [x] **Step 1: Add tserver connection method to pool**

In `src/include/storage/postgres_connection_pool.hpp`, add to the public section:

```cpp
	std::unique_ptr<PostgresConnection> CreateConnectionToHost(const string &host, int32_t port);
```

In `src/storage/postgres_connection_pool.cpp`, add the implementation:

```cpp
std::unique_ptr<PostgresConnection> PostgresConnectionPool::CreateConnectionToHost(const string &host, int32_t port) {
	string tserver_dsn = postgres_catalog.connection_string;
	tserver_dsn += StringUtil::Format(" host='%s' port=%d", host, port);
	auto conn = PostgresConnection::Open(tserver_dsn, postgres_catalog.attach_path);
	return make_uniq<PostgresConnection>(std::move(conn));
}
```

- [x] **Step 2: Route parallel connections to tservers**

In `src/postgres_scanner.cpp`, in `TryOpenNewConnection`, add YugabyteDB tserver routing. Replace the section after `used_main_thread` (the `if (pg_catalog)` block starting around line 422):

```cpp
	if (pg_catalog) {
		// Try direct tserver connection for YugabyteDB
		if (pg_catalog->GetPostgresVersion().type_v == PostgresInstanceType::YUGABYTE &&
		    pg_catalog->GetYugabyteTopology().direct_connect_available) {
			auto &topology = pg_catalog->GetYugabyteTopology();
			lock_guard<mutex> parallel_lock(lock);
			idx_t ts_idx = batch_idx % topology.tservers.size();
			for (idx_t i = 0; i < topology.tservers.size(); i++) {
				auto &ts = topology.tservers[(ts_idx + i) % topology.tservers.size()];
				if (ts.reachable) {
					try {
						auto conn = pg_catalog->GetConnectionPool().CreateConnectionToHost(
						    ts.ip_address, ts.port);
						lstate.connection = std::move(*conn);
						PostgresScanConnect(context, lstate.connection, snapshot,
						                    pg_catalog->access_mode, pg_catalog->isolation_level,
						                    bind_data.version.type_v);
						return true;
					} catch (...) {
						break;
					}
				}
			}
		}

		if (!pg_catalog->GetConnectionPool().TryGetConnection(lstate.pool_connection)) {
			return false;
		}
		lstate.connection = PostgresConnection(lstate.pool_connection.GetConnection().GetConnection());
		PostgresScanConnect(context, lstate.connection, snapshot, pg_catalog->access_mode,
		                    pg_catalog->isolation_level, bind_data.version.type_v);
	} else {
		lstate.connection = PostgresConnection::Open(bind_data.dsn, bind_data.attach_path);
		PostgresScanConnect(context, lstate.connection, snapshot, AccessMode::READ_ONLY,
		                    PostgresIsolationLevel::REPEATABLE_READ, bind_data.version.type_v);
	}
	return true;
```

- [x] **Step 3: Build and verify**

```bash
make -j$(nproc) -C build/release 2>&1 | tail -20
```

- [x] **Step 4: Commit**

```bash
git add src/include/storage/postgres_connection_pool.hpp src/storage/postgres_connection_pool.cpp \
        src/postgres_scanner.cpp
git commit -m "feat: route parallel scan connections to YugabyteDB tservers

When tservers are directly reachable, route parallel scan
connections round-robin to individual tservers for data locality.
Falls back to connection pool when tservers are not reachable."
```

---

## Phase C: COPY Optimization

### Task 10: Register YugabyteDB Settings and COPY Helpers

**Files:**
- Modify: `src/postgres_extension.cpp:301-306` (after pool settings)
- Modify: `src/postgres_copy_to.cpp:26-70` (BeginCopyTo)
- Modify: `src/include/postgres_connection.hpp:64-77` (add CommitAndRestartCopy)

- [x] **Step 1: Register new YugabyteDB settings**

In `src/postgres_extension.cpp`, add after the `pg_pool_health_check_query` block (around line 306):

```cpp
	// YugabyteDB-specific options
	config.AddExtensionOption("pg_yb_rows_per_transaction",
	                          "Number of rows per transaction batch for COPY FROM on YugabyteDB (0 to disable)",
	                          LogicalType::UBIGINT, Value::UBIGINT(10000));
	config.AddExtensionOption("pg_yb_disable_transactional_writes",
	                          "Disable transactional writes for bulk COPY FROM on YugabyteDB (no rollback on failure)",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(false));
```

- [x] **Step 2: Add bulk load GUC push in BeginCopyTo**

In `src/postgres_copy_to.cpp`, at the start of `BeginCopyTo` (line 26), add:

```cpp
void PostgresConnection::BeginCopyTo(ClientContext &context, PostgresCopyState &state, PostgresCopyFormat format,
                                     const string &schema_name, const string &table_name,
                                     const vector<string> &column_names) {
	Value yb_disable_txn_writes;
	if (context.TryGetCurrentSetting("pg_yb_disable_transactional_writes", yb_disable_txn_writes) &&
	    !yb_disable_txn_writes.IsNull() && BooleanValue::Get(yb_disable_txn_writes)) {
		Execute(context, "SET yb_disable_transactional_writes = true");
	}

	string query = "COPY ";
	// ... rest of existing method unchanged ...
```

- [x] **Step 3: Add CommitAndRestartCopy helper**

In `src/include/postgres_connection.hpp`, add to `PostgresConnection` public section:

```cpp
	void CommitAndRestartCopy(ClientContext &context, PostgresCopyState &state, PostgresCopyFormat format,
	                          const string &schema_name, const string &table_name,
	                          const vector<string> &column_names);
```

In `src/postgres_copy_to.cpp`, add the implementation:

```cpp
void PostgresConnection::CommitAndRestartCopy(ClientContext &context, PostgresCopyState &state,
                                              PostgresCopyFormat format, const string &schema_name,
                                              const string &table_name, const vector<string> &column_names) {
	FinishCopyTo(state);
	Execute(context, "COMMIT");
	Execute(context, "BEGIN");
	BeginCopyTo(context, state, format, schema_name, table_name, column_names);
}
```

- [x] **Step 4: Build and verify**

```bash
make -j$(nproc) -C build/release 2>&1 | tail -20
```

- [x] **Step 5: Commit**

```bash
git add src/postgres_extension.cpp src/postgres_copy_to.cpp src/include/postgres_connection.hpp
git commit -m "feat: add YugabyteDB COPY optimization settings and batch support

Register pg_yb_rows_per_transaction and pg_yb_disable_transactional_writes.
Push yb_disable_transactional_writes GUC for bulk loads.
Add CommitAndRestartCopy helper for batched transaction COPY FROM."
```

---

## Verification

### Task 11: Final Build and Consistency Check

**Files:** None (verification only)

- [x] **Step 1: Clean build**

```bash
cd /home/wdroste/build/duckdb-postgres && make clean && make -j$(nproc) -C build/release 2>&1 | tail -30
```
Expected: Clean build, no errors.

- [x] **Step 2: Verify all YugabyteDB instance type checks**

```bash
grep -rn "PostgresInstanceType::YUGABYTE" src/ --include="*.cpp" --include="*.hpp"
```
Expected: Consistent checks across all modified files.

- [x] **Step 3: Verify DISCARD ALL is properly gated**

```bash
grep -rn "DISCARD ALL" src/ --include="*.cpp"
```
Expected: Only in the `else` branch of Reset.

- [x] **Step 4: Review commit log**

```bash
git log --oneline feat/secret-options ^main | head -20
```
