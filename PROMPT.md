# YugabyteDB Integration - Autonomous Execution Prompt

You are implementing YugabyteDB support for the duckdb-postgres extension. Execute the plan at `docs/superpowers/plans/2026-04-27-yugabyte-integration.md` one task at a time.

## How This Works — EVERY Loop Iteration

Each time you are invoked, follow this sequence exactly:

### Step 0: Verify the Previous Task (if any are marked done)

Before touching ANY new work, check the last completed task:

1. Read the plan file. Find the most recently checked-off task (last `- [x]` section).
2. If there IS a completed task, run the **Retroactive Verification** on it (see below).
3. If retroactive verification FAILS, fix the previous task first. Do NOT start new work.
4. If retroactive verification PASSES (or there are no completed tasks yet), proceed to Step 1.

### Step 1: Execute the Next Task

1. Find the first unchecked (`- [ ]`) step in the plan
2. Execute that step exactly as written
3. Mark it `- [x]` when done
4. If the step says "Build and verify", actually run the build and fix any errors before marking done
5. If the step says "Commit", make the commit
6. After completing all steps in the current `### Task N:` section, run the **Completion Verification Gate**
7. Stop after the verification gate passes

---

## Retroactive Verification (Step 0)

This runs at the START of every loop iteration to catch the previous invocation's mistakes. You are a fresh context — you don't trust the previous you. Verify the work actually landed.

### R1. Git State Check
```bash
git log --oneline -5
git diff HEAD --stat
```
Confirm: the last commit message matches the task that's marked done. No uncommitted changes lingering.

### R2. Code Existence Check
For the last completed task, grep for the KEY symbols/changes it should have introduced:
- Task 1: `grep -n "YUGABYTE" src/include/postgres_version.hpp` and `grep -n "yb_version" src/include/postgres_version.hpp` and `grep -n "\-YB\-" src/postgres_connection.cpp`
- Task 2: `grep -n "YUGABYTE" src/postgres_scanner.cpp | grep -i ctid`
- Task 3: `grep -n "YUGABYTE" src/postgres_scanner.cpp | grep -i snapshot`
- Task 4: `grep -n "YUGABYTE" src/postgres_connection.cpp | grep -i reset` and `grep -n "instance_type" src/include/postgres_connection.hpp`
- Task 5: `grep -n "yb_num_tablets" src/include/postgres_scanner.hpp` and `grep -n "yb_table_properties" src/storage/postgres_table_set.cpp`
- Task 6: `test -f src/include/yugabyte_topology.hpp && echo EXISTS || echo MISSING`
- Task 7: `grep -n "yb_servers" src/storage/postgres_catalog.cpp` and `grep -n "YugabyteTopology" src/include/storage/postgres_catalog.hpp`
- Task 8: `grep -n "yb_hash_code" src/postgres_scanner.cpp` and `grep -n "yb_hash_idx" src/postgres_scanner.cpp`
- Task 9: `grep -n "CreateConnectionToHost" src/storage/postgres_connection_pool.cpp`
- Task 10: `grep -n "pg_yb_rows_per_transaction" src/postgres_extension.cpp` and `grep -n "CommitAndRestartCopy" src/postgres_copy_to.cpp`

If the expected symbols are MISSING, the task was not actually completed. Uncheck it in the plan, fix it, and re-verify.

### R3. Build Check
```bash
make -j$(nproc) -C build/release 2>&1 | tail -40
```
If the build is broken, the previous task broke it. Fix it before doing anything else.

### R4. Functional Smoke Test

Don't just prove the code exists — prove it WORKS. Load the built extension and exercise the code path your task touched. This is not optional.

**How to run a smoke test:**
```bash
./build/release/duckdb -unsigned <<'SQL'
LOAD 'build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
-- Task-specific test SQL goes here (see below)
SQL
```

**Connection string:** The YugabyteDB connection string is stored in the environment variable `$YB_CONN`. Use it for all ATTACH commands. If `$YB_CONN` is not set, stop and tell the user to set it before continuing. Do NOT fall back to vanilla Postgres — these tests must hit YugabyteDB.

Example: `ATTACH '$YB_CONN' AS yb (TYPE postgres);`

**Task-specific smoke tests:**

- **Task 1 (Detection):** Attach to YugabyteDB and verify the version string contains `-YB-`. Confirm it's detected as YUGABYTE, not POSTGRES:
  ```sql
  ATTACH '$YB_CONN' AS yb (TYPE postgres);
  SET pg_debug_show_queries=true;
  SELECT * FROM yb.information_schema.tables LIMIT 1;
  -- Check stdout for the version query. It should show "-YB-" in the version string.
  -- If detection is broken, the extension will try CTID scans or pg_export_snapshot and may error.
  ```

- **Task 2 (CTID disabled):** Scan a table on YugabyteDB. With CTID disabled, this should succeed as a single-threaded scan. If CTID logic is still active, it will try page-range queries and produce errors or wrong results:
  ```sql
  ATTACH '$YB_CONN' AS yb (TYPE postgres);
  SELECT count(*) FROM yb.pg_catalog.pg_class;
  SELECT * FROM yb.pg_catalog.pg_tables LIMIT 10;
  ```

- **Task 3 (Snapshot skip):** Run a scan that would trigger pg_export_snapshot on vanilla Postgres. On YugabyteDB this should be skipped. If it's NOT skipped, the query may fail:
  ```sql
  ATTACH '$YB_CONN' AS yb (TYPE postgres);
  SELECT count(*) FROM yb.pg_catalog.pg_class;
  -- Success = snapshot skip is working. Failure/error about pg_export_snapshot = broken.
  ```

- **Task 4 (DISCARD ALL replacement):** Attach, query, detach, re-attach, query again. This cycles the connection pool and exercises the Reset path. On YugabyteDB, DISCARD ALL would clobber session state — the replacement should preserve it:
  ```sql
  ATTACH '$YB_CONN' AS yb (TYPE postgres);
  SELECT count(*) FROM yb.pg_catalog.pg_tables;
  DETACH yb;
  ATTACH '$YB_CONN' AS yb (TYPE postgres);
  SELECT count(*) FROM yb.pg_catalog.pg_tables;
  -- Both counts should match. Errors on re-attach = Reset is broken.
  ```

- **Task 5 (Cardinality):** Run EXPLAIN to see the cardinality estimate. On YugabyteDB, this should use tablet count, not relpages:
  ```sql
  ATTACH '$YB_CONN' AS yb (TYPE postgres);
  EXPLAIN SELECT * FROM yb.pg_catalog.pg_class;
  -- Should show a cardinality estimate. If yb_table_properties fails, it may show 0 rows or crash.
  ```

- **Task 6 (Topology header):** No runtime behavior — just verify the header compiles (covered by build check).

- **Task 7 (Tserver discovery):** Attach to YugabyteDB and verify topology was discovered. Enable debug queries to see the yb_servers() call:
  ```sql
  SET pg_debug_show_queries=true;
  ATTACH '$YB_CONN' AS yb (TYPE postgres);
  -- Check stdout for "yb_servers" query. Should show tserver hosts being queried.
  SELECT * FROM yb.pg_catalog.pg_tables LIMIT 5;
  ```

- **Task 8 (Hash-code parallelism):** Scan a hash-sharded table on YugabyteDB. With debug queries on, you should see `yb_hash_code(...) BETWEEN` in the generated SQL:
  ```sql
  SET pg_debug_show_queries=true;
  ATTACH '$YB_CONN' AS yb (TYPE postgres);
  SELECT count(*) FROM yb.pg_catalog.pg_class;
  -- Look for yb_hash_code in the debug output. If present, hash-code parallelism is active.
  -- If you see ctid BETWEEN instead, something is wrong.
  ```

- **Task 9 (Tserver routing):** Same as Task 8 but look at connection patterns. With debug queries on, multiple COPY queries should appear (one per hash range). If direct tserver connections work, you'll see connections to different hosts:
  ```sql
  SET pg_debug_show_queries=true;
  ATTACH '$YB_CONN' AS yb (TYPE postgres);
  SELECT count(*) FROM yb.pg_catalog.pg_class;
  -- Multiple parallel COPY queries = routing is working.
  ```

- **Task 10 (COPY settings):** Verify the new settings are registered and can be SET:
  ```sql
  LOAD 'build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
  SET pg_yb_rows_per_transaction=5000;
  SET pg_yb_disable_transactional_writes=true;
  -- No error = settings registered correctly.
  ```
  Then test bulk load GUC push with a real connection:
  ```sql
  SET pg_yb_disable_transactional_writes=true;
  SET pg_debug_show_queries=true;
  ATTACH '$YB_CONN' AS yb (TYPE postgres);
  -- On next COPY TO (insert), look for "SET yb_disable_transactional_writes = true" in debug output.
  ```

**If `$YB_CONN` is not set**, STOP. Print this message and exit:
```
ERROR: $YB_CONN is not set. Set it to a YugabyteDB connection string before running.
Example: export YB_CONN="host=yb-tserver-0 port=5433 dbname=yugabyte user=yugabyte"
```
Do NOT substitute a vanilla Postgres connection. The point is to test against YugabyteDB.

**If the smoke test crashes or errors**, the task is not done. Debug it, fix it, re-commit.

### R5. Retroactive Verdict
```
RETROACTIVE CHECK (Task N):
  Git state: PASS/FAIL
  Code exists: PASS/FAIL [list any missing symbols]
  Build: PASS/FAIL
  Smoke test: PASS/FAIL [what you tested, what happened]
  VERDICT: PASS/FAIL — [proceed to next task / fix previous task]
```

---

## Completion Verification Gate (Step 1, end of task)

After you complete every step in a `### Task N:` section, run this BEFORE stopping. Do not skip it. Do not just say "looks good". Actually do each check.

### V1. Build Check
Run the full build. If it fails, you are not done. Fix it.
```bash
make -j$(nproc) -C build/release 2>&1 | tail -40
```

### V2. Diff Audit
Run `git diff HEAD~1 --stat` and `git diff HEAD~1` to see exactly what your commit changed. Read the diff. For every file in the diff, verify:
- The change matches what the plan step asked for
- You didn't leave debug code, TODO comments, or half-finished edits
- The change compiles in context (not just syntactically correct but semantically correct — right types, right includes, right namespaces)

### V3. Grep Verification
For each new symbol you introduced (enum value, struct field, function, variable), grep the codebase to confirm:
- It is actually referenced where the plan says it should be
- Spelling is consistent everywhere (e.g., `yb_num_tablets` not `yb_num_tablet` in one place and `yb_num_tablets` in another)
- No orphaned declarations (declared in header but never defined, or defined but never called)

### V4. Integration Check
Read the files your task modified and the files that CONSUME what you changed. Verify the interface actually connects:
- If you added a field to a struct, check that the struct's constructors initialize it
- If you added a method to a class, check that callers exist or will exist in a later task
- If you changed a function signature, check that all call sites were updated

### V5. Functional Smoke Test
Run the same smoke test from R4 above for the task you just completed. Prove the code works, not just that it compiles.

### V6. Verdict
After running checks V1-V5, write a short verdict:
```
TASK N COMPLETION VERIFICATION:
  Build: PASS/FAIL
  Diff audit: PASS/FAIL [note any issues]
  Grep check: PASS/FAIL [note any orphans or typos]
  Integration: PASS/FAIL [note any broken interfaces]
  Smoke test: PASS/FAIL [what you tested, what happened]
  VERDICT: PASS/FAIL
```

If ANY check is FAIL, fix the issue, amend the commit, and re-run the verification gate. Do not stop with a FAIL.

---

## Project Context

- **Repo:** duckdb-postgres — a DuckDB extension that connects to PostgreSQL via libpq
- **Branch:** `feat/secret-options`
- **Build:** `make -j$(nproc) -C build/release`
- **Language:** C++
- **Pattern:** Instance-specific behavior is gated on `PostgresInstanceType` enum (see Aurora/Redshift examples in `postgres_scanner.cpp` and `postgres_connection.cpp`)

## Key Files You'll Touch

- `src/include/postgres_version.hpp` — enum + version struct
- `src/include/postgres_connection.hpp` — connection wrapper with OwnedPostgresConnection
- `src/include/postgres_scanner.hpp` — PostgresBindData for scans
- `src/include/storage/postgres_catalog.hpp` — PostgresCatalog with version + pool
- `src/include/storage/postgres_table_entry.hpp` — PostgresTableInfo and PostgresTableEntry
- `src/postgres_connection.cpp` — version detection (GetPostgresVersion), connection reset (Reset)
- `src/postgres_scanner.cpp` — parallel scanning (PrepareBind, PostgresGetSnapshot, PostgresParallelStateNext, PostgresInitInternal, PostgresScanConnect, PostgresScanProgress)
- `src/storage/postgres_catalog.cpp` — ATTACH-time init (constructor)
- `src/storage/postgres_connection_pool.cpp` — connection creation (CreateNewConnection)
- `src/storage/postgres_table_set.cpp` — table metadata loading (CreateEntries, GetTableInfo)
- `src/storage/postgres_table_entry.cpp` — scan function binding (GetScanFunction)
- `src/postgres_copy_to.cpp` — COPY write path (BeginCopyTo, CopyChunk)
- `src/postgres_extension.cpp` — settings registration (LoadInternal)

## Rules

- **Read before edit.** Always read a file before modifying it. Read the CURRENT state, not what you think is there from the plan.
- **Build after each task.** Run the build and fix compile errors before committing.
- **One task per invocation.** Complete the current task, pass both verification gates, then stop.
- **Follow the plan literally.** Don't improvise, don't add features, don't refactor adjacent code.
- **If the build fails**, read the actual compiler error. Read the actual file at the line it points to. Fix the real problem. Don't guess.
- **Do not skip steps.** If a step says to build, build. If it says to commit, commit.
- **Do not skip verification.** Checking a box is not the same as doing the work. Grep for proof. Run the smoke test.
- **Trust nothing from a previous invocation.** You are a fresh context. Verify before you build on top.
- **Report what you did** at the end: which task, what files changed, build status, both verification verdicts.
