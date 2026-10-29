# DuckDB Postgres extension

The Postgres extension allows DuckDB to directly read and write data from a running Postgres database instance. The data can be queried directly from the underlying Postgres database. Data can be loaded from Postgres tables into DuckDB tables, or vice versa.

## Reading Data from Postgres

To make a Postgres database accessible to DuckDB use the `ATTACH` command:

```sql
ATTACH 'dbname=postgresscanner' AS postgres_db (TYPE postgres);
```

The `ATTACH` command takes as input a [`libpq` connection string](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING) - which is a set of `key=value` pairs separated by spaces. Below are some example connection strings and commonly used parameters. A full list of available parameters can be found [in the Postgres documentation](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS).

```
dbname=postgresscanner
host=localhost port=5432 dbname=mydb connect_timeout=10
```

|   Name   |             Description              |    Default     |
|----------|--------------------------------------|----------------|
| host     | Name of host to connect to           | localhost      |
| hostaddr | Host IP address                      | localhost      |
| port     | Port Number                          | 5432           |
| user     | Postgres User Name                   | [OS user name] |
| password | Postgres Password                    |                |
| dbname   | Database Name                        | [user]         |
| passfile | Name of file passwords are stored in | ~/.pgpass      |


The tables in the file can be read as if they were normal DuckDB tables, but the underlying data is read directly from Postgres at query time.

```sql
D SHOW ALL TABLES;
┌───────────────────────────────────────┐
│                 name                  │
│                varchar                │
├───────────────────────────────────────┤
│ uuids                                 │
└───────────────────────────────────────┘
D SELECT * FROM postgres_db.uuids;
┌──────────────────────────────────────┐
│                  u                   │
│                 uuid                 │
├──────────────────────────────────────┤
│ 6d3d2541-710b-4bde-b3af-4711738636bf │
│ NULL                                 │
│ 00000000-0000-0000-0000-000000000001 │
│ ffffffff-ffff-ffff-ffff-ffffffffffff │
└──────────────────────────────────────┘
```

For more information on how to use the connector, refer to the [Postgres documentation on the website](https://duckdb.org/docs/extensions/postgres).

### Remote Pushdown

Queries and statements that only reference tables of a single attached Postgres database are executed in Postgres as a whole (instead of scanning the tables and executing the query in DuckDB), when every construct they use can be translated to Postgres SQL with the same semantics. This covers e.g. joins, aggregates, window functions, CTEs, set operations, subqueries, many scalar functions, `UPDATE` / `DELETE` / `INSERT` (including `RETURNING` and `ON CONFLICT`), `CREATE TABLE ... AS`, `CREATE VIEW` and most `DROP` / `ALTER TABLE` statements. `EXPLAIN` shows the generated SQL (`Postgres Query`), and `SET pg_debug_show_queries = true` also prints why a query is not pushed down.

```sql
D EXPLAIN SELECT customer, sum(amount) FROM postgres_db.orders WHERE status = 'open' GROUP BY customer;
╭─ Postgres Query ───────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ SQL:                                                                                                                   │
│ SELECT "customer", sum("amount") AS "sum(amount)" FROM "public"."orders" WHERE ("status" = 'open') GROUP BY "customer" │
│ Projections: c0, c1                                                                                                    │
╰────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
```

A pushed down query is executed as a single Postgres query over a single connection. Queries that are plain scans of a single table are instead executed through the regular table scan, which reads the table in parallel over multiple connections (by ranges of the `ctid`). A query is a plain scan when it reads a single table, selects only columns (or `*`), filters only with comparisons of columns with constants, `IN` lists and `IS [NOT] NULL` combined with `AND` / `OR`, and has no `ORDER BY` or `OFFSET` (a `LIMIT` is fine). For example, this query is still executed with a parallel scan:

```sql
SELECT id, customer, amount FROM postgres_db.orders WHERE status = 'open' AND amount > 100 LIMIT 10;
```

Any other construct makes the query a candidate for pushdown, including seemingly simple ones such as a computed column, a `LIKE` filter or an `ORDER BY`. These queries are executed by Postgres over a single connection, which can be slower than a parallel scan for queries that read most of a large table:

```sql
-- pushed down: the computed column makes it more than a plain scan
SELECT id, amount * 1.2 AS amount_with_tax FROM postgres_db.orders WHERE status = 'open';
-- pushed down as well
SELECT * FROM postgres_db.orders WHERE customer LIKE 'c1%';
SELECT * FROM postgres_db.orders ORDER BY id;
```

Plain scans are pushed down as well when the settings at attach time do not allow parallel scans (`pg_use_ctid_scan = false`, the text protocol through `pg_query_mode`, `pg_use_text_protocol` or the `USE_TEXT_PROTOCOL` attach option, or Postgres versions before 14).

Remote pushdown is enabled by default. It can be disabled for newly attached databases:

```sql
SET pg_enable_remote_pushdown = false;
```

Known differences to executing the query in DuckDB:

* Floating point results of statistical aggregates (e.g. `corr`, `regr_*`, `quantile_cont`) can differ in the last digits.
* Postgres raises errors for floating point overflows, where DuckDB produces `inf`.
* `HUGEINT` results are transferred as `NUMERIC(38, 0)` - values of 10^38 or more raise an error.
* Duplicate result column names are made unique (e.g. `i` and `i_1`), like for the result of a subquery.
* `CURRENT_DATE` / `CURRENT_TIMESTAMP` (without parentheses) are evaluated by Postgres, at the start of the Postgres transaction.
* DuckDB orders strings byte-wise. When the database collation orders them differently (e.g. `en_US.UTF-8`), string ordering (`ORDER BY`, `<`, `min` / `max`, ...) is pushed down with the `C` collation (`COLLATE "C"`), which cannot use indexes built with the database collation. `SELECT DISTINCT` / set operations / `DISTINCT` aggregates ordered by strings are then executed locally, and `CREATE TABLE ... AS` / `CREATE VIEW` statements that order strings are not pushed down as a whole. Column-level collations are not taken into account.
* `lower` / `ILIKE` are only pushed down when Postgres maps Unicode characters to lower case.
* Casts of string columns to dates, timestamps or booleans use the Postgres parsing rules, which accept some inputs that DuckDB rejects (and vice versa).
* Unqualified tables are only pushed down when the Postgres database has no schema named `main` (or when it is attached with the `SCHEMA` option).

### Connection Pooling

By default, the extension opens a new connection whenever none are idle. Behind a transaction-mode pooler (e.g. PgBouncer `pool_mode=transaction`), this can exceed the pooler's own pool size under load and hang. If that applies to you, cap and wait instead:

```sql
SET pg_pool_acquire_mode = 'wait';
SET pg_pool_max_connections = 16;        -- ~80-90% of your pooler's pool size

ATTACH 'host=pgbouncer-host port=6432 dbname=mydb' AS postgres_db (TYPE postgres);
```

Note that `SET` statements are global and only apply pre-`ATTACH`. To change pooling on an already-attached database, use `postgres_configure_pool(catalog_name=...)` instead. Leave `force` mode if you connect directly to Postgres or through a session-mode pooler.

### AWS RDS IAM Authentication

The extension supports AWS RDS IAM-based authentication, which allows you to connect to RDS/Aurora PostgreSQL instances using IAM database authentication instead of static passwords. This feature automatically generates temporary authentication tokens using the AWS SDK.

#### Requirements

- RDS instance with IAM database authentication enabled
- IAM user/role with `rds-db:connect` permission for the RDS instance
- AWS credentials configured (via `AWS_PROFILE`, `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`, or IAM role)

#### Usage

To use RDS IAM authentication, create a Postgres secret with the `AWS_RDS_IAM_AUTH_ENABLED` parameter set to `TRUE`:

```sql
CREATE SECRET rds_secret (
    TYPE POSTGRES,
    HOST 'my-db-instance.xxxxxx.us-west-2.rds.amazonaws.com',
    PORT '5432',
    USER 'my_iam_user',
    DATABASE 'postgres',
    SSLMODE 'require',
    AWS_RDS_IAM_AUTH_ENABLED TRUE,
    AWS_REGION 'us-west-2'
);

ATTACH '' AS rds_db (TYPE POSTGRES, SECRET rds_secret);
```

#### Secret Parameters for RDS IAM Authentication

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `HOST` | VARCHAR | Yes | RDS/Aurora instance hostname |
| `PORT` | VARCHAR | Yes | RDS/Aurora instance port (typically 5432) |
| `USER` | VARCHAR | Yes | IAM database username |
| `AWS_RDS_IAM_AUTH_ENABLED` | BOOLEAN | Yes | Enable RDS IAM authentication |
| `AWS_RDS_IAM_TOKEN_EXPIRATION_SECONDS` | BIGINT | No | Token expiration time in seconds, default: 900 (15 min) |
| `AWS_REGION` | VARCHAR | Yes | AWS region |

#### Important Notes

- **Token Expiration**: RDS auth tokens expire after 15 minutes max. The extension caches tokens for `AWS_RDS_IAM_TOKEN_EXPIRATION_SECONDS - 60` seconds, so new pool connections reuse the cached token rather than obtaining new IAM token each time.
- **AWS Credentials**: The extension uses the [default credentials provider chain](https://docs.aws.amazon.com/sdk-for-cpp/v1/developer-guide/credproviders.html#credproviders-default-credentials-provider-chain), the credential providers are currently NOT configurable.

## Building & Loading the Extension

The DuckDB submodule must be initialized prior to building.

```bash
git submodule init
git pull --recurse-submodules
```

To build, type 
```
make
```

To run, run the bundled `duckdb` shell:
```
 ./build/release/duckdb -unsigned  # allow unsigned extensions
```

Then, load the Postgres extension like so:
```SQL
LOAD 'build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
```

## Testing

Requires a running Postgres instance reachable via libpq defaults (e.g. `psql` works without extra flags). Create the test database schema:

```bash
PGHOST=localhost PGPORT=5432 PGUSER=<user> PGPASSWORD=<password> ./create-postgres-tables.sh
```

Run the tests with `scripts/run-tests.sh`, which also links the freshly built extension into `~/.duckdb/extensions/<version>/<platform>/` (the extension is copied there on first load and not overwritten on reruns otherwise):

```bash
POSTGRES_TEST_DATABASE_AVAILABLE=1 scripts/run-tests.sh build/release
```

Run a single test:

```bash
POSTGRES_TEST_DATABASE_AVAILABLE=1 scripts/run-tests.sh build/release test/sql/storage/attach_simple.test
```

The script runs `unittest --keep-home --order lex`. Do not set `LOCAL_EXTENSION_REPO` (or pass `--autoloading`) - the test runner then autoloads extensions instead of loading them with `require`, and settings of the extension are unknown until it is autoloaded.

The remote pushdown tests pass with both byte-wise (e.g. `C.UTF-8`) and other (e.g. `en_US.UTF-8`, as in the Postgres Docker images used by the CI) database collations.

> **Note:** debug and release launchers and extensions should not be mixed — use `build/reldebug` consistently if building with `make reldebug`.
