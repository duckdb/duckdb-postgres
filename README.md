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

The extension is loaded from `~/.duckdb/extensions/<version>/<platform>/`, copied there on first load and not overwritten on reruns. Create a symlink so the latest build is always used:

```bash
DUCKDB_VERSION=$(ls build/reldebug/repository/)
DUCKDB_PLATFORM=$(ls build/reldebug/repository/${DUCKDB_VERSION}/)
ln -sf "$(pwd)/build/reldebug/extension/postgres_scanner/postgres_scanner.duckdb_extension" \
  ~/.duckdb/extensions/${DUCKDB_VERSION}/${DUCKDB_PLATFORM}/postgres_scanner.duckdb_extension
```

Run all tests:

```bash
POSTGRES_TEST_DATABASE_AVAILABLE=1 LOCAL_EXTENSION_REPO=./build/release/repository \
  ./build/release/test/unittest --autoloading available
```

Run a single test:

```bash
POSTGRES_TEST_DATABASE_AVAILABLE=1 LOCAL_EXTENSION_REPO=./build/release/repository \
  ./build/release/test/unittest --autoloading available test/sql/storage/attach_simple.test
```

> **Note:** debug and release launchers and extensions should not be mixed — use `build/reldebug` consistently if building with `make reldebug`.
