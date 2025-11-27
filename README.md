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

The extension supports AWS RDS IAM-based authentication, which allows you to connect to RDS PostgreSQL instances using IAM database authentication instead of static passwords. This feature automatically generates temporary authentication tokens using the AWS CLI.

#### Requirements

- AWS CLI installed and configured
- RDS instance with IAM database authentication enabled
- IAM user/role with `rds-db:connect` permission for the RDS instance
- AWS credentials configured (via `AWS_PROFILE`, `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`, or IAM role)

#### Usage

To use RDS IAM authentication, create a Postgres secret with the `USE_RDS_IAM_AUTH` parameter set to `true`:

```sql
CREATE SECRET rds_secret (
    TYPE POSTGRES,
    HOST 'my-db-instance.xxxxxx.us-west-2.rds.amazonaws.com',
    PORT '5432',
    USER 'my_iam_user',
    DATABASE 'mydb',
    USE_RDS_IAM_AUTH true,
    AWS_REGION 'us-west-2'  -- Optional: uses AWS CLI default if not specified
);

ATTACH '' AS rds_db (TYPE POSTGRES, SECRET rds_secret);
```

#### Secret Parameters for RDS IAM Authentication

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `USE_RDS_IAM_AUTH` | BOOLEAN | Yes | Enable RDS IAM authentication |
| `HOST` | VARCHAR | Yes | RDS instance hostname |
| `PORT` | VARCHAR | Yes | RDS instance port (typically 5432) |
| `USER` | VARCHAR | Yes | IAM database username |
| `DATABASE` or `DBNAME` | VARCHAR | No | Database name |
| `AWS_REGION` | VARCHAR | No | AWS region (optional, uses AWS CLI default if not specified) |

#### Important Notes

- **Token Expiration**: RDS auth tokens expire after 15 minutes. The extension automatically generates fresh tokens when creating new connections, so long-running queries will continue to work.
- **AWS CLI**: The extension uses the `aws rds generate-db-auth-token` command. Make sure the AWS CLI is installed and configured with appropriate credentials.
- **Environment Variables**: The AWS CLI command inherits environment variables from the parent process, so `AWS_PROFILE`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, and `AWS_REGION` will be available to the AWS CLI.


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
