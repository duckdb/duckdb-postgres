# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(postgres_scanner
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    DONT_LINK
    LOAD_TESTS
)

duckdb_extension_load(tpch)
duckdb_extension_load(tpcds)
duckdb_extension_load(json)

# Any extra extensions that should be built
# e.g.: duckdb_extension_load(json)
