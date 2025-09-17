//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/postgres_schema_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/postgres_catalog_set.hpp"
#include "storage/postgres_schema_entry.hpp"

namespace duckdb {
struct CreateSchemaInfo;

class PostgresSchemaSet : public PostgresCatalogSet {
public:
	explicit PostgresSchemaSet(Catalog &catalog, string schema_to_load);

public:
	optional_ptr<CatalogEntry> CreateSchema(PostgresTransaction &transaction, CreateSchemaInfo &info);

	static string GetInitializeQuery(const string &schema = string());

protected:
	void LoadEntries(PostgresTransaction &transaction) override;

protected:
	//! Schema to load - if empty loads all schemas (default behavior)
	string schema_to_load;
};

} // namespace duckdb
