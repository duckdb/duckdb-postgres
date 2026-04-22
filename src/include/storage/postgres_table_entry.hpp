//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/postgres_table_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "postgres_utils.hpp"

namespace duckdb {

struct PostgresTableInfo {
	PostgresTableInfo() {
		create_info = make_uniq<CreateTableInfo>();
		create_info->columns.SetAllowDuplicates(true);
	}
	PostgresTableInfo(const string &schema, const string &table) {
		create_info = make_uniq<CreateTableInfo>(string(), schema, table);
		create_info->columns.SetAllowDuplicates(true);
	}
	PostgresTableInfo(const SchemaCatalogEntry &schema, const string &table) {
		create_info = make_uniq<CreateTableInfo>((SchemaCatalogEntry &)schema, table);
		create_info->columns.SetAllowDuplicates(true);
	}

	const string &GetTableName() const {
		return create_info->table;
	}

	unique_ptr<CreateTableInfo> create_info;
	vector<PostgresType> postgres_types;
	vector<string> postgres_names;
	int64_t approx_num_pages = 0;
};

class PostgresTableEntry : public TableCatalogEntry {
public:
	PostgresTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info);
	PostgresTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, PostgresTableInfo &info);

public:
	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;

	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;

	TableStorageInfo GetStorageInfo(ClientContext &context) override;

	void BindUpdateConstraints(Binder &binder, LogicalGet &get, LogicalProjection &proj, LogicalUpdate &update,
	                           ClientContext &context) override;

	//! Get the copy format (text or binary) that should be used when writing data to this table
	PostgresCopyFormat GetCopyFormat(ClientContext &context);

public:
	//! Postgres type annotations
	vector<PostgresType> postgres_types;
	//! Column names as they are within Postgres
	//! We track these separately because of case sensitivity - Postgres allows e.g. the columns "ID" and "id" together
	//! We would in this case remap them to "ID" and "id:1", while postgres_names store the original names
	vector<string> postgres_names;
	//! The approximate number of pages a table consumes in Postgres
	std::atomic<int64_t> approx_num_pages;
};

} // namespace duckdb
