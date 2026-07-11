//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/postgres_schema_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "storage/postgres_table_set.hpp"
#include "storage/postgres_index_set.hpp"
#include "storage/postgres_type_set.hpp"

namespace duckdb {
class PostgresTransaction;

class PostgresSchemaEntry : public SchemaCatalogEntry {
public:
	PostgresSchemaEntry(Catalog &catalog, CreateSchemaInfo &info);
	PostgresSchemaEntry(Catalog &catalog, CreateSchemaInfo &info, unique_ptr<PostgresResultSlice> tables,
	                    unique_ptr<PostgresResultSlice> enums, unique_ptr<PostgresResultSlice> composite_types,
	                    unique_ptr<PostgresResultSlice> indexes);

public:
	optional_ptr<CatalogEntry> CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) override;
	optional_ptr<CatalogEntry> CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
	                                       TableCatalogEntry &table) override;
	optional_ptr<CatalogEntry> CreateView(CatalogTransaction transaction, CreateViewInfo &info) override;
	optional_ptr<CatalogEntry> CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) override;
	optional_ptr<CatalogEntry> CreateTableFunction(CatalogTransaction transaction,
	                                               CreateTableFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateCopyFunction(CatalogTransaction transaction,
	                                              CreateCopyFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreatePragmaFunction(CatalogTransaction transaction,
	                                                CreatePragmaFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info) override;
	optional_ptr<CatalogEntry> CreateType(CatalogTransaction transaction, CreateTypeInfo &info) override;
	void Alter(CatalogTransaction transaction, AlterInfo &info) override;
	void Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;
	void Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;
	void DropEntry(ClientContext &context, DropInfo &info) override;
	optional_ptr<CatalogEntry> LookupEntry(CatalogTransaction transaction, const EntryLookupInfo &lookup_info) override;

	static bool SchemaIsInternal(const string &name);

private:
	void AlterTable(PostgresTransaction &transaction, RenameTableInfo &info);
	void AlterTable(PostgresTransaction &transaction, RenameColumnInfo &info);
	void AlterTable(PostgresTransaction &transaction, AddColumnInfo &info);
	void AlterTable(PostgresTransaction &transaction, RemoveColumnInfo &info);

	void TryDropEntry(ClientContext &context, CatalogType catalog_type, const string &name);

	PostgresCatalogSet &GetCatalogSet(CatalogType type);

private:
	PostgresTableSet tables;
	PostgresIndexSet indexes;
	PostgresTypeSet types;
};

} // namespace duckdb
