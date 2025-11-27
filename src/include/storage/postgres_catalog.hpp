//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/postgres_catalog.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/enums/access_mode.hpp"
#include "postgres_connection.hpp"
#include "storage/postgres_schema_set.hpp"
#include "storage/postgres_connection_pool.hpp"

namespace duckdb {
class PostgresCatalog;
class PostgresSchemaEntry;

class PostgresCatalog : public Catalog {
public:
	explicit PostgresCatalog(AttachedDatabase &db_p, string connection_string, string attach_path,
	                         AccessMode access_mode, string schema_to_load, PostgresIsolationLevel isolation_level,
	                         string secret_name = string());
	~PostgresCatalog();

	string connection_string;
	string attach_path;
	string secret_name;
	AccessMode access_mode;
	PostgresIsolationLevel isolation_level;

public:
	void Initialize(bool load_builtin) override;
	string GetCatalogType() override {
		return "postgres";
	}
	string GetDefaultSchema() const override {
		return default_schema.empty() ? "public" : default_schema;
	}

	static string GetConnectionString(ClientContext &context, const string &attach_path, string secret_name);

	string GetFreshConnectionString(ClientContext &context);

	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override;

	void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;

	optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction, const EntryLookupInfo &schema_lookup,
	                                              OnEntryNotFound if_not_found) override;

	PhysicalOperator &PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner, LogicalCreateTable &op,
	                                    PhysicalOperator &plan) override;
	PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
	                             optional_ptr<PhysicalOperator> plan) override;
	PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
	                             PhysicalOperator &plan) override;
	PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
	                             PhysicalOperator &plan) override;
	PhysicalOperator &PlanMergeInto(ClientContext &context, PhysicalPlanGenerator &planner, LogicalMergeInto &op,
	                                PhysicalOperator &plan) override;

	unique_ptr<LogicalOperator> BindCreateIndex(Binder &binder, CreateStatement &stmt, TableCatalogEntry &table,
	                                            unique_ptr<LogicalOperator> plan) override;

	DatabaseSize GetDatabaseSize(ClientContext &context) override;

	PostgresVersion GetPostgresVersion() const {
		return version;
	}

	//! Label all postgres scans in the sub-tree as requiring materialization
	//! This is used for e.g. insert queries that have both (1) a scan from a postgres table, and (2) a sink into one
	static void MaterializePostgresScans(PhysicalOperator &op);
	static bool IsPostgresScan(const string &name);

	//! Whether or not this is an in-memory Postgres database
	bool InMemory() override;
	string GetDBPath() override;

	PostgresConnectionPool &GetConnectionPool() {
		return connection_pool;
	}

	void ClearCache();

	//! Whether or not this catalog should search a specific type with the standard priority
	CatalogLookupBehavior CatalogTypeLookupRule(CatalogType type) const override {
		switch (type) {
		case CatalogType::INDEX_ENTRY:
		case CatalogType::TABLE_ENTRY:
		case CatalogType::TYPE_ENTRY:
		case CatalogType::VIEW_ENTRY:
			return CatalogLookupBehavior::STANDARD;
		default:
			// unsupported type (e.g. scalar functions, aggregates, ...)
			return CatalogLookupBehavior::NEVER_LOOKUP;
		}
	}

private:
	void DropSchema(ClientContext &context, DropInfo &info) override;

private:
	PostgresVersion version;
	PostgresSchemaSet schemas;
	PostgresConnectionPool connection_pool;
	string default_schema;
};

} // namespace duckdb
