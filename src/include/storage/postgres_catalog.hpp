//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/postgres_catalog.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <mutex>

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/enums/access_mode.hpp"
#include "postgres_aws.hpp"
#include "postgres_connection.hpp"
#include "storage/postgres_schema_set.hpp"
#include "storage/postgres_connection_pool.hpp"
#include "storage/postgres_secret_storage.hpp"

namespace duckdb {
class PostgresCatalog;
class PostgresSchemaEntry;

class PostgresCatalog : public Catalog {
public:
	explicit PostgresCatalog(ClientContext &ctx, AttachedDatabase &db_p, string attach_path, AccessMode access_mode,
	                         vector<string> schemas_to_load, PostgresIsolationLevel isolation_level,
	                         const string &secret_name, SecretStorageTable secret_storage_table_p,
	                         PostgresTextProtocolMode text_protocol_mode);
	~PostgresCatalog();

	string attach_path;
	AccessMode access_mode;
	PostgresIsolationLevel isolation_level;
	PostgresTextProtocolMode text_protocol_mode;

public:
	void Initialize(bool load_builtin) override;
	string GetCatalogType() override {
		return "postgres";
	}
	string GetDefaultSchema() const override {
		return default_schema.empty() ? "public" : default_schema;
	}

	string GetConnectionString();

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

	//! Resolve whether scans against this database must use the text protocol instead of the binary COPY
	//! protocol. This is the single place where that decision is made for an attached database, so every
	//! scan path (table scans, postgres_query/CONNECT, ...) agrees. `setting_value` is the value of the
	//! `pg_use_text_protocol` setting, which acts as the default when the mode is AUTO.
	bool UseTextProtocol(bool setting_value) const {
		switch (text_protocol_mode) {
		case PostgresTextProtocolMode::TEXT:
			return true;
		case PostgresTextProtocolMode::BINARY:
			return false;
		default:
			// Redshift does not support the binary COPY protocol
			return setting_value || version.type_v == PostgresInstanceType::REDSHIFT;
		}
	}

	//! Label all postgres scans in the sub-tree as requiring materialization
	//! This is used for e.g. insert queries that have both (1) a scan from a postgres table, and (2) a sink into one
	static void MaterializePostgresScans(PhysicalOperator &op);
	static bool IsPostgresScan(const string &name);

	//! Whether or not this is an in-memory Postgres database
	bool InMemory() override;
	string GetDBPath() override;

	bool Supports(RemoteCapability capability) const override {
		return capability == RemoteCapability::IS_REMOTE || capability == RemoteCapability::CONNECT;
	}
	unique_ptr<TableRef> RemoteExecute(ClientContext &context, const string &sql) override;
	string GetConnectDisplay() override;

	PostgresConnectionPool &GetConnectionPool() {
		return *connection_pool;
	}

	shared_ptr<PostgresConnectionPool> GetConnectionPoolPtr() {
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

	void RegisterSecretStorage();

	static unique_ptr<SecretEntry> GetSecretEntry(ClientContext &ctx, const std::string &secret_name);
	static string CreateConnectionString(optional_ptr<SecretEntry> secret_entry, const string &attach_path);

private:
	PostgresVersion version;
	PostgresSchemaSet schemas;
	shared_ptr<PostgresConnectionPool> connection_pool;
	string default_schema;
	SecretStorageTable secret_storage_table;

	PostgresAwsRdsTokenConfig rds_token_config;
	std::mutex rds_token_mutex;
	std::string rds_token;
	std::chrono::steady_clock::time_point rds_token_last_refreshed;
	std::string connection_string;
};

} // namespace duckdb
