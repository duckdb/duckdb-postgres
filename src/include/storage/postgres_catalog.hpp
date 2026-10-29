//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/postgres_catalog.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "dbconnector/attached.hpp"

#include <memory>
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include <mutex>

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/enums/access_mode.hpp"
#include "postgres_aws.hpp"
#include "postgres_connection.hpp"
#include "storage/postgres_capabilities.hpp"
#include "storage/postgres_schema_set.hpp"
#include "storage/postgres_connection_pool.hpp"
#include "storage/postgres_secret_storage.hpp"

namespace duckdb {
class PostgresCatalog;
class PostgresSchemaEntry;
struct PostgresCacheInvalidation;

class PostgresCatalog : public Catalog {
public:
	explicit PostgresCatalog(ClientContext &ctx, AttachedDatabase &db_p, string attach_path, AccessMode access_mode,
	                         vector<string> schemas_to_load, PostgresIsolationLevel isolation_level,
	                         const string &secret_name, SecretStorageTable secret_storage_table_p,
	                         PostgresTextProtocolMode text_protocol_mode, string connect_display = string());
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
	optional<Identifier> GetDefaultSchema() const override {
		return default_schema.empty() ? Identifier("public") : default_schema;
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

	//! Whether the database default collation orders strings byte-wise, like DuckDB
	bool HasBinaryCollation() const {
		return capabilities.GetInfo().binary_collation;
	}
	PostgresVersion GetPostgresVersion() const {
		return version;
	}

	bool UseTextProtocol(PostgresQueryMode query_mode, bool use_text_protocol_setting) const;

	//! Label all postgres scans in the sub-tree as requiring materialization
	//! This is used for e.g. insert queries that have both (1) a scan from a postgres table, and (2) a sink into one
	static void MaterializePostgresScans(PhysicalOperator &op);
	static bool IsPostgresScan(const string &name);

	//! Whether or not this is an in-memory Postgres database
	bool InMemory() override;
	string GetDBPath() override;

	bool Supports(RemoteCapability capability) const override {
		if (!remote_pushdown_enabled) {
			return capability == RemoteCapability::IS_REMOTE || capability == RemoteCapability::CONNECT;
		}
		return capabilities.Supports(capability);
	}
	bool SupportsPushdown(const ParsedExpression &expression) override;
	bool SupportsPushdown(const TableRef &ref) override;
	bool SupportsPushdown(const QueryNode &node) override;
	bool SupportsPushdown(const SQLStatement &statement) override;
	unique_ptr<TableRef> RemoteExecute(ClientContext &context, unique_ptr<QueryNode> node) override;
	unique_ptr<TableRef> RemoteExecute(ClientContext &context, unique_ptr<SQLStatement> statement) override;
	unique_ptr<TableRef> RemoteExecute(ClientContext &context, const string &sql) override;
	string GetConnectDisplay() override;

	PostgresConnectionPool &GetConnectionPool() {
		return *connection_pool;
	}

	shared_ptr<PostgresConnectionPool> GetConnectionPoolPtr() {
		return connection_pool;
	}

	static dbconnector::attached::AttachedCatalog Lookup(ClientContext &ctx, const Identifier &name);

	void ClearCache();
	void InvalidateCacheEntry(ClientContext &context, const PostgresCacheInvalidation &invalidation);

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
	void InitializeRemotePushdown(ClientContext &context, PostgresConnection &connection);
	//! Statements to run before a pushed down query (e.g. aligning the session time zone with DuckDB's)
	vector<string> GetRemotePushdownPreliminaryQueries(ClientContext &context, bool requires_time_zone);

	static unique_ptr<SecretEntry> GetSecretEntry(ClientContext &ctx, const std::string &secret_name);
	static string CreateConnectionString(optional_ptr<SecretEntry> secret_entry, const string &attach_path);

private:
	PostgresVersion version;
	PostgresCapabilities capabilities;
	bool remote_pushdown_enabled = false;
	//! Whether table scans can be parallelized with ctid ranges (pg_use_ctid_scan, the protocol and the version at
	//! attach time) - otherwise plain table scans are pushed down as well
	bool parallel_scans_supported = false;
	//! The Postgres session time zone at attach time
	string remote_time_zone;
	PostgresSchemaSet schemas;
	shared_ptr<PostgresConnectionPool> connection_pool;
	Identifier default_schema;
	SecretStorageTable secret_storage_table;
	//! Overrides what the shell prompt displays for this catalog - see GetConnectDisplay
	string connect_display;

	PostgresAwsRdsTokenConfig rds_token_config;
	std::mutex rds_token_mutex;
	std::string rds_token;
	std::chrono::steady_clock::time_point rds_token_last_refreshed;
	std::string connection_string;
};

} // namespace duckdb
