//===----------------------------------------------------------------------===//
//                         DuckDB
//
// postgres_scanner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/main/client_context.hpp"

#include "dbconnector/bind_data.hpp"

#include "postgres_utils.hpp"
#include "postgres_connection.hpp"
#include "postgres_parameters.hpp"
#include "storage/postgres_connection_pool.hpp"

namespace duckdb {
class PostgresCatalog;
struct PostgresLocalState;
struct PostgresGlobalState;
class PostgresTransaction;

struct PostgresBindData : public dbconnector::BindData {
	static constexpr const idx_t DEFAULT_PAGES_PER_TASK = 1000;

public:
	PostgresBindData(ClientContext &context, const Identifier &catalog_name);

	string schema_name;
	string table_name;

	Identifier catalog_name;
	QualifiedName qualified_table_name;

	// required for get_bind_info and only used there
	weak_ptr<ClientContext> context_ptr;

	string sql;
	PostgresParameters params;
	idx_t pages_approx = 0;

	vector<PostgresType> postgres_types;
	vector<string> names;
	vector<LogicalType> types;

	idx_t pages_per_task = DEFAULT_PAGES_PER_TASK;
	string dsn;
	string attach_path;

	bool requires_materialization = true;
	bool can_use_main_thread = true;
	bool read_only = true;
	bool emit_ctid = false;
	bool use_transaction = true;
	bool use_text_protocol = false;
	//! Set by postgres_query's bind when the statement returns no columns (a command like DDL, or
	//! DML without RETURNING). InitGlobalState executes it and returns a single-row Success result.
	bool command_only = false;
	idx_t max_threads = 1;
	PostgresTypeConfig type_config;

	dbconnector::optimizer::OrderByAndLimitBindData order_by_and_limit_bind_data;
	dbconnector::optimizer::AggregateBindData aggregate_bind_data;

public:
	void SetTablePages(idx_t approx_num_pages);

	unique_ptr<FunctionData> Copy() const override {
		throw NotImplementedException("");
	}
	bool Equals(const FunctionData &other_p) const override {
		return false;
	}

	dbconnector::optimizer::OrderByAndLimitBindData &GetOrderByAndLimitBindData() override {
		return order_by_and_limit_bind_data;
	}

	dbconnector::optimizer::AggregateBindData &GetAggregateBindData() override {
		return aggregate_bind_data;
	}
};

class PostgresScanFunction : public TableFunction {
public:
	PostgresScanFunction();

	static void PrepareBind(PostgresVersion version, ClientContext &context, PostgresBindData &bind,
	                        int64_t approx_num_pages, optional_ptr<PostgresCatalog> pg_catalog);
};

class PostgresClearCacheFunction : public TableFunction {
public:
	PostgresClearCacheFunction();

	static void ClearCacheOnSetting(ClientContext &context, SetScope scope, Value &parameter);
	static void ClearPostgresCaches(ClientContext &context);
};

class PostgresQueryFunction : public TableFunction {
public:
	PostgresQueryFunction();
};

class PostgresExecuteFunction : public TableFunction {
public:
	PostgresExecuteFunction();
};

} // namespace duckdb
