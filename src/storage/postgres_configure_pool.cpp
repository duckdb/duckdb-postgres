#include "storage/postgres_connection_pool.hpp"

#include <memory>

#include "duckdb/function/table_function.hpp"
#include "duckdb/main/database_manager.hpp"

#include "storage/postgres_catalog.hpp"

namespace duckdb {

namespace {

enum class ExecState { UNINITIALIZED, EXHAUSTED };

struct ConfigurePoolBindData : public TableFunctionData {
	std::pair<std::string, bool> catalog_name;
	std::pair<uint64_t, bool> max_connections;
	std::pair<uint64_t, bool> wait_timeout_millis;
	std::pair<bool, bool> enable_thread_local_cache;
	std::pair<uint64_t, bool> max_lifetime_millis;
	std::pair<uint64_t, bool> idle_timeout_millis;
	std::pair<bool, bool> enable_reaper_thread;
	std::pair<std::string, bool> health_check_query;

	static Value Lookup(const named_parameter_map_t &map, const std::string &key) {
		auto it = map.find(key);
		if (it == map.end()) {
			return Value();
		}
		return it->second;
	}

	static std::pair<std::string, bool> LookupString(const named_parameter_map_t &map, const std::string &key) {
		Value val = Lookup(map, key);
		if (val.IsNull()) {
			return std::make_pair("", true);
		}
		std::string str = StringValue::Get(val);
		return std::make_pair(std::move(str), false);
	}

	static std::pair<uint64_t, bool> LookupUBigInt(const named_parameter_map_t &map, const std::string &key) {
		Value val = Lookup(map, key);
		if (val.IsNull()) {
			return std::make_pair(0, true);
		}
		uint64_t num = UBigIntValue::Get(val);
		return std::make_pair(num, false);
	}

	static std::pair<bool, bool> LookupBool(const named_parameter_map_t &map, const std::string &key) {
		Value val = Lookup(map, key);
		if (val.IsNull()) {
			return std::make_pair(false, true);
		}
		bool flag = BooleanValue::Get(val);
		return std::make_pair(flag, false);
	}

	ConfigurePoolBindData(const named_parameter_map_t &map)
	    : catalog_name(LookupString(map, "catalog")), max_connections(LookupUBigInt(map, "max_connections")),
	      wait_timeout_millis(LookupUBigInt(map, "wait_timeout_millis")),
	      enable_thread_local_cache(LookupBool(map, "enable_thread_local_cache")),
	      max_lifetime_millis(LookupUBigInt(map, "max_lifetime_millis")),
	      idle_timeout_millis(LookupUBigInt(map, "idle_timeout_millis")),
	      enable_reaper_thread(LookupBool(map, "enable_reaper_thread")),
	      health_check_query(LookupString(map, "health_check_query")) {
	}
};

struct GlobalState : public GlobalTableFunctionState {};

struct LocalState : public LocalTableFunctionState {
	ExecState exec_state = ExecState::UNINITIALIZED;
};

} // namespace

static void AddColumn(vector<LogicalType> &return_types, vector<string> &names, const std::string &col_name,
                      LogicalType col_type) {
	names.emplace_back(col_name);
	return_types.emplace_back(col_type);
}

static unique_ptr<FunctionData> ConfigurePoolBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	AddColumn(return_types, names, "catalog", LogicalType::VARCHAR);
	AddColumn(return_types, names, "available_connections", LogicalType::UBIGINT);
	AddColumn(return_types, names, "max_connections", LogicalType::UBIGINT);
	AddColumn(return_types, names, "wait_timeout_millis", LogicalType::UBIGINT);
	AddColumn(return_types, names, "thread_local_cache_enabled", LogicalType::BOOLEAN);
	AddColumn(return_types, names, "thread_local_cache_hits", LogicalType::UBIGINT);
	AddColumn(return_types, names, "thread_local_cache_misses", LogicalType::UBIGINT);
	AddColumn(return_types, names, "max_lifetime_millis", LogicalType::UBIGINT);
	AddColumn(return_types, names, "idle_timeout_millis", LogicalType::UBIGINT);
	AddColumn(return_types, names, "reaper_thread_running", LogicalType::BOOLEAN);
	AddColumn(return_types, names, "health_check_query", LogicalType::VARCHAR);

	return make_uniq<ConfigurePoolBindData>(input.named_parameters);
}

static unique_ptr<GlobalTableFunctionState> ConfigurePoolInitGlobalState(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<GlobalState>();
}

static unique_ptr<LocalTableFunctionState> ConfigurePoolInitLocalState(ExecutionContext &, TableFunctionInitInput &,
                                                                       GlobalTableFunctionState *) {
	return make_uniq<LocalState>();
}

static void ConfigurePoolFunction(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &bdata = input.bind_data->Cast<ConfigurePoolBindData>();
	auto &lstate = input.local_state->Cast<LocalState>();

	if (lstate.exec_state == ExecState::EXHAUSTED) {
		output.SetCardinality(0);
		return;
	}

	// collect pools
	std::vector<std::string> cat_names;
	std::vector<shared_ptr<PostgresConnectionPool>> pools;
	auto databases = DatabaseManager::Get(context).GetDatabases(context);
	for (auto &db_ref : databases) {
		auto &db = *db_ref;
		auto &catalog = db.GetCatalog();
		if (catalog.GetCatalogType() != "postgres") {
			continue;
		}
		if (!bdata.catalog_name.second && catalog.GetName() != bdata.catalog_name.first) {
			continue;
		}
		cat_names.push_back(catalog.GetName());
		shared_ptr<PostgresConnectionPool> pool = catalog.Cast<PostgresCatalog>().GetConnectionPoolPtr();
		pools.emplace_back(std::move(pool));
	}

	// configure pools
	for (auto &pool : pools) {
		if (!bdata.max_connections.second) {
			pool->SetMaxConnections(bdata.max_connections.first);
		}
		if (!bdata.wait_timeout_millis.second) {
			pool->SetWaitTimeoutMillis(bdata.wait_timeout_millis.first);
		}
		if (!bdata.enable_thread_local_cache.second) {
			pool->SetThreadLocalCacheEnabled(bdata.enable_thread_local_cache.first);
		}
		if (!bdata.max_lifetime_millis.second) {
			pool->SetMaxLifetimeMillis(bdata.max_lifetime_millis.first);
		}
		if (!bdata.idle_timeout_millis.second) {
			pool->SetIdleTimeoutMillis(bdata.idle_timeout_millis.first);
		}
		if (!bdata.enable_reaper_thread.second) {
			if (bdata.enable_reaper_thread.first) {
				pool->EnsureReaperRunning();
			} else {
				pool->ShutdownReaper();
			}
		}
		if (!bdata.health_check_query.second) {
			pool->SetHealthCheckQuery(bdata.health_check_query.first);
		}
	}

	// setresults
	idx_t row_idx = 0;
	for (auto &pool : pools) {
		idx_t col_idx = 0;
		output.SetValue(col_idx++, row_idx, Value(cat_names.at(row_idx)));
		output.SetValue(col_idx++, row_idx, Value::UBIGINT(pool->GetAvailableConnections()));
		output.SetValue(col_idx++, row_idx, Value::UBIGINT(pool->GetMaxConnections()));
		output.SetValue(col_idx++, row_idx, Value::UBIGINT(pool->GetWaitTimeoutMillis()));
		output.SetValue(col_idx++, row_idx, Value::BOOLEAN(pool->IsThreadLocalCacheEnabled()));
		output.SetValue(col_idx++, row_idx, Value::UBIGINT(pool->GetThreadLocalCacheHits()));
		output.SetValue(col_idx++, row_idx, Value::UBIGINT(pool->GetThreadLocalCacheMisses()));
		output.SetValue(col_idx++, row_idx, Value::UBIGINT(pool->GetMaxLifetimeMillis()));
		output.SetValue(col_idx++, row_idx, Value::UBIGINT(pool->GetIdleTimeoutMillis()));
		output.SetValue(col_idx++, row_idx, Value::BOOLEAN(false)); // todo
		output.SetValue(col_idx++, row_idx, Value(pool->GetHealthCheckQuery()));
		row_idx++;
	}

	output.SetCardinality(row_idx);
	lstate.exec_state = ExecState::EXHAUSTED;
}

PostgresConfigurePoolFunction::PostgresConfigurePoolFunction()
    : TableFunction("postgres_configure_pool", std::vector<LogicalType>(), ConfigurePoolFunction, ConfigurePoolBind,
                    ConfigurePoolInitGlobalState, ConfigurePoolInitLocalState) {
	named_parameters["catalog"] = LogicalType::VARCHAR;
	named_parameters["max_connections"] = LogicalType::UBIGINT;
	named_parameters["wait_timeout_millis"] = LogicalType::UBIGINT;
	named_parameters["enable_thread_local_cache"] = LogicalType::BOOLEAN;
	named_parameters["max_lifetime_millis"] = LogicalType::UBIGINT;
	named_parameters["idle_timeout_millis"] = LogicalType::UBIGINT;
	named_parameters["enable_reaper_thread"] = LogicalType::BOOLEAN;
	named_parameters["health_check_query"] = LogicalType::VARCHAR;
}

} // namespace duckdb
