#include "storage/postgres_connection_pool.hpp"

#include <memory>
#include <thread>

#include "duckdb/main/client_context.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/function/table_function.hpp"

#include "dbconnector/functions/configure_pool.hpp"

#include "storage/postgres_catalog.hpp"

namespace duckdb {

using dbconnector::functions::ConfigurePool;

PostgresConnectionPool::PostgresConnectionPool(PostgresCatalog &postgres_catalog, ClientContext &context)
    : dbconnector::pool::ConnectionPool<PostgresConnection>(CreateConfig(context)), postgres_catalog(postgres_catalog) {
}

bool PostgresConnectionPool::TryGetConnection(PostgresPoolConnection &connection) {
	PostgresPoolConnection acquired = PoolEnabled() ? TryAcquire() : ForceAcquire();
	if (!acquired) {
		return false;
	}
	connection = std::move(acquired);
	return true;
}

std::unique_ptr<PostgresConnection> PostgresConnectionPool::CreateNewConnection() {
	auto conn = PostgresConnection::Open(postgres_catalog.GetConnectionString(), postgres_catalog.attach_path);
	return make_uniq<PostgresConnection>(std::move(conn));
}

bool PostgresConnectionPool::CheckConnectionHealthy(PostgresConnection &conn) {
	if (!conn.IsOpen()) {
		return false;
	}
	std::string query = GetHealthCheckQuery();
	return conn.PingServer(query);
}

void PostgresConnectionPool::ResetConnection(PostgresConnection &conn) {
	std::string query = GetHealthCheckQuery();
	conn.Reset(query);
}

dbconnector::pool::ConnectionPoolConfig PostgresConnectionPool::CreateConfig(ClientContext &ctx) {
	dbconnector::pool::ConnectionPoolConfig config;

	{
		Value mode_val;
		if (ctx.TryGetCurrentSetting("pg_pool_acquire_mode", mode_val)) {
			config.acquire_mode = dbconnector::pool::AcquireModeHelpers::FromString(mode_val.ToString());
		}
	}
	{
		Value val;
		if (ctx.TryGetCurrentSetting("pg_pool_max_connections", val) && !val.IsNull()) {
			config.max_connections = UBigIntValue::Get(val);
		}
	}
	{
		Value val;
		if (ctx.TryGetCurrentSetting("pg_pool_wait_timeout_millis", val) && !val.IsNull()) {
			config.wait_timeout_millis = UBigIntValue::Get(val);
		}
	}
	{
		Value val;
		if (ctx.TryGetCurrentSetting("pg_pool_enable_thread_local_cache", val) && !val.IsNull()) {
			config.tl_cache_enabled = BooleanValue::Get(val);
		}
	}
	{
		Value val;
		if (ctx.TryGetCurrentSetting("pg_pool_max_lifetime_millis", val) && !val.IsNull()) {
			config.max_lifetime_millis = UBigIntValue::Get(val);
		}
	}
	{
		Value val;
		if (ctx.TryGetCurrentSetting("pg_pool_idle_timeout_millis", val) && !val.IsNull()) {
			config.idle_timeout_millis = UBigIntValue::Get(val);
		}
	}
	{
		Value val;
		if (ctx.TryGetCurrentSetting("pg_pool_enable_reaper_thread", val) && !val.IsNull()) {
			config.start_reaper_thread = BooleanValue::Get(val);
		}
	}
	{
		Value val;
		if (ctx.TryGetCurrentSetting("pg_pool_health_check_query", val) && !val.IsNull()) {
			config.health_check_query = StringValue::Get(val);
		}
	}

	return config;
}

bool PostgresConnectionPool::PoolEnabled() {
	return GetMaxConnections() > 0;
}

void PostgresConnectionPool::ValidatePoolAcquireMode(ClientContext &context, SetScope scope, Value &parameter) {
	dbconnector::pool::AcquireMode mode = dbconnector::pool::AcquireModeHelpers::FromString(parameter.ToString());
	if (mode != dbconnector::pool::AcquireMode::FORCE) {
		Value pool_size_val;
		if (context.TryGetCurrentSetting("pg_pool_max_connections", pool_size_val)) {
			auto pool_size = pool_size_val.GetValue<uint64_t>();
			if (pool_size == 0) {
				std::string mode_str = dbconnector::pool::AcquireModeHelpers::ToString(mode);
				throw InvalidInputException(
				    "pg_pool_pool_acquire_mode='%s' requires pg_pool_max_connections > 0 (pooling enabled)", mode_str);
			}
		}
	}
}

static shared_ptr<PostgresConnectionPool> GetConnnectionPoolFromCatalog(Catalog &catalog) {
	if (catalog.GetCatalogType() != "postgres") {
		return nullptr;
	}
	return catalog.Cast<PostgresCatalog>().GetConnectionPoolPtr();
}

PostgresConfigurePoolFunction::PostgresConfigurePoolFunction()
    : TableFunction("postgres_configure_pool", std::vector<LogicalType>(),
                    ConfigurePool::Function<PostgresConnection, GetConnnectionPoolFromCatalog>, ConfigurePool::Bind,
                    ConfigurePool::InitGlobalState, ConfigurePool::InitLocalState) {
	for (auto &en : ConfigurePool::NamedParameters()) {
		named_parameters[en.first] = en.second;
	}
}

} // namespace duckdb
