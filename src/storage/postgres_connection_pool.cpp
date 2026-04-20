#include "storage/postgres_connection_pool.hpp"

#include <thread>

#include "storage/postgres_catalog.hpp"

namespace duckdb {

static dbconnector::pool::ConnectionPoolConfig CreateConfig(ClientContext &context);

static std::string GetHealthCheckQueryFromConfig(ClientContext &context) {
	Value val;
	if (context.TryGetCurrentSetting("pg_pool_health_check_query", val)) {
		if (val.IsNull()) {
			return std::string();
		}
		return StringValue::Get(val);
	}
	return PostgresConnectionPool::DefaultHealthCheckQuery();
}

static PostgresPoolAcquireMode GetAcquireModeFromConfig(ClientContext &context) {
	Value mode_val;
	if (context.TryGetCurrentSetting("pg_pool_acquire_mode", mode_val)) {
		return PostgresConnectionPool::AcquireModeFromString(mode_val.ToString());
	}
	return PostgresPoolAcquireMode::FORCE;
}

PostgresConnectionPool::PostgresConnectionPool(PostgresCatalog &postgres_catalog, ClientContext &context)
    : dbconnector::pool::ConnectionPool<PostgresConnection>(CreateConfig(context)), postgres_catalog(postgres_catalog),
      health_check_query(GetHealthCheckQueryFromConfig(context)), acquire_mode(GetAcquireModeFromConfig(context)) {
}

PostgresPoolConnection PostgresConnectionPool::ForceGetConnection() {
	return ForceAcquire();
}

bool PostgresConnectionPool::TryGetConnection(PostgresPoolConnection &connection) {
	PostgresPoolConnection acquired = PoolEnabled() ? TryAcquire() : ForceAcquire();
	if (!acquired) {
		return false;
	}
	connection = std::move(acquired);
	return true;
}

PostgresPoolConnection PostgresConnectionPool::GetConnection() {
	if (!PoolEnabled()) {
		return ForceAcquire();
	}
	PostgresPoolAcquireMode mode = GetAcquireMode();
	switch (mode) {
	case PostgresPoolAcquireMode::FORCE:
		return ForceAcquire();
	case PostgresPoolAcquireMode::WAIT:
		return WaitAcquire();
	case PostgresPoolAcquireMode::TRY:
		return TryAcquire();
	default:
		throw IOException("Invalid unsupported acquire mode: %d" + static_cast<int>(mode));
	}
}

std::unique_ptr<PostgresConnection> PostgresConnectionPool::CreateNewConnection() {
	auto conn = PostgresConnection::Open(postgres_catalog.connection_string, postgres_catalog.attach_path);
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

std::string PostgresConnectionPool::GetHealthCheckQuery() {
	std::lock_guard<std::mutex> guard(config_mutex);
	return std::string(health_check_query.data(), health_check_query.length());
}

void PostgresConnectionPool::SetHealthCheckQuery(const std::string &query) {
	std::lock_guard<std::mutex> guard(config_mutex);
	this->health_check_query = std::string(query.data(), query.length());
}

PostgresPoolAcquireMode PostgresConnectionPool::GetAcquireMode() {
	std::lock_guard<std::mutex> guard(config_mutex);
	return acquire_mode;
}

void PostgresConnectionPool::SetAcquireMode(PostgresPoolAcquireMode mode) {
	std::lock_guard<std::mutex> guard(config_mutex);
	this->acquire_mode = mode;
}

idx_t PostgresConnectionPool::DefaultPoolSize() {
	idx_t detected = static_cast<idx_t>(std::thread::hardware_concurrency());
	idx_t detected_adjusted = detected * 3 / 2;
	idx_t default_num = static_cast<idx_t>(8);
	return detected_adjusted > default_num ? detected_adjusted : default_num;
}

std::string PostgresConnectionPool::DefaultHealthCheckQuery() {
	return "SELECT 1";
}

static dbconnector::pool::ConnectionPoolConfig CreateConfig(ClientContext &ctx) {
	dbconnector::pool::ConnectionPoolConfig config;

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

	return config;
}

bool PostgresConnectionPool::PoolEnabled() {
	return GetMaxConnections() > 0;
}

PostgresPoolAcquireMode PostgresConnectionPool::AcquireModeFromString(const std::string &mode_str) {
	auto ms = StringUtil::Lower(mode_str);
	if (ms == "force") {
		return PostgresPoolAcquireMode::FORCE;
	} else if (ms == "wait") {
		return PostgresPoolAcquireMode::WAIT;
	} else if (ms == "try") {
		return PostgresPoolAcquireMode::TRY;
	} else {
		throw InvalidInputException("Invalid unsupported acquire mode: '%s'", mode_str);
	}
}

std::string PostgresConnectionPool::AcquireModeToString(PostgresPoolAcquireMode mode) {
	switch (mode) {
	case PostgresPoolAcquireMode::FORCE:
		return "force";
	case PostgresPoolAcquireMode::WAIT:
		return "wait";
	case PostgresPoolAcquireMode::TRY:
		return "try";
	default:
		throw InvalidInputException("Invalid unsupported acquire mode: %d" + static_cast<int>(mode));
	}
}

void PostgresConnectionPool::ValidatePoolAcquireMode(ClientContext &context, SetScope scope, Value &parameter) {
	PostgresPoolAcquireMode mode = AcquireModeFromString(parameter.ToString());
	if (mode != PostgresPoolAcquireMode::FORCE) {
		Value pool_size_val;
		if (context.TryGetCurrentSetting("pg_pool_max_connections", pool_size_val)) {
			auto pool_size = pool_size_val.GetValue<uint64_t>();
			if (pool_size == 0) {
				std::string mode_str = AcquireModeToString(mode);
				throw InvalidInputException(
				    "pg_pool_pool_acquire_mode='%s' requires pg_pool_max_connections > 0 (pooling enabled)", mode_str);
			}
		}
	}
}

} // namespace duckdb
