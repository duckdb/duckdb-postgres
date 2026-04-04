#include "storage/postgres_connection_pool.hpp"

#include <thread>

#include "storage/postgres_catalog.hpp"

namespace duckdb {

static dbconnector::pool::ConnectionPoolConfig CreateConfig(PostgresCatalog &postgres_catalog);

static std::string GetHealthCheckQueryFromConfig(PostgresCatalog &postgres_catalog) {
	Value val;
	if (postgres_catalog.GetDatabase().TryGetCurrentSetting("pg_pool_health_check_query", val)) {
		if (val.IsNull()) {
			return std::string();
		}
		return StringValue::Get(val);
	}
	return PostgresConnectionPool::DefaultHealthCheckQuery();
}

PostgresConnectionPool::PostgresConnectionPool(PostgresCatalog &postgres_catalog)
    : dbconnector::pool::ConnectionPool<PostgresConnection>(CreateConfig(postgres_catalog)),
      postgres_catalog(postgres_catalog), health_check_query(GetHealthCheckQueryFromConfig(postgres_catalog)) {
}

PostgresPoolConnection PostgresConnectionPool::ForceGetConnection() {
	return ForceAcquire();
}

bool PostgresConnectionPool::TryGetConnection(PostgresPoolConnection &connection) {
	bool pool_enabled = GetMaxConnections() > 0;
	PostgresPoolConnection acquired = pool_enabled ? TryAcquire() : ForceAcquire();
	if (!acquired) {
		return false;
	}
	connection = std::move(acquired);
	return true;
}

PostgresPoolConnection PostgresConnectionPool::GetConnection() {
	bool pool_enabled = GetMaxConnections() > 0;
	return pool_enabled ? WaitAcquire() : ForceAcquire();
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

idx_t PostgresConnectionPool::DefaultPoolSize() {
	idx_t detected = static_cast<idx_t>(std::thread::hardware_concurrency());
	idx_t default_num = static_cast<idx_t>(8);
	return detected > default_num ? detected : default_num;
}

std::string PostgresConnectionPool::DefaultHealthCheckQuery() {
	return "SELECT 1";
}

static dbconnector::pool::ConnectionPoolConfig CreateConfig(PostgresCatalog &postgres_catalog) {
	DatabaseInstance &db = postgres_catalog.GetDatabase();

	Value connection_limit;
	uint64_t max_connections = PostgresConnectionPool::DefaultPoolSize();
	if (db.TryGetCurrentSetting("pg_connection_limit", connection_limit) && !connection_limit.IsNull()) {
		max_connections = UBigIntValue::Get(connection_limit);
	}

	dbconnector::pool::ConnectionPoolConfig config;
	config.max_connections = max_connections;
	return config;
}

} // namespace duckdb
