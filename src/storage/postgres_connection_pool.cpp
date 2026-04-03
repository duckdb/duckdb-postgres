#include "storage/postgres_connection_pool.hpp"

#include <thread>

#include "storage/postgres_catalog.hpp"

namespace duckdb {

PostgresConnectionPool::PostgresConnectionPool(PostgresCatalog &postgres_catalog, idx_t maximum_connections_p)
    : dbconnector::pool::ConnectionPool<PostgresConnection>(CreateConfig(maximum_connections_p)),
      postgres_catalog(postgres_catalog) {
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
	return conn.PingServer();
}

void PostgresConnectionPool::ResetConnection(PostgresConnection &conn) {
	conn.Reset();
}

dbconnector::pool::ConnectionPoolConfig PostgresConnectionPool::CreateConfig(idx_t max_connections) {
	dbconnector::pool::ConnectionPoolConfig config;
	config.max_connections = max_connections;
	return config;
}

idx_t PostgresConnectionPool::DefaultPoolSize() noexcept {
	idx_t detected = static_cast<idx_t>(std::thread::hardware_concurrency());
	idx_t default_num = static_cast<idx_t>(8);
	return detected < default_num ? detected : default_num;
}

} // namespace duckdb
