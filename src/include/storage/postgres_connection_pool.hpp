//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/postgres_connection_pool.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_ptr.hpp"

#include "dbconnector/pool.hpp"

#include "postgres_connection.hpp"

namespace duckdb {
class PostgresCatalog;
class PostgresConnectionPool;

using PostgresPoolConnection = dbconnector::pool::PooledConnection<PostgresConnection>;

class PostgresConnectionPool : public dbconnector::pool::ConnectionPool<PostgresConnection> {
public:
	PostgresConnectionPool(PostgresCatalog &postgres_catalog, idx_t maximum_connections = DefaultPoolSize());

public:
	bool TryGetConnection(PostgresPoolConnection &connection);
	PostgresPoolConnection GetConnection();
	//! Always returns a connection - even if the connection slots are exhausted
	PostgresPoolConnection ForceGetConnection();

	static idx_t DefaultPoolSize() noexcept;

protected:
	std::unique_ptr<PostgresConnection> CreateNewConnection() override;
	bool CheckConnectionHealthy(PostgresConnection &conn) override;
	void ResetConnection(PostgresConnection &conn) override;

private:
	PostgresCatalog &postgres_catalog;

	static dbconnector::pool::ConnectionPoolConfig CreateConfig(idx_t max_connections);
};

} // namespace duckdb
