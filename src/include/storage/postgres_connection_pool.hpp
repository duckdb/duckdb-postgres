//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/postgres_connection_pool.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <mutex>
#include "duckdb/main/client_context.hpp"

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
	PostgresConnectionPool(PostgresCatalog &postgres_catalog, ClientContext &context);

public:
	bool TryGetConnection(PostgresPoolConnection &connection);

	static void ValidatePoolAcquireMode(ClientContext &context, SetScope scope, Value &parameter);

protected:
	std::unique_ptr<PostgresConnection> CreateNewConnection() override;
	bool CheckConnectionHealthy(PostgresConnection &conn) override;
	void ResetConnection(PostgresConnection &conn) override;

private:
	PostgresCatalog &postgres_catalog;

	bool PoolEnabled();
	static dbconnector::pool::ConnectionPoolConfig CreateConfig(ClientContext &ctx);
};

class PostgresConfigurePoolFunction : public TableFunction {
public:
	PostgresConfigurePoolFunction();
};

} // namespace duckdb
