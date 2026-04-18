//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/postgres_connection_pool.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <mutex>

#include "duckdb/common/common.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_ptr.hpp"

#include "dbconnector/pool.hpp"

#include "postgres_connection.hpp"

namespace duckdb {
class PostgresCatalog;
class PostgresConnectionPool;

using PostgresPoolConnection = dbconnector::pool::PooledConnection<PostgresConnection>;

enum class PostgresPoolAcquireMode { FORCE, WAIT, TRY };

class PostgresConnectionPool : public dbconnector::pool::ConnectionPool<PostgresConnection> {
public:
	PostgresConnectionPool(PostgresCatalog &postgres_catalog, ClientContext &context);

public:
	bool TryGetConnection(PostgresPoolConnection &connection);
	PostgresPoolConnection GetConnection();
	//! Always returns a connection - even if the connection slots are exhausted
	PostgresPoolConnection ForceGetConnection();

	std::string GetHealthCheckQuery();
	void SetHealthCheckQuery(const std::string &query);
	PostgresPoolAcquireMode GetAcquireMode();
	void SetAcquireMode(PostgresPoolAcquireMode mode);

	static idx_t DefaultPoolSize();
	static std::string DefaultHealthCheckQuery();
	static PostgresPoolAcquireMode AcquireModeFromString(const std::string &mode_str);
	static std::string AcquireModeToString(PostgresPoolAcquireMode mode);
	static void ValidatePoolAcquireMode(ClientContext &context, SetScope scope, Value &parameter);

protected:
	std::unique_ptr<PostgresConnection> CreateNewConnection() override;
	bool CheckConnectionHealthy(PostgresConnection &conn) override;
	void ResetConnection(PostgresConnection &conn) override;

private:
	PostgresCatalog &postgres_catalog;

	std::mutex config_mutex;
	std::string health_check_query;
	PostgresPoolAcquireMode acquire_mode = PostgresPoolAcquireMode::FORCE;

	bool PoolEnabled();
};

class PostgresConfigurePoolFunction : public TableFunction {
public:
	PostgresConfigurePoolFunction();
};

} // namespace duckdb
