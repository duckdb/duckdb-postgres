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
#include "postgres_connection.hpp"

namespace duckdb {
class PostgresCatalog;
class PostgresConnectionPool;

class PostgresPoolConnection {
public:
	PostgresPoolConnection();
	PostgresPoolConnection(optional_ptr<PostgresConnectionPool> pool, PostgresConnection connection);
	~PostgresPoolConnection();
	// disable copy constructors
	PostgresPoolConnection(const PostgresPoolConnection &other) = delete;
	PostgresPoolConnection &operator=(const PostgresPoolConnection &) = delete;
	//! enable move constructors
	PostgresPoolConnection(PostgresPoolConnection &&other) noexcept;
	PostgresPoolConnection &operator=(PostgresPoolConnection &&) noexcept;

	bool HasConnection();
	PostgresConnection &GetConnection();

private:
	optional_ptr<PostgresConnectionPool> pool;
	PostgresConnection connection;
};

class PostgresConnectionPool {
public:
	static constexpr const idx_t DEFAULT_MAX_CONNECTIONS = 64;

	PostgresConnectionPool(PostgresCatalog &postgres_catalog, idx_t maximum_connections = DEFAULT_MAX_CONNECTIONS);

public:
	bool TryGetConnection(PostgresPoolConnection &connection, optional_ptr<ClientContext> context = nullptr);
	PostgresPoolConnection GetConnection(optional_ptr<ClientContext> context = nullptr);
	//! Always returns a connection - even if the connection slots are exhausted
	PostgresPoolConnection ForceGetConnection(optional_ptr<ClientContext> context = nullptr);
	void ReturnConnection(PostgresConnection connection);
	void SetMaximumConnections(idx_t new_max);

	static void PostgresSetConnectionCache(ClientContext &context, SetScope scope, Value &parameter);

private:
	PostgresCatalog &postgres_catalog;
	mutex connection_lock;
	idx_t active_connections;
	idx_t maximum_connections;
	vector<PostgresConnection> connection_cache;

private:
	PostgresPoolConnection GetConnectionInternal(optional_ptr<ClientContext> context = nullptr);
};

} // namespace duckdb
