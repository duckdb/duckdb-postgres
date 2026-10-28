//===----------------------------------------------------------------------===//
//                         DuckDB
//
// postgres_prepared_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "postgres_result.hpp"

namespace duckdb {
class PostgresConnection;

//! Owns a prepared statement created with PQprepare: holds the statement name, the query text, the ID of the
//! pooled connection the statement was prepared on and the PQprepare result. The constructor throws if the
//! preparation failed.
//! TODO: unless Destroy() is called, the statement is not closed on the server when this object is destroyed, it
//! is leaked until the connection is closed.
class PostgresPreparedStatement {
public:
	PostgresPreparedStatement(PGconn *conn, uint64_t pool_connection_id_p, string name_p, string query_p,
	                          PGresult *prepare_result_p);

	PostgresPreparedStatement(const PostgresPreparedStatement &) = delete;
	PostgresPreparedStatement &operator=(const PostgresPreparedStatement &) = delete;

public:
	//! Generates a prepared statement name that is unique within the process
	static string GenerateName();

	uint64_t GetPoolConnectionId() const;
	const string &GetName() const;
	const string &GetQuery() const;
	PGresult *GetPrepareResult() const;

	//! Closes the statement on the server, con must be the connection the statement was prepared on
	void Destroy(PostgresConnection &con);
	bool IsDestroyed() const;

private:
	uint64_t pool_connection_id;
	string name;
	string query;
	PostgresResult prepare_result;
	bool destroyed = false;
};

} // namespace duckdb
