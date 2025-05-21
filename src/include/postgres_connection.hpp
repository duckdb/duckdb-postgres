//===----------------------------------------------------------------------===//
//                         DuckDB
//
// postgres_connection.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "postgres_utils.hpp"
#include "postgres_result.hpp"
#include "duckdb/common/shared_ptr.hpp"

namespace duckdb {
class PostgresBinaryWriter;
class PostgresTextWriter;
struct PostgresBinaryReader;
struct PostgresTextReader;
class PostgresSchemaEntry;
class PostgresTableEntry;
class PostgresStatement;
class PostgresResult;
struct IndexInfo;

struct OwnedPostgresConnection {
	explicit OwnedPostgresConnection(PGconn *conn = nullptr);
	~OwnedPostgresConnection();

	PGconn *connection;
};

class PostgresConnection {
public:
	explicit PostgresConnection(shared_ptr<OwnedPostgresConnection> connection = nullptr);
	~PostgresConnection();
	// disable copy constructors
	PostgresConnection(const PostgresConnection &other) = delete;
	PostgresConnection &operator=(const PostgresConnection &) = delete;
	//! enable move constructors
	PostgresConnection(PostgresConnection &&other) noexcept;
	PostgresConnection &operator=(PostgresConnection &&) noexcept;

public:
	static PostgresConnection Open(const string &connection_string);
	void Execute(const string &query);
	unique_ptr<PostgresResult> TryQuery(const string &query, optional_ptr<string> error_message = nullptr);
	unique_ptr<PostgresResult> Query(const string &query);

	//! Submits a set of queries to be executed in the connection.
	vector<unique_ptr<PostgresResult>> ExecuteQueries(const string &queries);

	PostgresVersion GetPostgresVersion();

	vector<IndexInfo> GetIndexInfo(const string &table_name);

	void BeginCopyTo(ClientContext &context, PostgresCopyState &state, PostgresCopyFormat format,
	                 const string &schema_name, const string &table_name, const vector<string> &column_names);
	void CopyData(data_ptr_t buffer, idx_t size);
	void CopyData(PostgresBinaryWriter &writer);
	void CopyData(PostgresTextWriter &writer);
	void CopyChunk(ClientContext &context, PostgresCopyState &state, DataChunk &chunk, DataChunk &varchar_chunk);
	void FinishCopyTo(PostgresCopyState &state);

	void BeginCopyFrom(PostgresBinaryReader &reader, const string &query);
	void ReadTextFrom(PostgresTextReader &reader, const string &query);

	bool IsOpen();
	void Close();

	shared_ptr<OwnedPostgresConnection> GetConnection() {
		return connection;
	}
	string GetDSN() {
		return dsn;
	}

	PGconn *GetConn() {
		if (!connection || !connection->connection) {
			throw InternalException("PostgresConnection::GetConn - no connection available");
		}
		return connection->connection;
	}

	static void DebugSetPrintQueries(bool print);
	static bool DebugPrintQueries();

private:
	PGresult *PQExecute(const string &query);

	shared_ptr<OwnedPostgresConnection> connection;
	string dsn;
};

} // namespace duckdb
