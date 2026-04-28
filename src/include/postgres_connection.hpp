//===----------------------------------------------------------------------===//
//                         DuckDB
//
// postgres_connection.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "postgres_utils.hpp"
#include "postgres_parameters.hpp"
#include "postgres_result.hpp"
#include "duckdb/common/shared_ptr.hpp"

namespace duckdb {
class PostgresBinaryWriter;
class PostgresTextWriter;
struct PostgresBinaryReader;
class PostgresSchemaEntry;
class PostgresTableEntry;
class PostgresStatement;
class PostgresResult;
struct IndexInfo;

struct OwnedPostgresConnection {
	explicit OwnedPostgresConnection(PGconn *conn = nullptr);
	OwnedPostgresConnection(const OwnedPostgresConnection &) = delete;
	OwnedPostgresConnection &operator=(const OwnedPostgresConnection &) = delete;
	~OwnedPostgresConnection();

	PGconn *connection;
	mutex connection_lock;
	PostgresInstanceType instance_type = PostgresInstanceType::POSTGRES;
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
	static PostgresConnection Open(const string &dsn, const string &attach_path);
	void Execute(optional_ptr<ClientContext> context, const string &query,
	             const PostgresParameters &params = PostgresParameters());
	unique_ptr<PostgresResult> TryQuery(optional_ptr<ClientContext> context, const string &query,
	                                    optional_ptr<string> error_message = nullptr,
	                                    const PostgresParameters &params = PostgresParameters());
	unique_ptr<PostgresResult> Query(optional_ptr<ClientContext> context, const string &query,
	                                 const PostgresParameters &params = PostgresParameters());

	//! Submits a set of queries to be executed in the connection.
	vector<unique_ptr<PostgresResult>> ExecuteQueries(ClientContext &context, const string &queries);

	PostgresVersion GetPostgresVersion(ClientContext &context);

	vector<IndexInfo> GetIndexInfo(const string &table_name);

	void BeginCopyTo(ClientContext &context, PostgresCopyState &state, PostgresCopyFormat format,
	                 const string &schema_name, const string &table_name, const vector<string> &column_names);
	void CommitAndRestartCopy(ClientContext &context, PostgresCopyState &state, PostgresCopyFormat format,
	                          const string &schema_name, const string &table_name, const vector<string> &column_names);
	void CopyData(data_ptr_t buffer, idx_t size);
	void CopyData(PostgresBinaryWriter &writer);
	void CopyData(PostgresTextWriter &writer);
	void CopyChunk(ClientContext &context, PostgresCopyState &state, DataChunk &chunk, DataChunk &varchar_chunk);
	void FinishCopyTo(PostgresCopyState &state);

	void BeginCopyFrom(ClientContext &context, const string &query, ExecStatusType expected_result);

	bool IsOpen();
	void Close();
	bool PingServer(const std::string &health_check_query);
	void Reset(const std::string &health_check_query);

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
	PGresult *PQExecute(optional_ptr<ClientContext> context, const string &query,
	                    const PostgresParameters &params = PostgresParameters());

	shared_ptr<OwnedPostgresConnection> connection;
	string dsn;
};

} // namespace duckdb
