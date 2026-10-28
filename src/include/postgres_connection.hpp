//===----------------------------------------------------------------------===//
//                         DuckDB
//
// postgres_connection.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "postgres_utils.hpp"
#include "duckdb/main/client_context.hpp"
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
class PostgresPreparedStatement;
struct IndexInfo;

class PostgresConnection {
public:
	explicit PostgresConnection(PGconn *connection = nullptr);
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
	             const PostgresParameters &params = PostgresParameters(), int format = 0);
	unique_ptr<PostgresResult> TryQuery(optional_ptr<ClientContext> context, const string &query,
	                                    optional_ptr<string> error_message = nullptr,
	                                    const PostgresParameters &params = PostgresParameters(), int format = 0);
	unique_ptr<PostgresResult> Query(optional_ptr<ClientContext> context, const string &query,
	                                 const PostgresParameters &params = PostgresParameters(), int format = 0);
	//! Executes a statement that was prepared on this connection
	unique_ptr<PostgresResult> QueryStmt(optional_ptr<ClientContext> context, const PostgresPreparedStatement &stmt,
	                                     const PostgresParameters &params = PostgresParameters(), int format = 0);

	//! Submits a set of queries to be executed in the connection.
	vector<unique_ptr<PostgresResult>> ExecuteQueries(ClientContext &context, const string &queries);

	PostgresVersion GetPostgresVersion(ClientContext &context);

	vector<IndexInfo> GetIndexInfo(const string &table_name);

	void BeginCopyTo(ClientContext &context, PostgresCopyState &state, PostgresCopyFormat format,
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

	string GetDSN() {
		return dsn;
	}

	//! Locks the connection, must be held when calling libpq functions on the result of GetConn()
	unique_lock<mutex> Lock() {
		return unique_lock<mutex>(connection_lock);
	}
	PGconn *GetConn() {
		if (!connection) {
			throw InternalException("PostgresConnection::GetConn - no connection available");
		}
		return connection;
	}

	static void DebugSetPrintQueries(bool print);
	static bool DebugPrintQueries();

private:
	//! Requires connection_lock to be held
	PGresult *PQExecute(optional_ptr<ClientContext> context, const string &query,
	                    const PostgresParameters &params = PostgresParameters(), int format = 0);
	//! Requires connection_lock to be held
	bool PingServerInternal(const std::string &health_check_query);

	PGconn *connection = nullptr;
	mutex connection_lock;
	string dsn;
};

} // namespace duckdb
