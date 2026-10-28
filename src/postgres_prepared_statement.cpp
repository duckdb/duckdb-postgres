#include "postgres_prepared_statement.hpp"
#include "postgres_connection.hpp"

#include <atomic>

namespace duckdb {

PostgresPreparedStatement::PostgresPreparedStatement(PGconn *conn, uint64_t pool_connection_id_p, string name_p,
                                                     string query_p, PGresult *prepare_result_p)
    : pool_connection_id(pool_connection_id_p), name(std::move(name_p)), query(std::move(query_p)),
      prepare_result(prepare_result_p) {
	if (!prepare_result.res) {
		const char *error = conn ? PQerrorMessage(conn) : "";
		throw BinderException("Failed to prepare query \"%s\" (no result returned): %s", query, error);
	}
	if (PQresultStatus(prepare_result.res) != PGRES_COMMAND_OK) {
		throw BinderException("Failed to prepare query \"%s\": %s", query, PQresultErrorMessage(prepare_result.res));
	}
}

string PostgresPreparedStatement::GenerateName() {
	// statement names only need to be unique within a Postgres session, a process-wide counter guarantees this
	static std::atomic<uint64_t> counter {0};
	return "duckdb_stmt_" + std::to_string(counter.fetch_add(1, std::memory_order_relaxed));
}

uint64_t PostgresPreparedStatement::GetPoolConnectionId() const {
	return pool_connection_id;
}

const string &PostgresPreparedStatement::GetName() const {
	return name;
}

const string &PostgresPreparedStatement::GetQuery() const {
	return query;
}

PGresult *PostgresPreparedStatement::GetPrepareResult() const {
	return prepare_result.res;
}

void PostgresPreparedStatement::Destroy(PostgresConnection &con) {
	if (destroyed) {
		return;
	}
	// mark the statement as destroyed even if closing fails, so it is never executed again
	destroyed = true;
	auto conn_lock = con.Lock();
	auto conn = con.GetConn();
	PostgresResult close_result(PQclosePrepared(conn, name.c_str()));
	if (!close_result.res || PQresultStatus(close_result.res) != PGRES_COMMAND_OK) {
		auto error = close_result.res ? PQresultErrorMessage(close_result.res) : PQerrorMessage(conn);
		throw std::runtime_error(
		    StringUtil::Format("Failed to close prepared statement \"%s\": %s", name, string(error)));
	}
}

bool PostgresPreparedStatement::IsDestroyed() const {
	return destroyed;
}

} // namespace duckdb
