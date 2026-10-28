#include "postgres_connection.hpp"

#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/helper.hpp"

#include "postgres_logging.hpp"
#include "postgres_prepared_statement.hpp"

namespace duckdb {

static bool debug_postgres_print_queries = false;

PostgresConnection::PostgresConnection(PGconn *connection_p) : connection(connection_p) {
}

PostgresConnection::~PostgresConnection() {
	Close();
}

PostgresConnection::PostgresConnection(PostgresConnection &&other) noexcept {
	lock_guard<mutex> guard(other.connection_lock);
	std::swap(connection, other.connection);
	std::swap(dsn, other.dsn);
}

PostgresConnection &PostgresConnection::operator=(PostgresConnection &&other) noexcept {
	if (this == &other) {
		return *this;
	}
	std::lock(connection_lock, other.connection_lock);
	lock_guard<mutex> guard(connection_lock, std::adopt_lock);
	lock_guard<mutex> other_guard(other.connection_lock, std::adopt_lock);
	std::swap(connection, other.connection);
	std::swap(dsn, other.dsn);
	return *this;
}

PostgresConnection PostgresConnection::Open(const string &dsn, const string &attach_path) {
	PostgresConnection result;
	result.connection = PostgresUtils::PGConnect(dsn, attach_path);
	result.dsn = dsn;
	return result;
}

static bool ResultHasError(PGresult *result) {
	if (!result) {
		return true;
	}
	switch (PQresultStatus(result)) {
	case PGRES_COMMAND_OK:
	case PGRES_TUPLES_OK:
		return false;
	default:
		return true;
	}
}

PGresult *PostgresConnection::PQExecute(optional_ptr<ClientContext> context, const string &query,
                                        const PostgresParameters &params, int format) {
	if (PostgresConnection::DebugPrintQueries()) {
		Printer::Print(query + "\n");
	}
	int64_t start_time = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now())
	                         .time_since_epoch()
	                         .count();

	PGconn *conn = GetConn();
	PGresult *res = nullptr;

	if (params.Empty() && format == 0) {
		res = PQexec(GetConn(), query.c_str());
	} else {
		// Unlike PQexec, PQexecParams allows at most one SQL command in the given string.
		res = PQexecParams(conn, query.c_str(), params.Count(), params.Types(), params.Values(), params.Lengths(),
		                   params.Formats(), format);
	}

	int64_t end_time = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now())
	                       .time_since_epoch()
	                       .count();
	if (context) {
		DUCKDB_LOG(*context, PostgresQueryLogType, query, end_time - start_time);
	}
	return res;
}

unique_ptr<PostgresResult> PostgresConnection::TryQuery(optional_ptr<ClientContext> context, const string &query,
                                                        optional_ptr<string> error_message,
                                                        const PostgresParameters &params, int format) {
	lock_guard<mutex> guard(connection_lock);
	auto result = PQExecute(context, query.c_str(), params, format);

	if (ResultHasError(result)) {
		if (error_message) {
			auto error = result ? PQresultErrorMessage(result) : PQerrorMessage(GetConn());
			*error_message = StringUtil::Format("Failed to execute query \"" + query + "\": " + string(error));
		}
		PQclear(result);
		return nullptr;
	}
	return make_uniq<PostgresResult>(result);
}

unique_ptr<PostgresResult> PostgresConnection::Query(optional_ptr<ClientContext> context, const string &query,
                                                     const PostgresParameters &params, int format) {
	string error_msg;
	auto result = TryQuery(context, query, &error_msg, params, format);
	if (!result) {
		throw std::runtime_error(error_msg);
	}
	return result;
}

unique_ptr<PostgresResult> PostgresConnection::QueryStmt(optional_ptr<ClientContext> context,
                                                         const PostgresPreparedStatement &stmt,
                                                         const PostgresParameters &params, int format) {
	auto &query = stmt.GetQuery();
	if (PostgresConnection::DebugPrintQueries()) {
		Printer::Print(query + "\n");
	}
	int64_t start_time = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now())
	                         .time_since_epoch()
	                         .count();

	lock_guard<mutex> guard(connection_lock);
	PGconn *conn = GetConn();
	PGresult *result = PQexecPrepared(conn, stmt.GetName().c_str(), params.Count(), params.Values(), params.Lengths(),
	                                  params.Formats(), format);

	int64_t end_time = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now())
	                       .time_since_epoch()
	                       .count();
	if (context) {
		DUCKDB_LOG(*context, PostgresQueryLogType, query, end_time - start_time);
	}

	if (ResultHasError(result)) {
		auto error = result ? PQresultErrorMessage(result) : PQerrorMessage(conn);
		auto error_message = StringUtil::Format("Failed to execute prepared statement \"%s\" for query \"%s\": %s",
		                                        stmt.GetName(), query, string(error));
		PQclear(result);
		throw std::runtime_error(error_message);
	}
	return make_uniq<PostgresResult>(result);
}

void PostgresConnection::Execute(optional_ptr<ClientContext> context, const string &query,
                                 const PostgresParameters &params, int format) {
	Query(context, query, params, format);
}

vector<unique_ptr<PostgresResult>> PostgresConnection::ExecuteQueries(ClientContext &context, const string &queries) {
	if (PostgresConnection::DebugPrintQueries()) {
		Printer::Print(queries + "\n");
	}
	int64_t start_time = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now())
	                         .time_since_epoch()
	                         .count();
	lock_guard<mutex> guard(connection_lock);
	auto res = PQsendQuery(GetConn(), queries.c_str());
	if (res == 0) {
		throw std::runtime_error("Failed to execute query \"" + queries + "\": " + string(PQerrorMessage(GetConn())));
	}
	vector<unique_ptr<PostgresResult>> results;
	string error_message;
	while (true) {
		auto res = PQgetResult(GetConn());
		if (!res) {
			break;
		}
		auto result = make_uniq<PostgresResult>(res);
		if (ResultHasError(res)) {
			if (error_message.empty()) {
				error_message = "Failed to execute query \"" + queries + "\": " + string(PQresultErrorMessage(res));
			}
			continue;
		}
		if (PQresultStatus(res) != PGRES_TUPLES_OK) {
			continue;
		}
		results.push_back(std::move(result));
	}
	if (!error_message.empty()) {
		throw std::runtime_error(error_message);
	}
	int64_t end_time = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now())
	                       .time_since_epoch()
	                       .count();
	DUCKDB_LOG(context, PostgresQueryLogType, queries, end_time - start_time);
	return results;
}

PostgresVersion PostgresConnection::GetPostgresVersion(ClientContext &context) {
	auto result = TryQuery(context, "SELECT version(), (SELECT COUNT(*) FROM pg_settings WHERE name LIKE 'rds%')");
	if (!result) {
		PostgresVersion version;
		version.type_v = PostgresInstanceType::UNKNOWN;
		return version;
	}
	auto pg_version_string = result->GetString(0, 0);
	auto version = PostgresUtils::ExtractPostgresVersion(pg_version_string);
	if (result->GetInt64(0, 1) > 0) {
		version.type_v = PostgresInstanceType::AURORA;
	}
	if (StringUtil::Contains(pg_version_string, "Redshift")) {
		version.type_v = PostgresInstanceType::REDSHIFT;
	}
	return version;
}

bool PostgresConnection::IsOpen() {
	return connection != nullptr;
}

void PostgresConnection::Close() {
	lock_guard<mutex> guard(connection_lock);
	if (!connection) {
		return;
	}
	PQfinish(connection);
	connection = nullptr;
}

bool PostgresConnection::PingServer(const std::string &health_check_query) {
	lock_guard<mutex> guard(connection_lock);
	return PingServerInternal(health_check_query);
}

bool PostgresConnection::PingServerInternal(const std::string &health_check_query) {
	if (!IsOpen()) {
		return false;
	}
	PGconn *conn = GetConn();
	if (PQstatus(conn) != CONNECTION_OK) {
		return false;
	}
	if (health_check_query.empty()) {
		return true;
	}
	PGresult *res = PQexec(conn, health_check_query.c_str());
	PostgresResult res_holder(res);
	return PQresultStatus(res) == PGRES_TUPLES_OK;
}

void PostgresConnection::Reset(const std::string &health_check_query) {
	lock_guard<mutex> guard(connection_lock);
	if (!IsOpen()) {
		throw InternalException("Cannot reset a connection that is not open");
	}
	PGconn *conn = GetConn();
	auto tx_status = PQtransactionStatus(conn);
	if (tx_status == PQTRANS_INTRANS || tx_status == PQTRANS_INERROR) {
		PGresult *res = PQexec(conn, "ROLLBACK");
		PostgresResult res_holder(res);
	}
	{
		PGresult *res = PQexec(conn, "DISCARD ALL");
		PostgresResult res_holder(res);
		if (PQresultStatus(res) == PGRES_COMMAND_OK) {
			return;
		}
	}
	PQreset(conn);
	if (!PingServerInternal(health_check_query)) {
		throw InternalException("Connection reset failure");
	}
}

vector<IndexInfo> PostgresConnection::GetIndexInfo(const string &table_name) {
	return vector<IndexInfo>();
}

void PostgresConnection::DebugSetPrintQueries(bool print) {
	debug_postgres_print_queries = print;
}

bool PostgresConnection::DebugPrintQueries() {
	return debug_postgres_print_queries;
}

} // namespace duckdb
