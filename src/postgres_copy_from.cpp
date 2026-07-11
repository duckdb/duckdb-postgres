#include "postgres_connection.hpp"
#include "duckdb/main/client_context.hpp"
#include "postgres_binary_reader.hpp"

namespace duckdb {

void PostgresConnection::BeginCopyFrom(ClientContext &context, const string &query, ExecStatusType expected_result) {
	PostgresResult pg_res(PQExecute(context, query.c_str()));
	auto result = pg_res.res;
	if (!result || PQresultStatus(result) != expected_result) {
		throw std::runtime_error("Failed to prepare COPY \"" + query + "\": " + string(PQresultErrorMessage(result)));
	}
}

} // namespace duckdb
