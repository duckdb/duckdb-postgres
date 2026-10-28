#include "postgres_result_reader.hpp"

#include "postgres_connection.hpp"
#include "postgres_prepared_statement.hpp"
#include "postgres_scanner.hpp"

namespace duckdb {

unique_ptr<PostgresResult> PostgresResultReader::Query(ClientContext &context, const string &sql, int format) {
	if (prepared_statement && !prepared_statement->IsDestroyed() && prepared_statement->GetQuery() == sql) {
		auto result = con.QueryStmt(context, *prepared_statement, bind_data.params, format);
		prepared_statement->Destroy(con);
		return result;
	}
	// the query was modified, the statement will not be used
	DestroyPreparedStatement();
	return con.Query(context, sql, bind_data.params, format);
}

void PostgresResultReader::DestroyPreparedStatement() {
	if (prepared_statement && !prepared_statement->IsDestroyed()) {
		prepared_statement->Destroy(con);
	}
}

} // namespace duckdb
