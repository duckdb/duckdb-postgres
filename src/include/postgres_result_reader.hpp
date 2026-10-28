//===----------------------------------------------------------------------===//
//                         DuckDB
//
// postgres_result_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/types/interval.hpp"
#include "postgres_conversion.hpp"
#include "postgres_utils.hpp"
#include "postgres_result.hpp"

namespace duckdb {
class PostgresConnection;
class PostgresPreparedStatement;
struct PostgresBindData;

enum class PostgresReadResult { FINISHED, HAVE_MORE_TUPLES };

struct PostgresResultReader {
	explicit PostgresResultReader(PostgresConnection &con_p, const vector<column_t> &column_ids,
	                              const PostgresBindData &bind_data,
	                              optional_ptr<PostgresPreparedStatement> prepared_statement_p = nullptr)
	    : con(con_p), column_ids(column_ids), bind_data(bind_data), prepared_statement(prepared_statement_p) {
	}
	virtual ~PostgresResultReader() = default;

	PostgresConnection &GetConn() {
		return con;
	}

public:
	virtual void BeginCopy(ClientContext &context, const string &sql) = 0;
	virtual PostgresReadResult Read(DataChunk &result) = 0;

protected:
	//! Runs the query, executing the statement prepared in bind instead if it was not used yet and the query matches
	unique_ptr<PostgresResult> Query(ClientContext &context, const string &sql, int format);
	//! Closes the statement prepared in bind if this reader has it and it was not closed yet
	void DestroyPreparedStatement();

protected:
	PostgresConnection &con;
	const vector<column_t> &column_ids;
	const PostgresBindData &bind_data;
	//! Statement prepared on con, executed instead of the query if the query matches it
	optional_ptr<PostgresPreparedStatement> prepared_statement;
};

} // namespace duckdb
