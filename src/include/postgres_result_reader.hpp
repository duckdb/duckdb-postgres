//===----------------------------------------------------------------------===//
//                         DuckDB
//
// postgres_result_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/common/types/interval.hpp"
#include "postgres_conversion.hpp"
#include "postgres_utils.hpp"

namespace duckdb {
class PostgresConnection;
struct PostgresBindData;

enum class PostgresReadResult { FINISHED, HAVE_MORE_TUPLES };

struct PostgresResultReader {
	explicit PostgresResultReader(PostgresConnection &con_p, const vector<column_t> &column_ids,
	                              const PostgresBindData &bind_data)
	    : con(con_p), column_ids(column_ids), bind_data(bind_data) {
	}
	virtual ~PostgresResultReader() = default;

	PostgresConnection &GetConn() {
		return con;
	}

public:
	virtual void BeginCopy(const string &sql) = 0;
	virtual PostgresReadResult Read(DataChunk &result) = 0;

protected:
	PostgresConnection &con;
	const vector<column_t> &column_ids;
	const PostgresBindData &bind_data;
};

} // namespace duckdb
