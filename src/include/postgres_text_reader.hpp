//===----------------------------------------------------------------------===//
//                         DuckDB
//
// postgres_text_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/vector.hpp"
#include "libpq-fe.h"
#include "postgres_conversion.hpp"
#include "postgres_connection.hpp"
#include <cstring>

namespace duckdb {

struct PostgresTextReader {
	explicit PostgresTextReader(PostgresConnection &con_p) : con(con_p), result(nullptr), col_vec(LogicalType::VARCHAR) {
	}
	
	~PostgresTextReader() {
		Reset();
	}
	
	PostgresConnection &GetConn() {
		return con;
	}

	void ReadTextFrom(const string &query) {
		Reset();
		result = PQexec(con.GetConn(), query.c_str());
		if (!result || PQresultStatus(result) != PGRES_TUPLES_OK ) {
			throw IOException("Failed to execute query: %s", string(PQerrorMessage(con.GetConn())));
		}
	}
	
	void Reset() {
		if (result) {
			PQclear(result);
			result = nullptr;
		}
	}
	
	void ReadColumn(idx_t col_idx) {
		col_vec.Resize(0, RowCount());
		for (idx_t row_idx = 0; row_idx < RowCount(); row_idx++) {
			if (PQgetisnull(result, row_idx, col_idx)) {
				FlatVector::SetNull(col_vec, row_idx, true);
				continue;
			}
			char *value = PQgetvalue(result, row_idx, col_idx);
			int value_len = PQgetlength(result, row_idx, col_idx);
			FlatVector::GetData<string_t>(col_vec)[row_idx] = StringVector::AddStringOrBlob(col_vec, value, value_len);
		}
	}
	
	void LoadResultTo (idx_t &col_idx, Vector &out_vec) {
		ReadColumn(col_idx);
		VectorOperations::DefaultCast(col_vec, out_vec, RowCount());
	}

	// Get result metadata
	idx_t ColumnCount() {
		return PQnfields(result);
	}
	
	idx_t RowCount() {
		return PQntuples(result);
	}

private:
	PostgresConnection &con;
	PGresult *result;
	Vector col_vec;
};

} // namespace duckdb