//===----------------------------------------------------------------------===//
//                         DuckDB
//
// postgres_text_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/vector.hpp"
#include "libpq-fe.h"
#include "postgres_conversion.hpp"
#include "postgres_connection.hpp"
#include "postgres_utils.hpp"
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
	
	void ReadColumn(const PostgresType &pg_type, idx_t col_idx) {
		col_vec.Resize(0, RowCount());
		col_vec.Initialize(true);
		for (idx_t row_idx = 0; row_idx < RowCount(); row_idx++) {
			if (PQgetisnull(result, row_idx, col_idx)) {
				FlatVector::SetNull(col_vec, row_idx, true);
				continue;
			}
			char *value = PQgetvalue(result, row_idx, col_idx);
			int value_len = PQgetlength(result, row_idx, col_idx);
			FlatVector::SetNull(col_vec, row_idx, false);
			switch (pg_type.info) {
				case PostgresTypeAnnotation::FIXED_LENGTH_CHAR: {
					// CHAR column - remove trailing spaces
					while (value_len > 0 && value[value_len - 1] == ' ') {
						value_len--;
					}
					FlatVector::GetData<string_t>(col_vec)[row_idx] = StringVector::AddStringOrBlob(col_vec, value, value_len);
					break;
				}

				default: 
					FlatVector::GetData<string_t>(col_vec)[row_idx] = StringVector::AddStringOrBlob(col_vec, value, value_len);
					break;
			} 

		}
	}
	
	void LoadResultTo (const LogicalType &type, const PostgresType &pg_type, Vector &out_vec, idx_t &col_idx) {
		switch (type.id()) {
			case LogicalTypeId::LIST:
			case LogicalTypeId::ENUM:
				throw NotImplementedException("Type %s doesn't support straight casting from string", type.ToString());

			default:
				break;
		}
		ReadColumn(pg_type, col_idx);
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