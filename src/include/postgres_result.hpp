//===----------------------------------------------------------------------===//
//                         DuckDB
//
// postgres_result.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "libpq-fe.h"
#include "postgres_utils.hpp"

namespace duckdb {

class PostgresResult {
public:
	PostgresResult(PGresult *res_p) : res(res_p) {
	}
	~PostgresResult() {
		if (res) {
			PQclear(res);
		}
	}
	PGresult *res = nullptr;

public:
	string GetString(idx_t row, idx_t col) {
		D_ASSERT(res);
		return string(GetValueInternal(row, col));
	}

	int32_t GetInt32(idx_t row, idx_t col) {
		return atoi(GetValueInternal(row, col));
	}
	int64_t GetInt64(idx_t row, idx_t col) {
		return atoll(GetValueInternal(row, col));
	}
	bool GetBool(idx_t row, idx_t col) {
		return strcmp(GetValueInternal(row, col), "t") == 0;
	}
	bool IsNull(idx_t row, idx_t col) {
		return PQgetisnull(res, row, col);
	}
	idx_t Count() {
		D_ASSERT(res);
		return PQntuples(res);
	}
	idx_t AffectedRows() {
		auto affected = PQcmdTuples(res);
		if (!affected) {
			throw InternalException("Postgres scanner - AffectedRows called but none were available");
		}
		return atoll(affected);
	}

private:
	char *GetValueInternal(idx_t row, idx_t col) {
		auto val = PQgetvalue(res, row, col);
		if (!val) {
			throw InternalException("Postgres scanner - failed to fetch value for row %llu col %llu", row, col);
		}
		return val;
	}
};

} // namespace duckdb
