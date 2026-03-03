//===----------------------------------------------------------------------===//
//                         DuckDB
//
// postgres_parameters.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include <libpq-fe.h>
#include "postgres_version.hpp"

namespace duckdb {

class PostgresParameters {
	vector<Oid> types;
	vector<Value> values;
	vector<vector<char>> copied_values;
	vector<const char *> value_ptrs;
	vector<int> lengths;
	vector<int> formats;

public:
	PostgresParameters() {
	}

	PostgresParameters(vector<Oid> types_p, vector<Value> values_p);

	bool Empty() const {
		return types.empty();
	}

	int Count() const {
		return static_cast<int>(types.size());
	}

	const Oid *Types() const {
		return types.data();
	}

	const char *const *Values() const {
		return value_ptrs.data();
	}

	const int *Lengths() const {
		return lengths.data();
	}

	const int *Formats() const {
		return formats.data();
	}
};

} // namespace duckdb
