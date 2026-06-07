//===----------------------------------------------------------------------===//
//                         DuckDB
//
// postgres_filter_pushdown.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/table_filter_set.hpp"

namespace duckdb {

class PostgresFilterPushdown {
public:
	static string TransformFilters(const vector<column_t> &column_ids, optional_ptr<TableFilterSet> filters,
	                               const vector<string> &names);

private:
	// TODO
	// static string TransformCTIDLiteral(const Value &val);
};

} // namespace duckdb
