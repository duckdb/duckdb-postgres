//===----------------------------------------------------------------------===//
//                         DuckDB
//
// postgres_filter_pushdown.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"

namespace duckdb {

class PostgresFilterPushdown {
public:
	static string TransformFilters(const vector<column_t> &column_ids, optional_ptr<TableFilterSet> filters,
	                               const vector<string> &names);

private:
	static string TransformCTIDLiteral(const Value &val);
	static string TransformConstantFilter(string &column_name, ConstantFilter &filter, column_t column_id);
	static string TransformFilter(string &column_name, TableFilter &filter, column_t column_id);
	static string TransformComparison(ExpressionType type);
	static string CreateExpression(string &column_name, vector<unique_ptr<TableFilter>> &filters, string op,
	                               column_t column_id);
};

} // namespace duckdb
