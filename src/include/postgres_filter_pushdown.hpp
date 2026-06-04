//===----------------------------------------------------------------------===//
//                         DuckDB
//
// postgres_filter_pushdown.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/table_filter_set.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"

namespace duckdb {

class PostgresFilterPushdown {
public:
	static string TransformFilters(const vector<column_t> &column_ids, optional_ptr<TableFilterSet> filters,
	                               const vector<string> &names);

private:
	static string TransformCTIDLiteral(const Value &val);
	static string TransformConstantFilter(const string &column_name, ExpressionType comparison_type,
	                                      const Value &constant, column_t column_id);
	static string TransformFilter(const string &column_name, const TableFilter &filter, column_t column_id);
	static string TransformExpression(const string &column_name, const Expression &expr, column_t column_id);
	static string TransformExpressionSubject(const string &column_name, const Expression &expr);
	static string TransformComparison(ExpressionType type);
	static string CreateExpression(const string &column_name, const vector<unique_ptr<Expression>> &filters, string op,
	                               column_t column_id);
};

} // namespace duckdb
