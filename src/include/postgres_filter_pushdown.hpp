//===----------------------------------------------------------------------===//
//                         DuckDB
//
// postgres_filter_pushdown.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/table_filter_set.hpp"
#include "postgres_version.hpp"

namespace duckdb {

class PostgresFilterPushdown {
public:
	static bool CanPushExpressionDown(ClientContext &, const LogicalGet &, Expression &expr);

	static string TransformFilters(const vector<column_t> &column_ids, optional_ptr<TableFilterSet> filters,
	                               const vector<string> &names, PostgresInstanceType instance_type);

private:
	// TODO
	// static string TransformCTIDLiteral(const Value &val);
};

} // namespace duckdb
