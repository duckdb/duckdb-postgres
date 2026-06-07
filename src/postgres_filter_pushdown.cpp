#include "postgres_filter_pushdown.hpp"

#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/function/scalar/struct_utils.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/common/enum_util.hpp"

#include "dbconnector/table_scan/filter_pushdown.hpp"
#include "dbconnector/table_scan/filter_util.hpp"

#include "postgres_utils.hpp"

namespace duckdb {

string PostgresFilterPushdown::TransformFilters(const vector<column_t> &column_ids,
                                                optional_ptr<TableFilterSet> filters, const vector<string> &names) {
	using namespace dbconnector;
	if (!filters || !filters->HasFilters()) {
		// no filters
		return string();
	}
	string result;
	for (auto &entry : *filters) {
		string column_name;
		auto column_id = column_ids[entry.GetIndex()];
		if (IsVirtualColumn(column_id)) {
			column_name = "ctid";
		} else {
			column_name = names[column_id];
		}
		auto &filter = entry.Filter();
		auto config = table_scan::FilterPushdown::CreateConfig('"', '\'', query::QuoteEscapeStyle::DOUBLE_QUOTE, "'\\x",
		                                                       "::BYTEA");
		auto filter_text = table_scan::FilterPushdown::TransformFilter(config, column_name, filter, column_id);

		if (filter_text.empty()) {
			if (table_scan::FilterUtil::IsInternalFilter(filter)) {
				continue;
			}
			throw NotImplementedException(
			    "Unsupported filter pushdown, use 'pg_experimental_filter_pushdown=FALSE' to disable pushdowns."
			    " Problematic filter: \"%s\"",
			    table_scan::FilterUtil::ToString(filter));
		}
		if (!result.empty()) {
			result += " AND ";
		}
		result += filter_text;
	}
	return result;
}

} // namespace duckdb
