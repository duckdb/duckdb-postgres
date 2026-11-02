#include "postgres_filter_pushdown.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/string_util.hpp"

#include "dbconnector/table_scan/filter_pushdown.hpp"
#include "dbconnector/table_scan/filter_util.hpp"

namespace duckdb {

static string WriteDistinctFrom(ExpressionType distinct_type, const string &column_name,
                                const string &constant_string) {
	switch (distinct_type) {
	case ExpressionType::COMPARE_DISTINCT_FROM:
		return StringUtil::Format("%s %s %s", column_name, "IS DISTINCT FROM", constant_string);
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		return StringUtil::Format("%s %s %s", column_name, "IS NOT DISTINCT FROM", constant_string);
	default:
		throw InvalidInputException("Unsupported DISTINCT FROM comparion type: %s", EnumUtil::ToString(distinct_type));
	}
}

static dbconnector::table_scan::FilterPushdown::Config
CreatePostgresConfig(PostgresInstanceType instance_type = PostgresInstanceType::POSTGRES) {
	using namespace dbconnector;

	if (instance_type == PostgresInstanceType::REDSHIFT) {
		// Redshift does not understand the '\x...' bytea hex format, '\x41'::BYTEA is read as the bytes "x41"
		return table_scan::FilterPushdown::CreateConfig('"', '\'', query::QuoteEscapeStyle::DOUBLE_QUOTE, "FROM_HEX('",
		                                                "')", "C", WriteDistinctFrom);
	}
	return table_scan::FilterPushdown::CreateConfig('"', '\'', query::QuoteEscapeStyle::DOUBLE_QUOTE, "'\\x",
	                                                "'::BYTEA", "C", WriteDistinctFrom);
}

bool PostgresFilterPushdown::CanPushExpressionDown(ClientContext &, const LogicalGet &, Expression &expr) {
	using dbconnector::table_scan::FilterPushdown;

	auto config = CreatePostgresConfig();
	string filter = FilterPushdown::TransformFilterExpression(config, "dummy", expr);
	return !filter.empty();
}

string PostgresFilterPushdown::TransformFilters(const vector<column_t> &column_ids,
                                                optional_ptr<TableFilterSet> filters, const vector<string> &names,
                                                PostgresInstanceType instance_type) {
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
		auto config = CreatePostgresConfig(instance_type);
		auto filter_text = table_scan::FilterPushdown::TransformFilter(config, column_name, filter, column_id);

		if (filter_text.empty()) {
			if (table_scan::FilterUtil::IsInternalFilter(filter)) {
				continue;
			}
			throw NotImplementedException(
			    "Unsupported filter pushdown, use 'pg_enable_filter_pushdown=FALSE' to disable pushdowns."
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
