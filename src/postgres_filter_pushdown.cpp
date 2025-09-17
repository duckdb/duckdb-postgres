#include "postgres_filter_pushdown.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/filter/struct_filter.hpp"
#include "duckdb/common/enum_util.hpp"

namespace duckdb {

string PostgresFilterPushdown::CreateExpression(string &column_name, vector<unique_ptr<TableFilter>> &filters,
                                                string op, column_t column_id) {
	vector<string> filter_entries;
	for (auto &filter : filters) {
		auto filter_str = TransformFilter(column_name, *filter, column_id);
		if (!filter_str.empty()) {
			filter_entries.push_back(std::move(filter_str));
		}
	}
	if (filter_entries.empty()) {
		return string();
	}
	return "(" + StringUtil::Join(filter_entries, " " + op + " ") + ")";
}

string PostgresFilterPushdown::TransformComparison(ExpressionType type) {
	switch (type) {
	case ExpressionType::COMPARE_EQUAL:
		return "=";
	case ExpressionType::COMPARE_NOTEQUAL:
		return "<>";
	case ExpressionType::COMPARE_LESSTHAN:
		return "<";
	case ExpressionType::COMPARE_GREATERTHAN:
		return ">";
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return "<=";
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return ">=";
	default:
		throw NotImplementedException("Unsupported expression type");
	}
}

string TransformBlob(const string &val) {
	char const HEX_DIGITS[] = "0123456789ABCDEF";

	string result = "'\\x";
	for (idx_t i = 0; i < val.size(); i++) {
		uint8_t byte_val = static_cast<uint8_t>(val[i]);
		result += HEX_DIGITS[(byte_val >> 4) & 0xf];
		result += HEX_DIGITS[byte_val & 0xf];
	}
	result += "'::BYTEA";
	return result;
}

string TransformLiteral(const Value &val) {
	switch (val.type().id()) {
	case LogicalTypeId::BLOB:
		return TransformBlob(StringValue::Get(val));
	default:
		return KeywordHelper::WriteQuoted(val.ToString());
	}
}

string PostgresFilterPushdown::TransformCTIDLiteral(const Value &constant) {
	throw InternalException("FIXME: transform ctid literal");
}

string PostgresFilterPushdown::TransformConstantFilter(string &column_name, ConstantFilter &constant_filter,
                                                       column_t column_id) {
	string constant_string;
	if (IsVirtualColumn(column_id)) {
		return "FALSE";
	} else {
		constant_string = TransformLiteral(constant_filter.constant);
	}
	auto operator_string = TransformComparison(constant_filter.comparison_type);
	return StringUtil::Format("%s %s %s", column_name, operator_string, constant_string);
}

string PostgresFilterPushdown::TransformFilter(string &column_name, TableFilter &filter, column_t column_id) {
	switch (filter.filter_type) {
	case TableFilterType::IS_NULL:
		return column_name + " IS NULL";
	case TableFilterType::IS_NOT_NULL:
		return column_name + " IS NOT NULL";
	case TableFilterType::CONJUNCTION_AND: {
		auto &conjunction_filter = filter.Cast<ConjunctionAndFilter>();
		return CreateExpression(column_name, conjunction_filter.child_filters, "AND", column_id);
	}
	case TableFilterType::CONJUNCTION_OR: {
		auto &conjunction_filter = filter.Cast<ConjunctionOrFilter>();
		return CreateExpression(column_name, conjunction_filter.child_filters, "OR", column_id);
	}
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &constant_filter = filter.Cast<ConstantFilter>();
		return TransformConstantFilter(column_name, constant_filter, column_id);
	}
	case TableFilterType::STRUCT_EXTRACT: {
		auto &struct_filter = filter.Cast<StructFilter>();
		auto child_name = KeywordHelper::WriteQuoted(struct_filter.child_name, '\"');
		auto new_name = "(" + column_name + ")." + child_name;
		return TransformFilter(new_name, *struct_filter.child_filter, column_id);
	}
	case TableFilterType::OPTIONAL_FILTER: {
		auto &optional_filter = filter.Cast<OptionalFilter>();
		return TransformFilter(column_name, *optional_filter.child_filter, column_id);
	}
	case TableFilterType::IN_FILTER: {
		auto &in_filter = filter.Cast<InFilter>();
		string in_list;
		for (auto &val : in_filter.values) {
			if (!in_list.empty()) {
				in_list += ", ";
			}
			in_list += TransformLiteral(val);
		}
		return column_name + " IN (" + in_list + ")";
	}
	case TableFilterType::DYNAMIC_FILTER:
		return string();
	default:
		throw InternalException("Unsupported table filter type");
	}
}

string PostgresFilterPushdown::TransformFilters(const vector<column_t> &column_ids,
                                                optional_ptr<TableFilterSet> filters, const vector<string> &names) {
	if (!filters || filters->filters.empty()) {
		// no filters
		return string();
	}
	string result;
	for (auto &entry : filters->filters) {
		string column_name;
		auto column_id = column_ids[entry.first];
		if (IsVirtualColumn(column_id)) {
			column_name = "ctid";
		} else {
			column_name = KeywordHelper::WriteQuoted(names[column_id], '"');
		}
		auto &filter = *entry.second;
		auto filter_text = TransformFilter(column_name, filter, column_id);

		if (filter_text.empty()) {
			continue;
		}
		if (!result.empty()) {
			result += " AND ";
		}
		result += filter_text;
	}
	return result;
}

} // namespace duckdb
