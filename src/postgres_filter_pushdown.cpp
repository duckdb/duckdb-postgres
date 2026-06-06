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

#include "postgres_utils.hpp"

namespace duckdb {

string PostgresFilterPushdown::CreateExpression(const string &column_name,
                                                const vector<unique_ptr<Expression>> &filters, string op,
                                                column_t column_id) {
	vector<string> filter_entries;
	for (auto &filter : filters) {
		auto filter_str = TransformExpression(column_name, *filter, column_id);
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
		return PostgresUtils::WriteLiteral(val.ToString());
	}
}

string PostgresFilterPushdown::TransformCTIDLiteral(const Value &constant) {
	throw InternalException("FIXME: transform ctid literal");
}

string PostgresFilterPushdown::TransformConstantFilter(const string &column_name, ExpressionType comparison_type,
                                                       const Value &constant, column_t column_id) {
	string constant_string;
	if (IsVirtualColumn(column_id)) {
		return "FALSE";
	} else {
		constant_string = TransformLiteral(constant);
	}
	auto operator_string = TransformComparison(comparison_type);
	return StringUtil::Format("%s %s %s", column_name, operator_string, constant_string);
}

string PostgresFilterPushdown::TransformExpressionSubject(const string &column_name, const Expression &expr) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_REF:
	case ExpressionClass::BOUND_COLUMN_REF:
		return column_name;
	case ExpressionClass::BOUND_FUNCTION: {
		auto &func = expr.Cast<BoundFunctionExpression>();
		idx_t child_idx;
		if (!TryGetStructExtractChildIndex(func, child_idx) || func.GetChildren().empty()) {
			return string();
		}
		auto parent_name = TransformExpressionSubject(column_name, *func.GetChildren()[0]);
		if (parent_name.empty()) {
			return string();
		}
		auto &struct_type = func.GetChildren()[0]->GetReturnType();
		if (struct_type.id() != LogicalTypeId::STRUCT || StructType::IsUnnamed(struct_type)) {
			return string();
		}
		auto child_name = PostgresUtils::WriteIdentifier(StructType::GetChildName(struct_type, child_idx));
		return "(" + parent_name + ")." + child_name;
	}
	default:
		return string();
	}
}

string PostgresFilterPushdown::TransformExpression(const string &column_name, const Expression &expr,
                                                   column_t column_id) {
	if (BoundComparisonExpression::IsComparison(expr)) {
		auto &comparison = expr.Cast<BoundFunctionExpression>();
		auto comparison_type = comparison.GetExpressionType();
		auto &left = BoundComparisonExpression::Left(comparison);
		auto &right = BoundComparisonExpression::Right(comparison);
		auto subject = TransformExpressionSubject(column_name, left);
		const Value *constant = nullptr;
		if (!subject.empty() && right.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
			constant = &right.Cast<BoundConstantExpression>().GetValue();
		} else {
			subject = TransformExpressionSubject(column_name, right);
			if (!subject.empty() && left.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
				constant = &left.Cast<BoundConstantExpression>().GetValue();
				comparison_type = FlipComparisonExpression(comparison_type);
			}
		}
		if (!constant || subject.empty()) {
			return string();
		}
		return TransformConstantFilter(subject, comparison_type, *constant, column_id);
	}

	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_CONJUNCTION: {
		auto &conjunction = expr.Cast<BoundConjunctionExpression>();
		switch (conjunction.GetExpressionType()) {
		case ExpressionType::CONJUNCTION_AND:
			return CreateExpression(column_name, conjunction.GetChildren(), "AND", column_id);
		case ExpressionType::CONJUNCTION_OR:
			return CreateExpression(column_name, conjunction.GetChildren(), "OR", column_id);
		default:
			return string();
		}
	}
	case ExpressionClass::BOUND_OPERATOR: {
		auto &op = expr.Cast<BoundOperatorExpression>();
		auto subject =
		    op.GetChildren().empty() ? string() : TransformExpressionSubject(column_name, *op.GetChildren()[0]);
		switch (op.GetExpressionType()) {
		case ExpressionType::OPERATOR_IS_NULL:
			return !subject.empty() ? subject + " IS NULL" : string();
		case ExpressionType::OPERATOR_IS_NOT_NULL:
			return !subject.empty() ? subject + " IS NOT NULL" : string();
		case ExpressionType::COMPARE_IN: {
			if (subject.empty()) {
				return string();
			}
			string in_list;
			for (idx_t i = 1; i < op.GetChildren().size(); i++) {
				if (op.GetChildren()[i]->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
					return string();
				}
				if (!in_list.empty()) {
					in_list += ", ";
				}
				auto &constant = op.GetChildren()[i]->Cast<BoundConstantExpression>().GetValue();
				in_list += IsVirtualColumn(column_id) ? "FALSE" : TransformLiteral(constant);
			}
			return IsVirtualColumn(column_id) ? "FALSE" : subject + " IN (" + in_list + ")";
		}
		default:
			return string();
		}
	}
	case ExpressionClass::BOUND_FUNCTION: {
		auto &func = expr.Cast<BoundFunctionExpression>();
		if (func.Function().GetName() == OptionalFilterScalarFun::NAME && func.BindInfo()) {
			auto &data = func.BindInfo()->Cast<OptionalFilterFunctionData>();
			return data.child_filter_expr ? TransformExpression(column_name, *data.child_filter_expr, column_id)
			                              : string();
		}
		if (func.Function().GetName() == SelectivityOptionalFilterScalarFun::NAME && func.BindInfo()) {
			auto &data = func.BindInfo()->Cast<SelectivityOptionalFilterFunctionData>();
			return data.child_filter_expr ? TransformExpression(column_name, *data.child_filter_expr, column_id)
			                              : string();
		}
		if (func.Function().GetName() == DynamicFilterScalarFun::NAME) {
			return string();
		}
		return string();
	}
	default:
		throw InternalException("Unsupported table filter type");
	}
}

string PostgresFilterPushdown::TransformFilter(const string &column_name, const TableFilter &filter,
                                               column_t column_id) {
	auto &expr_filter = ExpressionFilter::GetExpressionFilter(filter, "PostgresFilterPushdown::TransformFilter");
	return TransformExpression(column_name, *expr_filter.expr, column_id);
}

string PostgresFilterPushdown::TransformFilters(const vector<column_t> &column_ids,
                                                optional_ptr<TableFilterSet> filters, const vector<string> &names) {
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
			column_name = PostgresUtils::WriteIdentifier(names[column_id]);
		}
		auto &filter = entry.Filter();
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
