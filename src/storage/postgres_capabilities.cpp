#include "storage/postgres_capabilities.hpp"

#include "duckdb.hpp"
#include "duckdb/catalog/default/default_types.hpp"
#include "duckdb/common/logical_type_info.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/expression/window_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/query_node/delete_query_node.hpp"
#include "duckdb/parser/query_node/insert_query_node.hpp"
#include "duckdb/parser/query_node/merge_query_node.hpp"
#include "duckdb/parser/statement/merge_into_statement.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/query_node/update_query_node.hpp"
#include "duckdb/parser/statement/alter_statement.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/statement/drop_statement.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/parser/result_modifier.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/expressionlistref.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//
//! Get the value of a constant - folded constants can also be represented as a cast of a literal
bool PostgresCapabilities::TryGetConstantValue(const ParsedExpression &expr, Value &result) {
	if (expr.GetExpressionClass() == ExpressionClass::CONSTANT) {
		result = expr.Cast<ConstantExpression>().GetLiteral().ToValue();
		return true;
	}
	if (expr.GetExpressionClass() == ExpressionClass::CAST) {
		auto &cast_expr = expr.Cast<CastExpression>();
		if (cast_expr.IsTryCast() || cast_expr.Child().GetExpressionClass() != ExpressionClass::CONSTANT) {
			return false;
		}
		auto target_type = PostgresCapabilities::ResolveType(cast_expr.TargetType());
		if (target_type.id() == LogicalTypeId::INVALID) {
			return false;
		}
		auto value = cast_expr.Child().Cast<ConstantExpression>().GetLiteral().ToValue();
		auto cast_result = value.DefaultTryCastAs(target_type);
		if (!cast_result) {
			return false;
		}
		result = std::move(*cast_result);
		return true;
	}
	return false;
}

static bool PostgresTryGetConstantValue(const ParsedExpression &expr, Value &result) {
	return PostgresCapabilities::TryGetConstantValue(expr, result);
}

static bool PostgresTryGetIntegerLiteral(const ParsedExpression &expr, int64_t &result) {
	Value val;
	if (!PostgresTryGetConstantValue(expr, val)) {
		return false;
	}
	if (!val.type().IsIntegral() || val.IsNull()) {
		return false;
	}
	auto bigint_value = val.DefaultTryCastAs(LogicalType::BIGINT);
	if (!bigint_value || bigint_value->IsNull()) {
		return false;
	}
	result = BigIntValue::Get(*bigint_value);
	return true;
}

//! Whether an expression is an integer literal - in ORDER BY / GROUP BY these are positional references
static bool PostgresIsIntegerLiteral(const ParsedExpression &expr) {
	int64_t value;
	return expr.GetExpressionClass() == ExpressionClass::CONSTANT && PostgresTryGetIntegerLiteral(expr, value);
}

static bool PostgresIsIntegerLiteralAtLeast(const ParsedExpression &expr, int64_t minimum_value) {
	int64_t value;
	return PostgresTryGetIntegerLiteral(expr, value) && value >= minimum_value;
}

static bool PostgresIsStringLiteral(const ParsedExpression &expr) {
	Value val;
	if (!PostgresTryGetConstantValue(expr, val)) {
		return false;
	}
	return val.type().id() == LogicalTypeId::VARCHAR && !val.IsNull();
}

static bool PostgresSupportsValue(const Value &value) {
	return PostgresCapabilities::SupportsValue(value);
}

//! Years outside of [1, 9999] are formatted differently by DuckDB (negative years) and Postgres (BC)
static bool PostgresSupportsYear(int32_t year) {
	return year >= 1 && year <= 9999;
}

bool PostgresCapabilities::SupportsValue(const Value &value) {
	if (value.IsNull()) {
		return true;
	}
	switch (value.type().id()) {
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::UHUGEINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::TIME:
	case LogicalTypeId::INTERVAL:
	case LogicalTypeId::UUID:
		return true;
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BLOB: {
		// Postgres text values cannot hold NUL bytes
		auto &str = StringValue::Get(value);
		return value.type().id() == LogicalTypeId::BLOB || str.find('\0') == string::npos;
	}
	case LogicalTypeId::DATE: {
		auto date = DateValue::Get(value);
		if (!Value::IsFinite(date)) {
			return true;
		}
		return PostgresSupportsYear(Date::ExtractYear(date));
	}
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ: {
		auto ts = value.GetValue<timestamp_t>();
		if (!Value::IsFinite(ts)) {
			return true;
		}
		return PostgresSupportsYear(Date::ExtractYear(Timestamp::GetDate(ts)));
	}
	default:
		// nested types, enums, ... are not supported
		return false;
	}
}

LogicalType PostgresCapabilities::ResolveType(const TypeExpression &type_expr) {
	if (!type_expr.GetCatalog().empty() || !type_expr.GetSchema().empty()) {
		// a qualified (user-defined) type
		return LogicalType::INVALID;
	}
	auto type_id = DefaultTypeGenerator::GetDefaultType(type_expr.GetTypeName());
	auto &children = type_expr.GetChildren();
	switch (type_id) {
	case LogicalTypeId::INVALID:
		return LogicalType::INVALID;
	case LogicalTypeId::DECIMAL: {
		int64_t width = 18;
		int64_t scale = 3;
		if (children.size() > 2) {
			return LogicalType::INVALID;
		}
		if (!children.empty() && !PostgresTryGetIntegerLiteral(*children[0], width)) {
			return LogicalType::INVALID;
		}
		if (children.size() > 1 && !PostgresTryGetIntegerLiteral(*children[1], scale)) {
			return LogicalType::INVALID;
		}
		if (children.size() == 1) {
			scale = 0;
		}
		if (width < 1 || width > Decimal::MAX_WIDTH_DECIMAL || scale < 0 || scale > width) {
			return LogicalType::INVALID;
		}
		return LogicalType::DECIMAL(NumericCast<uint8_t>(width), NumericCast<uint8_t>(scale));
	}
	case LogicalTypeId::VARCHAR:
		// VARCHAR(n) does not truncate in DuckDB - the length modifier is ignored
		for (auto &child : children) {
			if (!PostgresIsIntegerLiteral(*child)) {
				return LogicalType::INVALID;
			}
		}
		return LogicalType::VARCHAR;
	default:
		if (!children.empty()) {
			return LogicalType::INVALID;
		}
		return LogicalType(type_id);
	}
}

//! Whether a function expression is a call to the built-in function with the given name, qualified with the
//! system catalog (as produced by ConstantExpression::FromValue)
static bool PostgresIsSystemFunction(const ParsedExpression &expr, const char *name, idx_t argument_count) {
	if (expr.GetExpressionClass() != ExpressionClass::FUNCTION) {
		return false;
	}
	auto &func = expr.Cast<FunctionExpression>();
	auto &qualified_name = func.GetQualifiedName();
	if (qualified_name.Catalog() != Identifier::SystemCatalog() || qualified_name.Schema() != DEFAULT_SCHEMA ||
	    qualified_name.Name() != name) {
		return false;
	}
	return func.GetArguments().size() == argument_count && !func.Distinct() && !func.Filter() &&
	       !func.GetArguments()[0].HasName();
}

static bool PostgresTryGetIntervalPart(const ParsedExpression &expr, const char *name, int64_t &result) {
	if (!PostgresIsSystemFunction(expr, name, 1)) {
		return false;
	}
	return PostgresTryGetIntegerLiteral(expr.Cast<FunctionExpression>().GetArguments()[0].GetExpression(), result);
}

bool PostgresCapabilities::TryGetIntervalLiteral(const ParsedExpression &expr, interval_t &result) {
	if (!PostgresIsSystemFunction(expr, "add", 2)) {
		return false;
	}
	auto &arguments = expr.Cast<FunctionExpression>().GetArguments();
	auto &months_and_days = arguments[0].GetExpression();
	if (!PostgresIsSystemFunction(months_and_days, "add", 2)) {
		return false;
	}
	auto &parts = months_and_days.Cast<FunctionExpression>().GetArguments();
	int64_t months, days, micros;
	if (!PostgresTryGetIntervalPart(parts[0].GetExpression(), "to_months", months) ||
	    !PostgresTryGetIntervalPart(parts[1].GetExpression(), "to_days", days) ||
	    !PostgresTryGetIntervalPart(arguments[1].GetExpression(), "to_microseconds", micros)) {
		return false;
	}
	result.months = NumericCast<int32_t>(months);
	result.days = NumericCast<int32_t>(days);
	result.micros = micros;
	return true;
}

bool PostgresCapabilities::SupportsCastType(const LogicalType &type) {
	// whether a cast is supported also depends on the source type - which is checked when serializing
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::UUID:
		return true;
	default:
		// Postgres has no unsigned / TINYINT types, intervals are formatted differently, ...
		return false;
	}
}

//===--------------------------------------------------------------------===//
// Functions
//===--------------------------------------------------------------------===//
//! Functions whose result depends on the string ordering
static bool PostgresIsOrderSensitiveFunction(const string &name) {
	static const case_insensitive_set_t ORDER_SENSITIVE = {"min",     "max",      "greatest",      "least",
	                                                       "median",  "quantile", "quantile_disc", "quantile_cont",
	                                                       "arg_min", "arg_max",  "min_by",        "max_by"};
	return ORDER_SENSITIVE.count(name) > 0;
}

//! Aggregate functions that can be pushed into Postgres
//! Aggregate functions that can be pushed into Postgres - also as window functions
static const case_insensitive_set_t &PostgresAggregateFunctions() {
	//   "count_star"          - serialized as count(*)
	//   "stddev"/"variance"   - sample variants, identical in Postgres
	//   "string_agg"/"group_concat"/"listagg" - serialized as string_agg with an explicit separator
	//   "list"                - serialized as array_agg
	//   "corr"/"covar_*"/"regr_*" - note: Postgres uses different algorithms, results can differ in the
	//                           last digits of floating point precision
	// Excluded:
	//   "mode"                - ties are broken differently
	//   "first"/"last"        - depend on the (undefined) order of rows
	//   "product"/"entropy"/"kurtosis"/"skewness"/"approx_*"/"histogram" - not available in Postgres
	static const case_insensitive_set_t AGGREGATES = {
	    "count",          "count_star",   "sum",         "avg",        "min",       "max",       "stddev",
	    "variance",       "stddev_pop",   "stddev_samp", "var_pop",    "var_samp",  "bool_and",  "bool_or",
	    "string_agg",     "group_concat", "listagg",     "array_agg",  "list",      "bit_and",   "bit_or",
	    "bit_xor",        "corr",         "covar_pop",   "covar_samp", "regr_avgx", "regr_avgy", "regr_count",
	    "regr_intercept", "regr_r2",      "regr_slope",  "regr_sxx",   "regr_sxy",  "regr_syy",
	};
	return AGGREGATES;
}

//! Aggregate functions that are rewritten into an expression, and can therefore not be used as window functions
static const case_insensitive_set_t &PostgresRewrittenAggregateFunctions() {
	//   "count_if"            - serialized as count(CASE WHEN x THEN 1 END)
	//   "median"/"quantile_*" - serialized as the percentile_cont / percentile_disc ordered-set aggregates
	//   "arg_min"/"arg_max"   - serialized as the first element of an ordered array_agg
	//   "any_value"           - available from Postgres 16
	static const case_insensitive_set_t AGGREGATES = {
	    "count_if", "countif", "median", "quantile", "quantile_cont", "quantile_disc",
	    "arg_min",  "arg_max", "min_by", "max_by",   "any_value",
	};
	return AGGREGATES;
}

static bool PostgresIsAggregateFunction(const string &name) {
	return PostgresAggregateFunctions().count(name) > 0 || PostgresRewrittenAggregateFunctions().count(name) > 0;
}

bool PostgresCapabilities::SupportsFunction(const FunctionExpression &func) {
	auto &function_name = func.FunctionName().GetIdentifierName();
	// Scalar functions that can be pushed into Postgres - either because name and semantics are
	// identical, or because PostgresSQLWriter rewrites them during serialization:
	//   "/"            - DuckDB's division always yields a DOUBLE, serialized with a float8 cast
	//   "~~"/"!~~"     - [NOT] LIKE, serialized with an empty ESCAPE clause (Postgres' default escape
	//                    character is backslash, DuckDB's LIKE has no default escape character)
	//   "len"          - serialized as length
	//   "strlen"       - serialized as octet_length
	//   "instr"/"position" - serialized as strpos
	//   "prefix"       - serialized as starts_with
	//   "ifnull"       - serialized as coalesce
	//   "year"/"month"/... - serialized as CAST(EXTRACT(<part> FROM x) AS BIGINT)
	// Excluded (verified mismatches that cannot be fixed by rewriting):
	//   "//"           - DuckDB's integer division on doubles does not floor
	//   "sqrt"/"ln"/"log"/"log2"/"log10"/"asin"/"acos"/"exp"/"pow"/"power"/"cot" - Postgres raises
	//                    errors on domain / range errors where DuckDB yields nan / inf
	//   "round"        - Postgres rounds doubles half-to-even, DuckDB half away from zero
	//   "reverse"      - DuckDB reverses grapheme clusters, Postgres code points
	//   "regexp_*"     - different regex dialects (RE2 vs Postgres ARE)
	static const case_insensitive_set_t SCALAR_FUNCTIONS = {
	    // operators
	    "+",
	    "-",
	    "*",
	    "/",
	    "%",
	    "||",
	    "~~",
	    "!~~",
	    // string
	    "concat",
	    "concat_ws",
	    "length",
	    "len",
	    "char_length",
	    "character_length",
	    "strlen",
	    "octet_length",
	    "bit_length",
	    "trim",
	    "ltrim",
	    "rtrim",
	    "replace",
	    "left",
	    "right",
	    "lpad",
	    "rpad",
	    "substring",
	    "substr",
	    "strpos",
	    "instr",
	    "position",
	    "starts_with",
	    "prefix",
	    "md5",
	    "ascii",
	    "unicode",
	    "chr",
	    "repeat",
	    "translate",
	    "contains",
	    "ends_with",
	    "suffix",
	    "^@",
	    "sha256",
	    "lower",
	    "lcase",
	    "~~*",
	    "!~~*",
	    // math
	    "abs",
	    "ceil",
	    "ceiling",
	    "floor",
	    "trunc",
	    "sign",
	    "cbrt",
	    "sin",
	    "cos",
	    "tan",
	    "atan",
	    "atan2",
	    "degrees",
	    "radians",
	    "pi",
	    "sqrt",
	    "ln",
	    "log",
	    "log10",
	    "exp",
	    "pow",
	    "power",
	    "^",
	    "**",
	    "isnan",
	    "isinf",
	    "isfinite",
	    "gcd",
	    "lcm",
	    "greatest_common_divisor",
	    "least_common_multiple",
	    "xor",
	    "&",
	    "|",
	    "~",
	    "round",
	    "round_even",
	    "roundbankers",
	    // date / time
	    "year",
	    "month",
	    "day",
	    "dayofmonth",
	    "hour",
	    "minute",
	    "quarter",
	    "dayofweek",
	    "isodow",
	    "dayofyear",
	    "week",
	    "weekofyear",
	    "second",
	    "millisecond",
	    "microsecond",
	    "isoyear",
	    "decade",
	    "century",
	    "millennium",
	    "epoch",
	    "epoch_ms",
	    "date_part",
	    "datepart",
	    "date_trunc",
	    "datetrunc",
	    "date_diff",
	    "datediff",
	    "last_day",
	    "make_date",
	    "to_timestamp",
	    // grouping sets
	    "grouping",
	    "grouping_id",
	    // conditional
	    "coalesce",
	    "nullif",
	    "ifnull",
	    "greatest",
	    "least",
	};
	if (PostgresIsOrderSensitiveFunction(function_name) && StringOrderingDiffers()) {
		// the result depends on the string ordering, which differs from DuckDB's for this database
		return false;
	}
	static const case_insensitive_set_t CASE_MAPPING_FUNCTIONS = {"lower", "lcase", "~~*", "!~~*"};
	if (CASE_MAPPING_FUNCTIONS.count(function_name) > 0 && !info.unicode_case_mapping) {
		// the server only maps ASCII characters to lower case
		return false;
	}
	if (function_name == "sha256" && info.version < PostgresVersion(11, 0)) {
		return false;
	}
	static const case_insensitive_set_t PG13_FUNCTIONS = {"gcd", "lcm", "greatest_common_divisor",
	                                                      "least_common_multiple"};
	if (PG13_FUNCTIONS.count(function_name) > 0 && info.version < PostgresVersion(13, 0)) {
		return false;
	}
	if ((function_name == "date_part" || function_name == "datepart" || function_name == "date_diff" ||
	     function_name == "datediff" || function_name == "epoch" || function_name == "epoch_ms") &&
	    info.version < PostgresVersion(14, 0)) {
		// EXTRACT returns an exact NUMERIC from Postgres 14
		return false;
	}
	return SCALAR_FUNCTIONS.count(function_name) > 0 || PostgresIsAggregateFunction(function_name);
}

static bool PostgresIsLikeFunction(const string &name) {
	return name == "~~" || name == "!~~";
}

//===--------------------------------------------------------------------===//
// Select alias usage
//===--------------------------------------------------------------------===//
static void PostgresCollectColumnRefs(const QueryNode &node, case_insensitive_set_t &names);

//! Collect the names of all unqualified column references in an expression tree, descending into subqueries
static void PostgresCollectColumnRefs(const ParsedExpression &expr, case_insensitive_set_t &names) {
	if (expr.GetExpressionClass() == ExpressionClass::COLUMN_REF) {
		auto &colref = expr.Cast<ColumnRefExpression>();
		if (!colref.IsQualified()) {
			names.insert(colref.GetColumnName().GetIdentifierName());
		}
	}
	if (expr.GetExpressionClass() == ExpressionClass::SUBQUERY) {
		auto &subquery = expr.Cast<SubqueryExpression>();
		if (subquery.Subquery() && subquery.Subquery()->node) {
			PostgresCollectColumnRefs(*subquery.Subquery()->node, names);
		}
	}
	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](const ParsedExpression &child) { PostgresCollectColumnRefs(child, names); });
}

static void PostgresCollectColumnRefs(const TableRef &ref, case_insensitive_set_t &names) {
	switch (ref.type) {
	case TableReferenceType::JOIN: {
		auto &join = ref.Cast<JoinRef>();
		PostgresCollectColumnRefs(*join.left, names);
		PostgresCollectColumnRefs(*join.right, names);
		if (join.condition) {
			PostgresCollectColumnRefs(*join.condition, names);
		}
		break;
	}
	case TableReferenceType::SUBQUERY: {
		auto &subquery = ref.Cast<SubqueryRef>();
		if (subquery.subquery && subquery.subquery->node) {
			PostgresCollectColumnRefs(*subquery.subquery->node, names);
		}
		break;
	}
	default:
		break;
	}
}

static void PostgresCollectColumnRefs(const QueryNode &node, case_insensitive_set_t &names) {
	for (auto &cte_pair : node.cte_map.map) {
		if (cte_pair.second->query_node) {
			PostgresCollectColumnRefs(*cte_pair.second->query_node, names);
		}
	}
	for (auto &modifier : node.modifiers) {
		if (modifier->type == ResultModifierType::ORDER_MODIFIER) {
			for (auto &order : modifier->Cast<OrderModifier>().orders) {
				PostgresCollectColumnRefs(*order.expression, names);
			}
		}
	}
	switch (node.type) {
	case QueryNodeType::SELECT_NODE: {
		auto &select = node.Cast<SelectNode>();
		for (auto &expr : select.select_list) {
			PostgresCollectColumnRefs(*expr, names);
		}
		if (select.from_table) {
			PostgresCollectColumnRefs(*select.from_table, names);
		}
		if (select.where_clause) {
			PostgresCollectColumnRefs(*select.where_clause, names);
		}
		for (auto &group : select.groups.group_expressions) {
			PostgresCollectColumnRefs(*group, names);
		}
		if (select.having) {
			PostgresCollectColumnRefs(*select.having, names);
		}
		if (select.qualify) {
			PostgresCollectColumnRefs(*select.qualify, names);
		}
		break;
	}
	case QueryNodeType::SET_OPERATION_NODE: {
		auto &setop = node.Cast<SetOperationNode>();
		for (auto &child : setop.children) {
			PostgresCollectColumnRefs(*child, names);
		}
		break;
	}
	case QueryNodeType::RECURSIVE_CTE_NODE: {
		auto &cte = node.Cast<RecursiveCTENode>();
		PostgresCollectColumnRefs(*cte.left, names);
		PostgresCollectColumnRefs(*cte.right, names);
		break;
	}
	default:
		break;
	}
}

//! Postgres can only resolve select-list aliases as bare names in GROUP BY and ORDER BY - not in the
//! WHERE / HAVING clauses, not within other select-list expressions and not within GROUP BY / ORDER BY
//! expressions, while DuckDB can. Both systems prefer a real table column over an alias, so an alias
//! reference is only a problem when no column with that name exists - which cannot be known pre-bind.
//! We approximate: a name is assumed to be a real column if it is also referenced at a position where
//! alias resolution is impossible (in a select-list item at or before the item that defines the alias).
//! Otherwise the query is conservatively not pushed down.
bool PostgresCapabilities::SupportsSelectAliasUsage(const SelectNode &select) {
	// find the first select-list item that defines each alias
	case_insensitive_map_t<idx_t> alias_definitions;
	for (idx_t i = 0; i < select.select_list.size(); i++) {
		auto &alias = select.select_list[i]->GetAlias().GetIdentifierName();
		if (!alias.empty() && alias_definitions.find(alias) == alias_definitions.end()) {
			alias_definitions[alias] = i;
		}
	}
	if (alias_definitions.empty()) {
		return true;
	}
	// collect the column references per select-list item
	vector<case_insensitive_set_t> item_refs(select.select_list.size());
	for (idx_t i = 0; i < select.select_list.size(); i++) {
		PostgresCollectColumnRefs(*select.select_list[i], item_refs[i]);
	}
	// a reference at or before the defining item cannot be an alias reference - it must be a column
	case_insensitive_set_t column_evidence;
	for (idx_t i = 0; i < select.select_list.size(); i++) {
		for (auto &name : item_refs[i]) {
			auto entry = alias_definitions.find(name);
			if (entry == alias_definitions.end() || entry->second >= i) {
				column_evidence.insert(name);
			}
		}
	}
	// collect the names that DuckDB might resolve as an alias where Postgres cannot
	case_insensitive_set_t suspects;
	if (select.where_clause) {
		PostgresCollectColumnRefs(*select.where_clause, suspects);
	}
	if (select.having) {
		PostgresCollectColumnRefs(*select.having, suspects);
	}
	for (auto &group : select.groups.group_expressions) {
		if (group->GetExpressionClass() != ExpressionClass::COLUMN_REF) {
			PostgresCollectColumnRefs(*group, suspects);
		}
	}
	for (auto &modifier : select.modifiers) {
		if (modifier->type != ResultModifierType::ORDER_MODIFIER) {
			continue;
		}
		for (auto &order : modifier->Cast<OrderModifier>().orders) {
			if (order.expression->GetExpressionClass() != ExpressionClass::COLUMN_REF) {
				PostgresCollectColumnRefs(*order.expression, suspects);
			}
		}
	}
	for (idx_t i = 0; i < select.select_list.size(); i++) {
		for (auto &name : item_refs[i]) {
			auto entry = alias_definitions.find(name);
			if (entry != alias_definitions.end() && entry->second < i) {
				suspects.insert(name);
			}
		}
	}
	for (auto &name : suspects) {
		if (alias_definitions.find(name) != alias_definitions.end() && column_evidence.count(name) == 0) {
			return false;
		}
	}
	return true;
}

//! Check whether a join tree contains a NATURAL or USING join - these coalesce the join columns when
//! expanding an unqualified *, and Postgres places the coalesced columns first while DuckDB keeps them
//! in the position of the left table's column order
static bool PostgresJoinTreeHasCoalescingJoin(const TableRef &ref) {
	if (ref.type != TableReferenceType::JOIN) {
		return false;
	}
	auto &join = ref.Cast<JoinRef>();
	if (join.ref_type == JoinRefType::NATURAL || !join.using_columns.empty()) {
		return true;
	}
	return PostgresJoinTreeHasCoalescingJoin(*join.left) || PostgresJoinTreeHasCoalescingJoin(*join.right);
}

//===--------------------------------------------------------------------===//
// Result modifiers
//===--------------------------------------------------------------------===//
static bool PostgresIsPlainStar(const ParsedExpression &expr) {
	auto &star = expr.Cast<StarExpression>();
	return star.RelationName().empty() && star.ExcludeList().empty() && star.ReplaceList().empty() &&
	       star.RenameList().empty() && !star.Expression();
}

//! Whether an expression tree contains a COLUMNS(*) expression
static bool PostgresContainsColumnsStar(const ParsedExpression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::STAR && expr.Cast<StarExpression>().IsColumns()) {
		return true;
	}
	bool result = false;
	ParsedExpressionIterator::EnumerateChildren(expr, [&](const ParsedExpression &child) {
		if (PostgresContainsColumnsStar(child)) {
			result = true;
		}
	});
	return result;
}

bool PostgresCapabilities::SupportsOrderEntries(const vector<OrderByNode> &orders,
                                                optional_ptr<const vector<unique_ptr<ParsedExpression>>> select_list) {
	if (!orders.empty() && StringOrderingDiffers()) {
		// string ordering differs from DuckDB's for this database
		return false;
	}
	for (auto &order : orders) {
		auto &expr = *order.expression;
		if (expr.GetExpressionClass() == ExpressionClass::STAR) {
			// ORDER BY ALL is serialized as an ORDER BY of all select-list positions - which requires the
			// select list to not contain any stars itself
			if (!select_list || orders.size() != 1 || !PostgresIsPlainStar(expr)) {
				return false;
			}
			for (auto &select_expr : *select_list) {
				if (select_expr->GetExpressionClass() == ExpressionClass::STAR) {
					return false;
				}
			}
		}
		if (expr.GetExpressionClass() == ExpressionClass::CONSTANT && !PostgresIsIntegerLiteral(expr)) {
			// ORDER BY with a non-integer literal is a binder error in DuckDB (unless
			// order_by_non_integer_literal is set)
			return false;
		}
	}
	return true;
}

bool PostgresCapabilities::SupportsResultModifiers(
    const QueryNode &node, optional_ptr<const vector<unique_ptr<ParsedExpression>>> select_list) {
	bool has_order = false;
	bool has_distinct = false;
	bool has_distinct_on = false;
	for (auto &modifier : node.modifiers) {
		switch (modifier->type) {
		case ResultModifierType::ORDER_MODIFIER: {
			auto &order_mod = modifier->Cast<OrderModifier>();
			if (!SupportsOrderEntries(order_mod.orders, select_list)) {
				return false;
			}
			has_order = true;
			break;
		}
		case ResultModifierType::LIMIT_MODIFIER: {
			auto &limit_mod = modifier->Cast<LimitModifier>();
			// negative values are a binder error in DuckDB - keep them local so the user gets DuckDB's error
			if (limit_mod.limit_type != LimitValueType::ROW_COUNT) {
				// Postgres has no LIMIT n PERCENT
				return false;
			}
			if (limit_mod.limit && !PostgresIsIntegerLiteralAtLeast(*limit_mod.limit, 0)) {
				return false;
			}
			if (limit_mod.offset && !PostgresIsIntegerLiteralAtLeast(*limit_mod.offset, 0)) {
				return false;
			}
			break;
		}
		case ResultModifierType::DISTINCT_MODIFIER: {
			auto &distinct_mod = modifier->Cast<DistinctModifier>();
			if (!select_list) {
				// DISTINCT is only serialized as part of a SELECT node
				return false;
			}
			if (!distinct_mod.distinct_on_targets.empty()) {
				has_distinct_on = true;
			} else {
				has_distinct = true;
			}
			break;
		}
		default:
			// unknown result modifier
			return false;
		}
	}
	if (has_order && has_distinct_on) {
		// Postgres requires the DISTINCT ON expressions to match the leading ORDER BY expressions
		return false;
	}
	if (has_order && has_distinct && select_list) {
		// Postgres requires the ORDER BY expressions of a SELECT DISTINCT to appear in the select list
		for (auto &modifier : node.modifiers) {
			if (modifier->type != ResultModifierType::ORDER_MODIFIER) {
				continue;
			}
			for (auto &order : modifier->Cast<OrderModifier>().orders) {
				auto &expr = *order.expression;
				if (PostgresIsIntegerLiteral(expr) || expr.GetExpressionClass() == ExpressionClass::STAR) {
					continue;
				}
				bool in_select_list = false;
				for (auto &select_expr : *select_list) {
					if (select_expr->Equals(expr) ||
					    (expr.GetExpressionClass() == ExpressionClass::COLUMN_REF &&
					     !expr.Cast<ColumnRefExpression>().IsQualified() &&
					     select_expr->GetAlias() == expr.Cast<ColumnRefExpression>().GetColumnName())) {
						in_select_list = true;
					}
				}
				if (!in_select_list) {
					return false;
				}
			}
		}
	}
	return true;
}

bool PostgresCapabilities::SupportsCTEMap(const CommonTableExpressionMap &cte_map) {
	for (auto &cte_pair : cte_map.map) {
		auto &cte_info = *cte_pair.second;
		if (!cte_info.key_targets.empty()) {
			// USING KEY is DuckDB-specific
			return false;
		}
		if (cte_info.materialized != CTEMaterialize::CTE_MATERIALIZE_DEFAULT && info.version < PostgresVersion(12, 0)) {
			// AS [NOT] MATERIALIZED requires Postgres 12
			return false;
		}
	}
	return true;
}

bool PostgresCapabilities::SupportsWindow(const WindowExpression &window) {
	if (window.IgnoreNulls()) {
		// IGNORE NULLS is not supported by all Postgres versions
		return false;
	}
	if (window.Distinct()) {
		// Postgres window functions do not support DISTINCT
		return false;
	}
	if (!window.ArgOrders().empty()) {
		// ORDER BY within the argument list is DuckDB-specific
		return false;
	}
	if (!window.GetQualifiedName().Path().empty() && window.GetQualifiedName().Path().size() > 1) {
		return false;
	}
	bool has_frame_extensions = window.WindowExclude() != WindowExcludeMode::NO_OTHER;
	for (auto boundary : {window.WindowStart(), window.WindowEnd()}) {
		switch (boundary) {
		case WindowBoundary::UNBOUNDED_PRECEDING:
		case WindowBoundary::UNBOUNDED_FOLLOWING:
		case WindowBoundary::CURRENT_ROW_RANGE:
		case WindowBoundary::CURRENT_ROW_ROWS:
		case WindowBoundary::EXPR_PRECEDING_ROWS:
		case WindowBoundary::EXPR_FOLLOWING_ROWS:
			break;
		case WindowBoundary::CURRENT_ROW_GROUPS:
		case WindowBoundary::EXPR_PRECEDING_GROUPS:
		case WindowBoundary::EXPR_FOLLOWING_GROUPS:
		case WindowBoundary::EXPR_PRECEDING_RANGE:
		case WindowBoundary::EXPR_FOLLOWING_RANGE:
			has_frame_extensions = true;
			break;
		default:
			return false;
		}
	}
	if (has_frame_extensions && info.version < PostgresVersion(11, 0)) {
		// GROUPS frames, RANGE offsets and EXCLUDE require Postgres 11
		return false;
	}
	// frame offsets must be constants in Postgres
	for (auto offset : {window.StartExpr().get(), window.EndExpr().get()}) {
		Value value;
		interval_t interval;
		if (offset && !TryGetConstantValue(*offset, value) && !TryGetIntervalLiteral(*offset, interval)) {
			return false;
		}
	}
	if (!SupportsOrderEntries(window.OrderBy())) {
		return false;
	}
	auto &children = window.GetArguments();
	for (auto &child : children) {
		if (child.HasName()) {
			// named arguments are not supported
			return false;
		}
	}
	auto &function_name = window.FunctionName().GetIdentifierName();
	if (window.Filter() && PostgresAggregateFunctions().count(function_name) == 0) {
		// Postgres only supports FILTER for aggregate window functions
		return false;
	}
	if (function_name == "lead" || function_name == "lag") {
		// only push down literal non-negative offsets
		if (children.size() >= 2 && !PostgresIsIntegerLiteralAtLeast(children[1].GetExpression(), 0)) {
			return false;
		}
		if (children.empty() || children.size() > 3) {
			return false;
		}
	} else if (function_name == "ntile") {
		if (children.size() != 1 || !PostgresIsIntegerLiteralAtLeast(children[0].GetExpression(), 1)) {
			return false;
		}
	} else if (function_name == "nth_value") {
		if (children.size() != 2 || !PostgresIsIntegerLiteralAtLeast(children[1].GetExpression(), 1)) {
			return false;
		}
	}
	static const case_insensitive_set_t WINDOW_FUNCTIONS = {
	    "row_number", "rank", "dense_rank", "rank_dense",  "percent_rank", "cume_dist",
	    "ntile",      "lead", "lag",        "first_value", "last_value",   "nth_value",
	};
	if (WINDOW_FUNCTIONS.count(function_name) > 0) {
		return true;
	}
	if (PostgresIsOrderSensitiveFunction(function_name) && StringOrderingDiffers()) {
		return false;
	}
	return PostgresAggregateFunctions().count(function_name) > 0;
}

//===--------------------------------------------------------------------===//
// Capabilities
//===--------------------------------------------------------------------===//
bool PostgresCapabilities::Supports(RemoteCapability capability) const {
	switch (capability) {
	case RemoteCapability::IS_REMOTE:
	case RemoteCapability::CONNECT:
		return true;
	case RemoteCapability::EXECUTE_QUERY_NODE:
	case RemoteCapability::EXECUTE_STATEMENT:
		// Redshift speaks a different SQL dialect
		return info.version.type_v != PostgresInstanceType::REDSHIFT;
	default:
		return false;
	}
}

bool PostgresCapabilities::SupportsPushdown(const ParsedExpression &expr) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::CONSTANT: {
		auto value = expr.Cast<ConstantExpression>().GetLiteral().ToValue();
		return PostgresSupportsValue(value);
	}
	case ExpressionClass::CAST: {
		auto &cast_expr = expr.Cast<CastExpression>();
		if (cast_expr.IsTryCast()) {
			// Postgres has no TRY_CAST
			return false;
		}
		auto target_type = ResolveType(cast_expr.TargetType());
		if (cast_expr.Child().GetExpressionClass() == ExpressionClass::CONSTANT) {
			// casts of literals are evaluated locally and serialized as the resulting value - note that casting
			// a string without an explicit UTC offset to TIMESTAMP WITH TIME ZONE fails here (it requires the
			// session time zone), so those are not pushed down
			Value cast_result;
			return TryGetConstantValue(expr, cast_result) && PostgresSupportsValue(cast_result);
		}
		return SupportsCastType(target_type);
	}
	case ExpressionClass::FUNCTION: {
		auto &func = expr.Cast<FunctionExpression>();
		auto &function_name = func.FunctionName().GetIdentifierName();
		interval_t interval;
		if (TryGetIntervalLiteral(expr, interval)) {
			return true;
		}
		if (func.GetQualifiedName().Path().size() > 1) {
			// qualified function names refer to (user-defined) functions in the remote catalog
			return false;
		}
		if (func.ExportState()) {
			return false;
		}
		bool has_order_by = func.OrderBy() && !func.OrderBy()->orders.empty();
		if ((func.Filter() || has_order_by) && !PostgresIsAggregateFunction(function_name)) {
			return false;
		}
		if (has_order_by && (PostgresRewrittenAggregateFunctions().count(function_name) > 0 ||
		                     !SupportsOrderEntries(func.OrderBy()->orders))) {
			return false;
		}
		if (func.Distinct()) {
			static const case_insensitive_set_t DISTINCT_AGGREGATES = {
			    "count", "sum", "avg", "min", "max", "string_agg", "group_concat", "listagg", "array_agg", "list"};
			if (DISTINCT_AGGREGATES.count(function_name) == 0) {
				return false;
			}
		}
		auto &children = func.GetArguments();
		if (func.Distinct() && has_order_by && (function_name == "array_agg" || function_name == "list")) {
			for (auto &order : func.OrderBy()->orders) {
				if (children.empty() || !order.expression->Equals(children[0].GetExpression())) {
					return false;
				}
			}
		}
		static const case_insensitive_set_t STRING_AGG_FUNCTIONS = {"string_agg", "group_concat", "listagg"};
		if (STRING_AGG_FUNCTIONS.count(function_name) > 0) {
			// a NULL separator yields NULL in DuckDB - only push down literal separators
			if (children.empty() || children.size() > 2 ||
			    (children.size() == 2 && !PostgresIsStringLiteral(children[1].GetExpression()))) {
				return false;
			}
			if (func.Distinct() && has_order_by) {
				// Postgres requires the ORDER BY expressions to match the arguments for DISTINCT aggregates
				for (auto &order : func.OrderBy()->orders) {
					if (!order.expression->Equals(children[0].GetExpression())) {
						return false;
					}
				}
			}
		}
		if (function_name == "median" && children.size() != 1) {
			return false;
		}
		static const case_insensitive_set_t QUANTILE_FUNCTIONS = {"quantile", "quantile_cont", "quantile_disc"};
		if (QUANTILE_FUNCTIONS.count(function_name) > 0) {
			// only a single literal quantile is supported (DuckDB returns a list for a list of quantiles)
			Value quantile;
			if (children.size() != 2 || !PostgresTryGetConstantValue(children[1].GetExpression(), quantile)) {
				return false;
			}
			auto quantile_value = quantile.DefaultTryCastAs(LogicalType::DOUBLE);
			if (!quantile_value || quantile_value->IsNull() || DoubleValue::Get(*quantile_value) < 0 ||
			    DoubleValue::Get(*quantile_value) > 1) {
				return false;
			}
		}
		static const case_insensitive_set_t ARG_FUNCTIONS = {"arg_min", "arg_max", "min_by", "max_by"};
		if (ARG_FUNCTIONS.count(function_name) > 0 && (children.size() != 2 || func.Distinct())) {
			return false;
		}
		if ((function_name == "count_if" || function_name == "countif") && (children.size() != 1 || func.Distinct())) {
			return false;
		}
		if (function_name == "any_value" && info.version < PostgresVersion(16, 0)) {
			return false;
		}
		if (function_name == "bit_xor" && info.version < PostgresVersion(14, 0)) {
			return false;
		}
		for (auto &child : children) {
			if (child.HasName()) {
				// named arguments are not supported
				return false;
			}
		}
		if (PostgresIsLikeFunction(function_name) && children.size() != 2) {
			return false;
		}
		static const case_insensitive_set_t PART_FUNCTIONS = {"date_part", "datepart",  "date_trunc",
		                                                      "datetrunc", "date_diff", "datediff"};
		if (PART_FUNCTIONS.count(function_name) > 0) {
			// the date part must be a literal
			auto expected_children = function_name == "date_diff" || function_name == "datediff" ? 3 : 2;
			if (children.size() != expected_children || !PostgresIsStringLiteral(children[0].GetExpression())) {
				return false;
			}
		}
		if (function_name == "round" || function_name == "round_even" || function_name == "roundbankers") {
			// only literal precisions are supported
			if (children.empty() || children.size() > 2 ||
			    (children.size() == 2 && !PostgresIsIntegerLiteral(children[1].GetExpression()))) {
				return false;
			}
		}
		if ((function_name == "count_star" && !children.empty()) || (function_name == "count" && children.size() > 1)) {
			return false;
		}
		if ((StringUtil::CIEquals(function_name, "substring") || StringUtil::CIEquals(function_name, "substr"))) {
			// DuckDB counts negative offsets from the end of the string - only push down literal non-negative
			// offsets (and a literal non-negative length)
			if (children.size() < 2 || children.size() > 3) {
				return false;
			}
			if (!PostgresIsIntegerLiteralAtLeast(children[1].GetExpression(), 0)) {
				return false;
			}
			if (children.size() == 3 && !PostgresIsIntegerLiteralAtLeast(children[2].GetExpression(), 0)) {
				return false;
			}
		}
		if (StringUtil::CIEquals(function_name, "trunc") && children.size() != 1) {
			// the two-argument trunc only exists for NUMERIC in Postgres
			return false;
		}
		static const case_insensitive_set_t DATE_PART_FUNCTIONS = {"year",   "month",     "day",     "dayofmonth",
		                                                           "hour",   "minute",    "quarter", "dayofweek",
		                                                           "isodow", "dayofyear", "week",    "weekofyear"};
		if (DATE_PART_FUNCTIONS.count(function_name) > 0 && children.size() != 1) {
			return false;
		}
		return SupportsFunction(func);
	}
	case ExpressionClass::WINDOW:
		return SupportsWindow(expr.Cast<WindowExpression>());
	case ExpressionClass::COMPARISON:
		switch (expr.GetExpressionType()) {
		case ExpressionType::COMPARE_EQUAL:
		case ExpressionType::COMPARE_NOTEQUAL:
		case ExpressionType::COMPARE_DISTINCT_FROM:
		case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
			return true;
		case ExpressionType::COMPARE_LESSTHAN:
		case ExpressionType::COMPARE_GREATERTHAN:
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			// string ordering must match DuckDB's binary ordering
			return !StringOrderingDiffers();
		default:
			return false;
		}
	case ExpressionClass::BETWEEN:
		return !StringOrderingDiffers();
	case ExpressionClass::OPERATOR:
		switch (expr.GetExpressionType()) {
		case ExpressionType::OPERATOR_IS_NULL:
		case ExpressionType::OPERATOR_IS_NOT_NULL:
		case ExpressionType::OPERATOR_NOT:
		case ExpressionType::OPERATOR_COALESCE:
		case ExpressionType::OPERATOR_NULLIF:
		case ExpressionType::COMPARE_IN:
		case ExpressionType::COMPARE_NOT_IN:
		case ExpressionType::GROUPING_FUNCTION:
			return true;
		default:
			// TRY(...), list / struct operators, ... are DuckDB-specific
			return false;
		}
	case ExpressionClass::SUBQUERY: {
		auto &subquery = expr.Cast<SubqueryExpression>();
		switch (subquery.GetSubqueryType()) {
		case SubqueryType::EXISTS:
		case SubqueryType::NOT_EXISTS:
		case SubqueryType::SCALAR:
			return true;
		case SubqueryType::ANY:
			switch (subquery.GetComparisonType()) {
			case ExpressionType::COMPARE_EQUAL:
			case ExpressionType::COMPARE_NOTEQUAL:
				return true;
			case ExpressionType::COMPARE_LESSTHAN:
			case ExpressionType::COMPARE_GREATERTHAN:
			case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
				return !StringOrderingDiffers();
			default:
				return false;
			}
		default:
			return false;
		}
	}
	case ExpressionClass::STAR: {
		auto &star = expr.Cast<StarExpression>();
		if (!star.ExcludeList().empty() || !star.ReplaceList().empty() || !star.RenameList().empty()) {
			// EXCLUDE / REPLACE / RENAME are DuckDB-specific
			return false;
		}
		if (star.Expression()) {
			// COLUMNS(...) expressions are DuckDB-specific
			return false;
		}
		// COLUMNS(*) is DuckDB-specific, but ORDER BY ALL is represented the same way - those are verified
		// by SupportsPushdown(QueryNode)
		return true;
	}
	case ExpressionClass::COLUMN_REF: {
		auto &colref = expr.Cast<ColumnRefExpression>();
		if (!colref.IsQualified()) {
			// SQL value functions without parentheses (CURRENT_DATE, ...) are parsed as column references
			static const case_insensitive_set_t UNSUPPORTED_VALUE_FUNCTIONS = {
			    "current_time", "localtime",    "current_user",    "user",
			    "session_user", "current_role", "current_catalog", "current_schema"};
			if (UNSUPPORTED_VALUE_FUNCTIONS.count(colref.GetColumnName().GetIdentifierName()) > 0) {
				return false;
			}
		}
		return colref.ColumnNames().size() <= 3;
	}
	case ExpressionClass::CASE:
	case ExpressionClass::CONJUNCTION:
		return true;
	case ExpressionClass::DEFAULT:
		// DEFAULT in an UPDATE SET clause / INSERT VALUES list - anywhere else it is rejected by the binder
		return true;
	default:
		// parameters, COLLATE, lambdas, positional references, ... - not supported
		return false;
	}
}

bool PostgresCapabilities::SupportsPushdown(const TableRef &ref) {
	if (ref.sample) {
		// TABLESAMPLE semantics differ
		return false;
	}
	switch (ref.type) {
	case TableReferenceType::BASE_TABLE: {
		auto &base = ref.Cast<BaseTableRef>();
		if (base.at_clause) {
			// AT (...) time travel clauses are DuckDB-specific
			return false;
		}
		return true;
	}
	case TableReferenceType::JOIN: {
		auto &join = ref.Cast<JoinRef>();
		switch (join.ref_type) {
		case JoinRefType::REGULAR:
		case JoinRefType::NATURAL:
		case JoinRefType::CROSS:
			break;
		default:
			// ASOF / POSITIONAL / ... joins are DuckDB-specific
			return false;
		}
		switch (join.type) {
		case JoinType::INNER:
		case JoinType::LEFT:
		case JoinType::RIGHT:
		case JoinType::OUTER:
			break;
		case JoinType::SEMI:
		case JoinType::ANTI:
			// serialized as a [NOT] EXISTS filter over the left side - which must be a single (named) relation
			if (join.ref_type != JoinRefType::REGULAR || !join.condition || !join.using_columns.empty()) {
				return false;
			}
			if (join.left->type == TableReferenceType::BASE_TABLE) {
				break;
			}
			if (join.left->type == TableReferenceType::SUBQUERY && !join.left->alias.empty()) {
				break;
			}
			return false;
		default:
			return false;
		}
		if (!join.duplicate_eliminated_columns.empty()) {
			return false;
		}
		return true;
	}
	case TableReferenceType::SUBQUERY:
	case TableReferenceType::EMPTY_FROM:
		return true;
	case TableReferenceType::EXPRESSION_LIST: {
		auto &expression_list = ref.Cast<ExpressionListRef>();
		return !expression_list.values.empty();
	}
	case TableReferenceType::TABLE_FUNCTION: {
		auto &table_function = ref.Cast<TableFunctionRef>();
		if (table_function.subquery || table_function.function->GetExpressionClass() != ExpressionClass::FUNCTION) {
			return false;
		}
		auto &func = table_function.function->Cast<FunctionExpression>();
		if (func.GetQualifiedName().Path().size() > 1) {
			return false;
		}
		auto &name = func.FunctionName();
		auto &children = func.GetArguments();
		for (auto &child : children) {
			if (child.HasName()) {
				return false;
			}
		}
		if (name == "generate_series") {
			return children.size() >= 2 && children.size() <= 3;
		}
		if (name == "range") {
			// the exclusive upper bound is serialized with a literal step
			if (children.size() == 3) {
				int64_t step;
				return PostgresTryGetIntegerLiteral(children[2].GetExpression(), step) && step != 0;
			}
			return !children.empty() && children.size() <= 2;
		}
		// other table functions are DuckDB-specific
		return false;
	}
	default:
		// pivots, ... - not supported
		return false;
	}
}

static void PostgresCollectTableNames(QueryNode &node, identifier_set_t &cte_names, identifier_set_t &table_names,
                                      bool all_tables);

static void PostgresCollectTableNames(ParsedExpression &expr, identifier_set_t &cte_names,
                                      identifier_set_t &table_names, bool all_tables) {
	if (expr.GetExpressionClass() == ExpressionClass::SUBQUERY) {
		PostgresCollectTableNames(*expr.Cast<SubqueryExpression>().SubqueryMutable()->node, cte_names, table_names,
		                          all_tables);
	}
	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](ParsedExpression &child) { PostgresCollectTableNames(child, cte_names, table_names, all_tables); });
}

//! Collect the names of all CTEs, and of all qualified table references (or of all table references)
static void PostgresCollectTableNames(QueryNode &node, identifier_set_t &cte_names, identifier_set_t &table_names,
                                      bool all_tables) {
	for (auto &cte : node.cte_map.map) {
		cte_names.insert(cte.first);
	}
	ParsedExpressionIterator::EnumerateQueryNodeChildren(
	    node,
	    [&](unique_ptr<ParsedExpression> &child) {
		    PostgresCollectTableNames(*child, cte_names, table_names, all_tables);
	    },
	    [&](TableRef &ref) {
		    if (ref.type == TableReferenceType::BASE_TABLE) {
			    auto &base = ref.Cast<BaseTableRef>();
			    if (all_tables || base.GetQualifiedName().Path().size() > 1) {
				    table_names.insert(base.Table());
			    }
		    }
	    });
}

//! The catalog qualification is stripped from table references before the query is serialized - a qualified
//! reference to a table with the same name as a CTE would then refer to the CTE
static bool PostgresHasCTETableNameConflict(const QueryNode &node) {
	auto copy = node.Copy();
	identifier_set_t cte_names;
	identifier_set_t qualified_tables;
	PostgresCollectTableNames(*copy, cte_names, qualified_tables, false);
	for (auto &name : cte_names) {
		if (qualified_tables.count(name) > 0) {
			return true;
		}
	}
	return false;
}

bool PostgresCapabilities::SupportsPushdown(const QueryNode &node) {
	if (!SupportsCTEMap(node.cte_map)) {
		return false;
	}
	if (!node.cte_map.map.empty() && PostgresHasCTETableNameConflict(node)) {
		return false;
	}
	switch (node.type) {
	case QueryNodeType::SELECT_NODE: {
		auto &select = node.Cast<SelectNode>();
		if (select.qualify) {
			// Postgres has no QUALIFY
			return false;
		}
		if (select.aggregate_handling == AggregateHandling::FORCE_AGGREGATES) {
			// GROUP BY ALL is DuckDB-specific
			return false;
		}
		if (select.sample) {
			// Postgres has no USING SAMPLE
			return false;
		}
		if (!SupportsSelectAliasUsage(select)) {
			return false;
		}
		vector<reference<const ParsedExpression>> expressions;
		for (auto &expr : select.select_list) {
			expressions.push_back(*expr);
		}
		for (auto &expr : select.groups.group_expressions) {
			expressions.push_back(*expr);
		}
		if (select.where_clause) {
			expressions.push_back(*select.where_clause);
		}
		if (select.having) {
			expressions.push_back(*select.having);
		}
		for (auto &expr : expressions) {
			if (PostgresContainsColumnsStar(expr.get())) {
				// COLUMNS(*) is DuckDB-specific
				return false;
			}
		}
		if (select.from_table && PostgresJoinTreeHasCoalescingJoin(*select.from_table)) {
			for (auto &expr : select.select_list) {
				if (expr->GetExpressionClass() == ExpressionClass::STAR &&
				    expr->Cast<StarExpression>().RelationName().empty()) {
					return false;
				}
			}
		}
		return SupportsResultModifiers(node, &select.select_list);
	}
	case QueryNodeType::SET_OPERATION_NODE: {
		auto &setop = node.Cast<SetOperationNode>();
		switch (setop.setop_type) {
		case SetOperationType::UNION:
		case SetOperationType::EXCEPT:
		case SetOperationType::INTERSECT:
			break;
		default:
			// UNION BY NAME is DuckDB-specific
			return false;
		}
		for (auto &modifier : node.modifiers) {
			if (modifier->type != ResultModifierType::ORDER_MODIFIER) {
				continue;
			}
			for (auto &order : modifier->Cast<OrderModifier>().orders) {
				// Postgres can only order the result of a set operation by output column names or positions
				auto &expr = *order.expression;
				bool is_name = expr.GetExpressionClass() == ExpressionClass::COLUMN_REF &&
				               !expr.Cast<ColumnRefExpression>().IsQualified();
				if (!is_name && !PostgresIsIntegerLiteral(expr)) {
					return false;
				}
			}
		}
		return SupportsResultModifiers(node);
	}
	case QueryNodeType::RECURSIVE_CTE_NODE: {
		auto &cte = node.Cast<RecursiveCTENode>();
		if (!cte.key_targets.empty()) {
			// USING KEY is DuckDB-specific
			return false;
		}
		return SupportsResultModifiers(node);
	}
	case QueryNodeType::UPDATE_QUERY_NODE: {
		auto &update = node.Cast<UpdateQueryNode>();
		if (info.read_only) {
			// the read-only check is performed by DuckDB when executing the statement locally
			return false;
		}
		if (!update.set_info || !update.table || update.table->type != TableReferenceType::BASE_TABLE) {
			return false;
		}
		// Postgres does not support ORDER BY / LIMIT in UPDATE statements
		return node.modifiers.empty();
	}
	case QueryNodeType::DELETE_QUERY_NODE: {
		auto &del = node.Cast<DeleteQueryNode>();
		if (info.read_only) {
			return false;
		}
		if (!del.table || del.table->type != TableReferenceType::BASE_TABLE) {
			return false;
		}
		return node.modifiers.empty();
	}
	case QueryNodeType::INSERT_QUERY_NODE: {
		auto &insert = node.Cast<InsertQueryNode>();
		if (info.read_only) {
			return false;
		}
		if (insert.column_order != InsertColumnOrder::INSERT_BY_POSITION) {
			// INSERT BY NAME is DuckDB-specific
			return false;
		}
		if (!insert.select_statement && !insert.default_values) {
			return false;
		}
		if (insert.on_conflict_info) {
			auto &on_conflict = *insert.on_conflict_info;
			if (on_conflict.condition) {
				// conflict targets on partial indexes are not supported
				return false;
			}
			switch (on_conflict.action_type) {
			case OnConflictAction::THROW:
			case OnConflictAction::NOTHING:
			case OnConflictAction::REPLACE:
				break;
			case OnConflictAction::UPDATE:
				if (!on_conflict.set_info) {
					return false;
				}
				break;
			default:
				return false;
			}
		}
		return node.modifiers.empty();
	}
	case QueryNodeType::MERGE_QUERY_NODE: {
		auto &merge = node.Cast<MergeQueryNode>();
		if (info.read_only || info.version < PostgresVersion(15, 0)) {
			// MERGE requires Postgres 15
			return false;
		}
		if (!merge.target || merge.target->type != TableReferenceType::BASE_TABLE || !merge.source ||
		    !node.modifiers.empty()) {
			return false;
		}
		if (!merge.returning_list.empty() && info.version < PostgresVersion(17, 0)) {
			// MERGE ... RETURNING requires Postgres 17
			return false;
		}
		for (auto &entry : merge.actions) {
			if (entry.first == MergeActionCondition::WHEN_NOT_MATCHED_BY_SOURCE &&
			    info.version < PostgresVersion(17, 0)) {
				// WHEN NOT MATCHED BY SOURCE requires Postgres 17
				return false;
			}
			for (auto &action : entry.second) {
				if (!action->exclude_columns.empty() || action->column_order != InsertColumnOrder::INSERT_BY_POSITION) {
					return false;
				}
				switch (action->action_type) {
				case MergeActionType::MERGE_UPDATE:
					// UPDATE without a SET clause updates all columns by name in DuckDB
					if (!action->update_info || action->update_info->columns.empty()) {
						return false;
					}
					break;
				case MergeActionType::MERGE_INSERT:
					// INSERT without a column list inserts all columns by name in DuckDB
					if (!action->default_values && action->expressions.empty()) {
						return false;
					}
					break;
				case MergeActionType::MERGE_DELETE:
				case MergeActionType::MERGE_DO_NOTHING:
					break;
				default:
					// THEN ERROR is DuckDB-specific
					return false;
				}
			}
		}
		return true;
	}
	default:
		return false;
	}
}

//! Functions whose serialization depends on the types of their arguments - these are verified when serializing,
//! and can therefore not be part of statements that cannot be executed locally instead
static bool PostgresIsTypeDependentFunction(const string &name) {
	static const case_insensitive_set_t TYPE_DEPENDENT_FUNCTIONS = {"/",
	                                                                "%",
	                                                                "||",
	                                                                "length",
	                                                                "len",
	                                                                "char_length",
	                                                                "character_length",
	                                                                "strlen",
	                                                                "octet_length",
	                                                                "concat",
	                                                                "concat_ws",
	                                                                "string_agg",
	                                                                "group_concat",
	                                                                "listagg",
	                                                                "array_agg",
	                                                                "list",
	                                                                "bit_and",
	                                                                "bit_or",
	                                                                "bit_xor",
	                                                                "median",
	                                                                "quantile",
	                                                                "quantile_cont",
	                                                                "quantile_disc",
	                                                                "arg_min",
	                                                                "arg_max",
	                                                                "min_by",
	                                                                "max_by",
	                                                                "lower",
	                                                                "lcase",
	                                                                "~~*",
	                                                                "!~~*",
	                                                                "contains",
	                                                                "ends_with",
	                                                                "suffix",
	                                                                "^@",
	                                                                "translate",
	                                                                "unicode",
	                                                                "chr",
	                                                                "sha256",
	                                                                "sqrt",
	                                                                "ln",
	                                                                "log",
	                                                                "log10",
	                                                                "exp",
	                                                                "pow",
	                                                                "power",
	                                                                "^",
	                                                                "**",
	                                                                "isnan",
	                                                                "isinf",
	                                                                "isfinite",
	                                                                "gcd",
	                                                                "lcm",
	                                                                "greatest_common_divisor",
	                                                                "least_common_multiple",
	                                                                "xor",
	                                                                "&",
	                                                                "|",
	                                                                "~",
	                                                                "round",
	                                                                "round_even",
	                                                                "roundbankers",
	                                                                "date_part",
	                                                                "datepart",
	                                                                "year",
	                                                                "month",
	                                                                "day",
	                                                                "dayofmonth",
	                                                                "hour",
	                                                                "minute",
	                                                                "second",
	                                                                "millisecond",
	                                                                "microsecond",
	                                                                "quarter",
	                                                                "dayofweek",
	                                                                "isodow",
	                                                                "dayofyear",
	                                                                "week",
	                                                                "weekofyear",
	                                                                "isoyear",
	                                                                "decade",
	                                                                "century",
	                                                                "millennium",
	                                                                "epoch",
	                                                                "epoch_ms",
	                                                                "date_diff",
	                                                                "datediff",
	                                                                "date_trunc",
	                                                                "datetrunc",
	                                                                "last_day",
	                                                                "make_date",
	                                                                "to_timestamp"};
	return TYPE_DEPENDENT_FUNCTIONS.count(name) > 0;
}

bool PostgresCapabilities::SupportsTypeIndependent(ParsedExpression &expr) {
	if (!SupportsPushdown(expr)) {
		return false;
	}
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::CAST: {
		Value value;
		if (!TryGetConstantValue(expr, value)) {
			// casts of non-constants depend on the source type
			return false;
		}
		break;
	}
	case ExpressionClass::FUNCTION: {
		interval_t interval;
		if (TryGetIntervalLiteral(expr, interval)) {
			return true;
		}
		if (PostgresIsTypeDependentFunction(expr.Cast<FunctionExpression>().FunctionName().GetIdentifierName())) {
			return false;
		}
		break;
	}
	case ExpressionClass::WINDOW:
		if (PostgresIsTypeDependentFunction(expr.Cast<WindowExpression>().FunctionName().GetIdentifierName())) {
			return false;
		}
		break;
	case ExpressionClass::SUBQUERY:
		if (!SupportsQueryTypeIndependent(*expr.Cast<SubqueryExpression>().SubqueryMutable()->node)) {
			return false;
		}
		break;
	default:
		break;
	}
	bool result = true;
	ParsedExpressionIterator::EnumerateChildren(expr, [&](ParsedExpression &child) {
		if (result && !SupportsTypeIndependent(child)) {
			result = false;
		}
	});
	return result;
}

bool PostgresCapabilities::SupportsQueryTypeIndependent(QueryNode &node) {
	if (!SupportsPushdown(node)) {
		return false;
	}
	bool result = true;
	ParsedExpressionIterator::EnumerateQueryNodeChildren(
	    node,
	    [&](unique_ptr<ParsedExpression> &child) {
		    if (result && !SupportsTypeIndependent(*child)) {
			    result = false;
		    }
	    },
	    [&](TableRef &ref) {
		    if (!result) {
			    return;
		    }
		    if (!SupportsPushdown(ref) || ref.type == TableReferenceType::TABLE_FUNCTION) {
			    // table functions are serialized depending on the types of their arguments
			    result = false;
		    } else if (ref.type == TableReferenceType::SUBQUERY &&
		               !SupportsQueryTypeIndependent(*ref.Cast<SubqueryRef>().subquery->node)) {
			    result = false;
		    }
	    });
	if (!result) {
		return false;
	}
	for (auto &cte : node.cte_map.map) {
		if (cte.second->query_node && !SupportsQueryTypeIndependent(*cte.second->query_node)) {
			return false;
		}
	}
	switch (node.type) {
	case QueryNodeType::SET_OPERATION_NODE:
		for (auto &child : node.Cast<SetOperationNode>().children) {
			if (!SupportsQueryTypeIndependent(*child)) {
				return false;
			}
		}
		break;
	case QueryNodeType::RECURSIVE_CTE_NODE: {
		auto &cte = node.Cast<RecursiveCTENode>();
		if (!SupportsQueryTypeIndependent(*cte.left) || !SupportsQueryTypeIndependent(*cte.right)) {
			return false;
		}
		break;
	}
	default:
		break;
	}
	return true;
}

PostgresCapabilities PostgresCapabilities::StatementCapabilities() const {
	PostgresCapabilities result(*this);
	result.writer_pins_collation = false;
	return result;
}

bool PostgresCapabilities::SupportsPushdown(const SQLStatement &statement) {
	if (info.read_only) {
		// the read-only check is performed by DuckDB when executing the statement locally
		return false;
	}
	// note: statements cannot be executed locally instead when they turn out not to be translatable, so everything
	// that is pushed down must be verified here
	switch (statement.type) {
	case StatementType::CREATE_STATEMENT: {
		auto &create_info = *statement.Cast<CreateStatement>().info;
		if (create_info.temporary) {
			return false;
		}
		switch (create_info.type) {
		case CatalogType::TABLE_ENTRY: {
			auto &table_info = create_info.Cast<CreateTableInfo>();
			switch (table_info.on_conflict) {
			case OnCreateConflict::ERROR_ON_CONFLICT:
			case OnCreateConflict::REPLACE_ON_CONFLICT:
				break;
			case OnCreateConflict::IGNORE_ON_CONFLICT:
				if (table_info.query) {
					// the query must not be executed when the table exists
					return false;
				}
				break;
			default:
				return false;
			}
			if (!table_info.partition_keys.empty() || !table_info.sort_keys.empty() || !table_info.options.empty()) {
				return false;
			}
			if (!table_info.query) {
				// a regular CREATE TABLE issues the same statement - and keeps the definition of the table (e.g.
				// column defaults) in the catalog
				return false;
			}
			// CREATE TABLE AS - the query is serialized into the INSERT that fills the table
			auto query = table_info.query->node->Copy();
			if (table_info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
				// the existing table is dropped before the query is executed - the query must not read it
				identifier_set_t cte_names;
				identifier_set_t table_names;
				PostgresCollectTableNames(*query, cte_names, table_names, true);
				if (table_names.count(table_info.GetQualifiedName().Name()) > 0) {
					return false;
				}
			}
			return StatementCapabilities().SupportsQueryTypeIndependent(*query);
		}
		case CatalogType::VIEW_ENTRY: {
			auto &view_info = create_info.Cast<CreateViewInfo>();
			if (view_info.on_conflict != OnCreateConflict::ERROR_ON_CONFLICT &&
			    view_info.on_conflict != OnCreateConflict::REPLACE_ON_CONFLICT) {
				// Postgres has no CREATE VIEW IF NOT EXISTS
				return false;
			}
			if (!view_info.query) {
				return false;
			}
			auto query = view_info.query->node->Copy();
			return StatementCapabilities().SupportsQueryTypeIndependent(*query);
		}
		default:
			return false;
		}
	}
	case StatementType::DROP_STATEMENT: {
		auto &drop_info = *statement.Cast<DropStatement>().info;
		switch (drop_info.type) {
		case CatalogType::TABLE_ENTRY:
		case CatalogType::VIEW_ENTRY:
		case CatalogType::INDEX_ENTRY:
		case CatalogType::SEQUENCE_ENTRY:
		case CatalogType::TYPE_ENTRY:
			// note: schemas are not reloaded when they are removed from the cache, DROP SCHEMA is executed regularly
			return drop_info.GetQualifiedName().Path().size() <= 2;
		default:
			return false;
		}
	}
	case StatementType::ALTER_STATEMENT: {
		auto &alter_info = *statement.Cast<AlterStatement>().info;
		if (alter_info.type != AlterType::ALTER_TABLE) {
			return false;
		}
		auto &alter_table_info = alter_info.Cast<AlterTableInfo>();
		switch (alter_table_info.alter_table_type) {
		case AlterTableType::RENAME_TABLE:
		case AlterTableType::REMOVE_COLUMN:
			return true;
		case AlterTableType::RENAME_COLUMN:
			return true;
		case AlterTableType::SET_NOT_NULL:
			return alter_table_info.Cast<SetNotNullInfo>().column_path.size() == 1;
		case AlterTableType::DROP_NOT_NULL:
			return alter_table_info.Cast<DropNotNullInfo>().column_path.size() == 1;
		case AlterTableType::ADD_COLUMN: {
			auto &add_info = alter_table_info.Cast<AddColumnInfo>();
			auto &column = add_info.new_column;
			if (column.HasDefaultValue() || column.Generated()) {
				// DuckDB fills in the default values of the existing rows itself
				return false;
			}
			LogicalType type = column.Type();
			if (type.id() == LogicalTypeId::UNBOUND) {
				auto &type_expr = UnboundType::GetTypeExpression(type);
				if (!type_expr || type_expr->GetExpressionClass() != ExpressionClass::TYPE) {
					return false;
				}
				type = ResolveType(type_expr->Cast<TypeExpression>());
			}
			return type.id() != LogicalTypeId::INVALID && SupportsCastType(type);
		}
		default:
			return false;
		}
	}
	default:
		return false;
	}
}

} // namespace duckdb
