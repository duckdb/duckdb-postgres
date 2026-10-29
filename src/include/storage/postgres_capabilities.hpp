//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/postgres_capabilities.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/type_expression.hpp"
#include "duckdb/parser/expression/window_expression.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/query_node.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/tableref.hpp"

#include "postgres_version.hpp"

namespace duckdb {

//! Server properties that influence which constructs can be pushed down into Postgres
struct PostgresPushdownInfo {
	PostgresVersion version;
	//! Whether the database default collation orders strings byte-wise (C / POSIX / C.UTF-8 / builtin),
	//! which matches DuckDB's (binary) string ordering
	bool binary_collation = false;
	//! Whether lower() / ILIKE apply Unicode case mapping (as DuckDB does) instead of only mapping ASCII characters
	bool unicode_case_mapping = false;
	//! Whether the database was attached read-only - DML and DDL are never pushed down then
	bool read_only = false;
};

//! Decides which parsed constructs can be serialized by PostgresSQLWriter and executed in Postgres
//! with the same semantics as in DuckDB
class PostgresCapabilities {
public:
	PostgresCapabilities() {
	}
	explicit PostgresCapabilities(PostgresPushdownInfo info_p) : info(std::move(info_p)) {
	}

	bool Supports(RemoteCapability capability) const;
	bool SupportsPushdown(const ParsedExpression &expression);
	bool SupportsPushdown(const TableRef &ref);
	bool SupportsPushdown(const QueryNode &node);
	bool SupportsPushdown(const SQLStatement &statement);

	const PostgresPushdownInfo &GetInfo() const {
		return info;
	}

	//! Resolve a cast target / column type expression against the built-in DuckDB types,
	//! returns LogicalTypeId::INVALID for types that are not built-in
	static LogicalType ResolveType(const TypeExpression &type_expr);
	//! Constant INTERVAL values are represented as add(add(to_months(m), to_days(d)), to_microseconds(us)) -
	//! recognize this representation
	static bool TryGetIntervalLiteral(const ParsedExpression &expr, interval_t &result);
	//! Whether a constant value can be serialized as a Postgres literal
	static bool SupportsValue(const Value &value);
	//! Get the value of a constant expression (or of a cast of a constant)
	static bool TryGetConstantValue(const ParsedExpression &expr, Value &result);

private:
	bool SupportsFunction(const FunctionExpression &func);
	bool SupportsWindow(const WindowExpression &window);
	bool SupportsCTEMap(const CommonTableExpressionMap &cte_map);
	bool SupportsResultModifiers(const QueryNode &node,
	                             optional_ptr<const vector<unique_ptr<ParsedExpression>>> select_list = nullptr);
	bool SupportsOrderEntries(const vector<OrderByNode> &orders,
	                          optional_ptr<const vector<unique_ptr<ParsedExpression>>> select_list = nullptr);
	bool SupportsSelectAliasUsage(const SelectNode &select);
	bool SupportsCastType(const LogicalType &type);
	//! Whether an expression / query can be serialized without depending on the types it operates on
	bool SupportsTypeIndependent(ParsedExpression &expr);
	bool SupportsQueryTypeIndependent(QueryNode &node);
	//! Whether string ordering (ORDER BY, <, min, ...) has to be blocked because it differs from DuckDB's
	//! The checks for statements - these cannot fall back to local execution when the writer cannot serialize them
	PostgresCapabilities StatementCapabilities() const;
	bool StringOrderingDiffers() const {
		return !info.binary_collation && !writer_pins_collation;
	}

private:
	PostgresPushdownInfo info;
	//! Whether the writer pins the C collation for string ordering (and falls back to local execution where it
	//! cannot) - not possible for statements, which cannot be executed locally instead
	bool writer_pins_collation = true;
};

} // namespace duckdb
