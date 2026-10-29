//===----------------------------------------------------------------------===//
//                         DuckDB
//
// postgres_sql_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/query_node.hpp"
#include "duckdb/parser/result_modifier.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/tableref.hpp"

namespace duckdb {
class BaseTableRef;
class AlterInfo;
class ColumnDefinition;
class CreateStatement;
struct CreateViewInfo;
struct DropInfo;
class DeleteQueryNode;
class ExpressionListRef;
class InsertQueryNode;
class MergeQueryNode;
class OnConflictInfo;
class UpdateQueryNode;
class UpdateSetInfo;
class CaseExpression;
class FunctionArgument;
class ColumnRefExpression;
class FunctionExpression;
class OperatorExpression;
class PostgresCatalog;
class RecursiveCTENode;
class SelectNode;
class SetOperationNode;
class SubqueryExpression;
class TableCatalogEntry;
class WindowExpression;

//! Thrown by the writer when a construct turns out not to be translatable once its types are known - the
//! query is then executed locally instead
class PostgresPushdownNotSupported : public std::exception {
public:
	explicit PostgresPushdownNotSupported(string reason_p) : reason(std::move(reason_p)) {
	}
	const char *what() const noexcept override {
		return reason.c_str();
	}

private:
	string reason;
};

//! A catalog entry that is modified by a statement, and has to be removed from the cache
struct PostgresCacheInvalidation {
	CatalogType type;
	string schema;
	string name;
	//! Remove all tables of the schema from the cache
	bool all_tables = false;
};

//! The result of serializing a query or statement for Postgres
struct PostgresWriterResult {
	//! Statements executed before the main statement (e.g. setting the session time zone)
	vector<string> preliminary_queries;
	//! The main statement, its result is returned to DuckDB
	string sql;
	//! Whether the main statement is a DML statement whose number of changed rows is the result
	bool changed_rows = false;
	//! Whether the result of the statement depends on the session time zone
	bool requires_time_zone = false;
	//! The cached catalog entries that are modified by the statement
	vector<PostgresCacheInvalidation> invalidations;
};

//! Serializes a parsed query tree into Postgres SQL.
//! This mirrors the structure of the DuckDB ToString() methods, with Postgres-specific differences
//! applied during serialization:
//! * identifiers are quoted using the spelling of the corresponding Postgres catalog entries
//!   (DuckDB identifiers are case-insensitive, quoted Postgres identifiers are not)
//! * base tables are always qualified with their Postgres schema
//! * functions and operators are remapped where semantics are identical under a different
//!   name or form (e.g. count_star() -> count(*), year(x) -> EXTRACT(YEAR FROM x), a / b -> float8 division)
//! * ORDER BY entries get an explicit NULLS FIRST / NULLS LAST
//! Constructs that cannot be serialized are rejected up-front by PostgresCapabilities - encountering
//! them here is an internal error.
class PostgresSQLWriter {
public:
	PostgresSQLWriter(ClientContext &context, PostgresCatalog &catalog);

	//! Serialize a query node - result_types are the types DuckDB itself binds the query to (if known)
	static PostgresWriterResult WriteQuery(ClientContext &context, PostgresCatalog &catalog, QueryNode &node,
	                                       optional_ptr<const vector<LogicalType>> result_types);

	//! Serialize a (DDL) statement
	static PostgresWriterResult WriteStatement(ClientContext &context, PostgresCatalog &catalog,
	                                           SQLStatement &statement);

	//! Collect the names of all CTEs defined anywhere in the query
	static void CollectCTENames(QueryNode &node, identifier_set_t &cte_names);
	//! Qualify all base table references with the catalog name again (the optimizer strips it before
	//! pushdown), so that the query can be bound by DuckDB itself
	static void QualifyTables(QueryNode &node, const Identifier &catalog_name, const identifier_set_t &cte_names);

private:
	//! Collect the Postgres spelling of all identifiers referenced by the query
	void CollectNames(QueryNode &node);
	void CollectNames(TableRef &ref);
	void CollectNames(ParsedExpression &expr);
	void AddCatalogName(const string &name);
	void AddCatalogColumnName(const string &name);
	void AddQueryName(const Identifier &name);
	optional_ptr<TableCatalogEntry> LookupTable(const BaseTableRef &ref);
	optional_ptr<TableCatalogEntry> LookupTable(const QualifiedName &qualified_name);
	void AddTableNames(TableCatalogEntry &table);
	bool IsCTEReference(const BaseTableRef &ref);

	void CollectResultNodes(const QueryNode &node);
	string WriteResultCast(const string &expr, idx_t column_index);
	string WriteResultCast(const string &expr, const LogicalType &type);
	//! The type DuckDB binds an expression at the current position in the query to - INVALID if unknown
	LogicalType InferType(const ParsedExpression &expr);
	LogicalType InferTypeInternal(const ParsedExpression &expr);
	//! The type of a column of a base table of an enclosing query (e.g. a correlated or a LATERAL reference)
	LogicalType InferOuterColumnType(const ColumnRefExpression &column_ref);
	//! The result types of a query (empty if they cannot be determined)
	vector<LogicalType> InferQueryTypes(const QueryNode &node);
	void CollectScopeTables(const TableRef &ref, vector<reference<const BaseTableRef>> &tables, bool &only_base_tables);
	//! The table a (base table) binding refers to at the current position in the query
	optional_ptr<TableCatalogEntry> FindBindingTable(const Identifier &binding);
	string WriteOrderingKey(const ParsedExpression &expr);
	//! "ordering" - whether the operands are ordered (e.g. <, BETWEEN), as opposed to compared for equality
	vector<string> WriteComparisonOperands(const vector<reference<const ParsedExpression>> &operands,
	                                       bool ordering = false);
	//! Write an expression that is ordered (ORDER BY key, operand of <, min, ...) - DuckDB orders strings
	//! byte-wise, so the C collation is pinned when the database collation orders them differently
	string WriteOrderedOperand(const string &expr_str, const LogicalType &type);
	string WriteOrderedExpression(const ParsedExpression &expr);
	string WriteStringOperand(const ParsedExpression &expr);
	string WriteCast(const ParsedExpression &child, const LogicalType &target_type);
	string WriteCast(const string &child_str, const LogicalType &source_type, const LogicalType &target_type);
	string WriteLength(const string &name, const ParsedExpression &child);

	string WriteQueryNode(const QueryNode &node);
	void WriteCreateTable(CreateStatement &statement, PostgresWriterResult &result);
	void WriteCreateView(CreateViewInfo &info, PostgresWriterResult &result);
	string WriteDrop(const DropInfo &info, PostgresWriterResult &result);
	void VerifyEntryExists(CatalogType type, const QualifiedName &name);
	string WriteAlter(const AlterInfo &info, PostgresWriterResult &result);
	string WriteQualifiedTableName(const QualifiedName &name);
	//! The Postgres schema and name of an entry
	pair<string, string> ResolveEntryName(const QualifiedName &name);
	string WriteSelectNode(const SelectNode &node);
	string WriteSetOperationNode(const SetOperationNode &node);
	string WriteRecursiveCTENode(const RecursiveCTENode &node);
	string WriteUpdateNode(const UpdateQueryNode &node);
	string WriteDeleteNode(const DeleteQueryNode &node);
	string WriteInsertNode(const InsertQueryNode &node);
	string WriteMergeNode(const MergeQueryNode &node);
	string WriteOnConflict(const OnConflictInfo &info, TableCatalogEntry &table, const Identifier &target_binding,
	                       const vector<reference<const ColumnDefinition>> &columns,
	                       optional_ptr<const ExpressionListRef> values_list,
	                       const vector<reference<const ColumnDefinition>> &value_columns);
	string WriteSetClause(const UpdateSetInfo &set_info, TableCatalogEntry &table, bool on_conflict);
	string WriteAssignment(const ParsedExpression &expr, TableCatalogEntry &table, const ColumnDefinition &column,
	                       optional_ptr<const LogicalType> source_type_override = nullptr);
	string WriteAssignmentCast(const string &expr_str, const LogicalType &source_type, TableCatalogEntry &table,
	                           const ColumnDefinition &column);
	string WriteColumnName(TableCatalogEntry &table, const ColumnDefinition &column);
	string WriteReturningList(const vector<unique_ptr<ParsedExpression>> &returning_list);
	TableCatalogEntry &GetTargetTable(const TableRef &ref);
	string WriteCTEMap(const CommonTableExpressionMap &cte_map);
	string WriteResultModifiers(const QueryNode &node,
	                            optional_ptr<const vector<unique_ptr<ParsedExpression>>> select_list = nullptr);
	//! "node" - the query whose result is ordered, nullptr for the ORDER BY of a window / aggregate
	string WriteOrderList(const vector<OrderByNode> &orders, optional_ptr<const QueryNode> node = nullptr);

	string WriteTableRef(const TableRef &ref, bool lateral = false);
	string WriteTableAlias(const TableRef &ref);

	string WriteExpression(const ParsedExpression &expr);
	string WriteExpressionList(const vector<unique_ptr<ParsedExpression>> &expressions, const string &separator = ", ");
	string WriteArgumentList(const vector<FunctionArgument> &arguments);
	string WriteFunction(const FunctionExpression &func);
	string WriteFunctionCall(const string &function_name, const vector<FunctionArgument> &children, bool distinct,
	                         const string &order_by);
	string WriteAggregate(const FunctionExpression &func, const string &name);
	bool TryWriteSpecialFunction(const string &name, const vector<FunctionArgument> &children, string &result);
	string WriteDatePart(const string &part, const ParsedExpression &arg);
	string WriteDateDiff(const string &part, const ParsedExpression &start, const ParsedExpression &end);
	string WriteRound(const vector<FunctionArgument> &children, bool half_even);
	string WriteDivision(const ParsedExpression &left, const ParsedExpression &right);
	bool HasDefaultDivisionSemantics();
	string WriteFrameOffset(const WindowExpression &window, const ParsedExpression &offset, const string &mode);
	string WriteWindow(const WindowExpression &window);
	string WriteOperator(const OperatorExpression &op);
	string WriteCase(const CaseExpression &case_expr);
	string WriteSubquery(const SubqueryExpression &subquery);
	string WriteColumnRef(const ColumnRefExpression &column_ref);
	string WriteConstant(const Value &value);
	string WriteType(const LogicalType &type);
	string WriteIdentifier(const Identifier &identifier);
	string WriteIdentifier(const string &identifier);

private:
	ClientContext &context;
	PostgresCatalog &catalog;
	//! The Postgres spelling of identifiers, keyed case-insensitively
	case_insensitive_map_t<string> identifier_spelling;
	//! The names of the columns of the tables referenced by the query
	case_insensitive_set_t catalog_columns;
	//! The types columns are read as by DuckDB, if they differ from their Postgres type ("?" if ambiguous)
	case_insensitive_map_t<string> column_read_types;
	//! The aliases of single-column table functions (without column aliases), and the names of the functions
	case_insensitive_map_t<string> table_function_aliases;
	//! The tables base table references are bound to
	case_insensitive_map_t<reference<TableCatalogEntry>> binding_tables;
	//! The names tables are bound to in the query (table names, aliases, CTE names)
	case_insensitive_set_t table_bindings;
	//! Identifiers which have multiple spellings in the Postgres catalog - written as specified in the query
	case_insensitive_set_t ambiguous_identifiers;
	//! The CTE names visible at the current position in the query
	vector<reference<const CommonTableExpressionMap>> cte_scopes;
	//! The tables in scope at the current position in the query (e.g. the FROM clause of the enclosing SELECT,
	//! or the target and FROM clause of an UPDATE)
	vector<vector<reference<const unique_ptr<TableRef>>>> table_scopes;
	//! Whether the query formats dates / times as text, which requires the ISO DateStyle
	bool requires_iso_date_style = false;
	//! Whether the result of the query depends on the session time zone
	bool requires_time_zone = false;
	//! Cache of inferred expression types
	unordered_map<const ParsedExpression *, LogicalType> type_cache;
	//! The SELECT nodes producing the query result (e.g. the children of a top-level UNION), and the
	//! types DuckDB binds the result to
	unordered_set<const QueryNode *> result_nodes;
	optional_ptr<const vector<LogicalType>> result_types;
	//! The nesting depth of the query node that is being written
	idx_t query_depth = 0;
	//! The target table of the ON CONFLICT DO UPDATE clause that is being written (if any)
	optional_ptr<TableCatalogEntry> on_conflict_target;
	idx_t on_conflict_depth = 0;
	string on_conflict_binding;
	//! Counter for generating aliases for unnamed subqueries
	idx_t unnamed_subquery_index = 0;
};

} // namespace duckdb
