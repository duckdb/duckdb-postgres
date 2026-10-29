#include "postgres_sql_writer.hpp"

#include "duckdb.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/expression/between_expression.hpp"
#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/expression/window_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/expression/default_expression.hpp"
#include "duckdb/parser/query_node/delete_query_node.hpp"
#include "duckdb/parser/query_node/insert_query_node.hpp"
#include "duckdb/parser/query_node/merge_query_node.hpp"
#include "duckdb/parser/statement/merge_into_statement.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"
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
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "storage/postgres_table_set.hpp"
#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/tableref/emptytableref.hpp"
#include "duckdb/parser/tableref/expressionlistref.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"

#include "duckdb/common/type_visitor.hpp"
#include "postgres_utils.hpp"
#include "storage/postgres_table_entry.hpp"
#include "storage/postgres_capabilities.hpp"
#include "storage/postgres_catalog.hpp"

namespace duckdb {

PostgresSQLWriter::PostgresSQLWriter(ClientContext &context, PostgresCatalog &catalog)
    : context(context), catalog(catalog) {
}

PostgresWriterResult PostgresSQLWriter::WriteQuery(ClientContext &context, PostgresCatalog &catalog, QueryNode &node,
                                                   optional_ptr<const vector<LogicalType>> result_types) {
	PostgresSQLWriter writer(context, catalog);
	writer.CollectNames(node);
	writer.CollectResultNodes(node);
	writer.result_types = result_types;
	PostgresWriterResult result;
	result.sql = writer.WriteQueryNode(node);
	result.requires_time_zone = writer.requires_time_zone;
	if (writer.requires_iso_date_style) {
		// reverted at the end of the transaction
		result.preliminary_queries.push_back("SET LOCAL DateStyle = 'ISO'");
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Local binding
//===--------------------------------------------------------------------===//
static void PostgresCollectCTENamesExpr(ParsedExpression &expr, identifier_set_t &cte_names) {
	if (expr.GetExpressionClass() == ExpressionClass::SUBQUERY) {
		PostgresSQLWriter::CollectCTENames(*expr.Cast<SubqueryExpression>().SubqueryMutable()->node, cte_names);
	}
	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](ParsedExpression &child) { PostgresCollectCTENamesExpr(child, cte_names); });
}

void PostgresSQLWriter::CollectCTENames(QueryNode &node, identifier_set_t &cte_names) {
	for (auto &cte : node.cte_map.map) {
		cte_names.insert(cte.first);
	}
	ParsedExpressionIterator::EnumerateQueryNodeChildren(
	    node, [&](unique_ptr<ParsedExpression> &child) { PostgresCollectCTENamesExpr(*child, cte_names); });
}

static void PostgresQualifyTablesExpr(ParsedExpression &expr, const Identifier &catalog_name,
                                      const identifier_set_t &cte_names) {
	if (expr.GetExpressionClass() == ExpressionClass::SUBQUERY) {
		PostgresSQLWriter::QualifyTables(*expr.Cast<SubqueryExpression>().SubqueryMutable()->node, catalog_name,
		                                 cte_names);
	}
	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](ParsedExpression &child) { PostgresQualifyTablesExpr(child, catalog_name, cte_names); });
}

void PostgresSQLWriter::QualifyTables(QueryNode &node, const Identifier &catalog_name,
                                      const identifier_set_t &cte_names) {
	if (node.type == QueryNodeType::INSERT_QUERY_NODE) {
		auto &insert = node.Cast<InsertQueryNode>();
		vector<Identifier> schema_path;
		if (!insert.qualified_name.Schema().empty()) {
			schema_path.push_back(insert.qualified_name.Schema());
		}
		insert.qualified_name =
		    QualifiedName::FromCatalogSchema(catalog_name, std::move(schema_path), insert.qualified_name.Name());
		if (insert.table_ref && insert.table_ref->type == TableReferenceType::BASE_TABLE) {
			// the catalog name is only stripped from the qualified name of the INSERT target
			insert.table_ref->Cast<BaseTableRef>().SetQualifiedName(insert.qualified_name);
		}
	}
	ParsedExpressionIterator::EnumerateQueryNodeChildren(
	    node, [&](unique_ptr<ParsedExpression> &child) { PostgresQualifyTablesExpr(*child, catalog_name, cte_names); },
	    [&](TableRef &ref) {
		    if (ref.type != TableReferenceType::BASE_TABLE) {
			    return;
		    }
		    auto &base = ref.Cast<BaseTableRef>();
		    auto &name = base.GetQualifiedName();
		    if (name.Path().size() == 1 && cte_names.count(name.Name()) > 0) {
			    // (possibly) a CTE reference
			    return;
		    }
		    if (name.Path().size() > 2) {
			    return;
		    }
		    vector<Identifier> schema_path;
		    if (!name.Schema().empty()) {
			    schema_path.push_back(name.Schema());
		    }
		    base.SetQualifiedName(QualifiedName::FromCatalogSchema(catalog_name, std::move(schema_path), name.Name()));
	    });
}

void PostgresSQLWriter::CollectScopeTables(const TableRef &ref, vector<reference<const BaseTableRef>> &tables,
                                           bool &only_base_tables) {
	switch (ref.type) {
	case TableReferenceType::BASE_TABLE:
		if (IsCTEReference(ref.Cast<BaseTableRef>())) {
			only_base_tables = false;
		} else {
			tables.push_back(ref.Cast<BaseTableRef>());
		}
		break;
	case TableReferenceType::JOIN: {
		auto &join = ref.Cast<JoinRef>();
		if (join.type == JoinType::SEMI || join.type == JoinType::ANTI || !join.using_columns.empty() ||
		    join.ref_type == JoinRefType::NATURAL) {
			// the bindings of the join differ from the bindings of the joined tables
			only_base_tables = false;
		}
		CollectScopeTables(*join.left, tables, only_base_tables);
		CollectScopeTables(*join.right, tables, only_base_tables);
		break;
	}
	default:
		only_base_tables = false;
		break;
	}
}

optional_ptr<TableCatalogEntry> PostgresSQLWriter::FindBindingTable(const Identifier &binding) {
	for (idx_t i = table_scopes.size(); i > 0; i--) {
		vector<reference<const BaseTableRef>> tables;
		bool only_base_tables = true;
		for (auto &table_ref : table_scopes[i - 1]) {
			if (!table_ref.get()) {
				continue;
			}
			if (table_ref.get()->alias == binding && table_ref.get()->type != TableReferenceType::BASE_TABLE) {
				// e.g. a subquery
				return nullptr;
			}
			CollectScopeTables(*table_ref.get(), tables, only_base_tables);
		}
		for (auto &base : tables) {
			auto base_binding = base.get().alias.empty() ? base.get().Table() : base.get().alias;
			if (base_binding == binding) {
				return LookupTable(base.get());
			}
		}
	}
	// e.g. a reference to a table in an outer query of a subquery in the FROM clause
	auto entry = binding_tables.find(binding.GetIdentifierName());
	if (entry != binding_tables.end()) {
		return &entry->second.get();
	}
	return nullptr;
}

LogicalType PostgresSQLWriter::InferType(const ParsedExpression &expr) {
	auto result = InferTypeInternal(expr);
	if (result.id() == LogicalTypeId::TIMESTAMP_TZ || result.id() == LogicalTypeId::TIME_TZ) {
		requires_time_zone = true;
	}
	return result;
}

LogicalType PostgresSQLWriter::InferTypeInternal(const ParsedExpression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::CONSTANT) {
		return expr.Cast<ConstantExpression>().GetLiteral().ToValue().type();
	}
	if (expr.GetExpressionClass() == ExpressionClass::COLUMN_REF && !table_scopes.empty()) {
		// fast path: a column of one of the base tables in scope
		auto &names = expr.Cast<ColumnRefExpression>().ColumnNames();
		if (names.size() == 1 && on_conflict_target && query_depth == on_conflict_depth &&
		    on_conflict_target->ColumnExists(names[0])) {
			// unqualified columns in ON CONFLICT DO UPDATE refer to the existing row
			return on_conflict_target->GetColumn(names[0]).Type();
		}
		vector<reference<const BaseTableRef>> tables;
		bool only_base_tables = true;
		for (auto &table_ref : table_scopes.back()) {
			if (table_ref.get()) {
				CollectScopeTables(*table_ref.get(), tables, only_base_tables);
			}
		}
		optional_ptr<TableCatalogEntry> found;
		idx_t matches = 0;
		for (auto &base : tables) {
			auto table = LookupTable(base.get());
			if (!table) {
				only_base_tables = false;
				continue;
			}
			auto binding = base.get().alias.empty() ? base.get().Table() : base.get().alias;
			if (names.size() == 1 && table->ColumnExists(names[0])) {
				found = table;
				matches++;
			} else if (names.size() == 2 && names[0] == binding && table->ColumnExists(names[1])) {
				return table->GetColumn(names[1]).Type();
			}
		}
		if (names.size() == 1 && only_base_tables && matches == 1) {
			return found->GetColumn(names[0]).Type();
		}
	}
	interval_t interval;
	if (PostgresCapabilities::TryGetIntervalLiteral(expr, interval)) {
		return LogicalType::INTERVAL;
	}
	auto entry = type_cache.find(&expr);
	if (entry != type_cache.end()) {
		return entry->second;
	}
	// bind a probe query that evaluates the expression over the FROM clause of the enclosing SELECT
	auto probe = make_uniq<SelectNode>();
	probe->select_list.push_back(expr.Copy());
	if (!table_scopes.empty()) {
		for (auto &table : table_scopes.back()) {
			auto table_copy = table.get()->Copy();
			if (!probe->from_table) {
				probe->from_table = std::move(table_copy);
				continue;
			}
			auto join = make_uniq<JoinRef>(JoinRefType::CROSS);
			join->left = std::move(probe->from_table);
			join->right = std::move(table_copy);
			probe->from_table = std::move(join);
		}
	}
	if (!probe->from_table) {
		probe->from_table = make_uniq<EmptyTableRef>();
	}
	for (idx_t i = cte_scopes.size(); i > 0; i--) {
		// inner CTEs shadow outer ones - insert those first
		for (auto &cte : cte_scopes[i - 1].get().map) {
			probe->cte_map.map.insert(cte.first, cte.second->Copy());
		}
	}
	identifier_set_t cte_names;
	CollectCTENames(*probe, cte_names);
	QualifyTables(*probe, catalog.GetName(), cte_names);
	LogicalType result = LogicalType::INVALID;
	try {
		auto binder = Binder::CreateBinder(context);
		auto bound = binder->Bind(*probe);
		if (bound.types.size() == 1) {
			result = bound.types[0];
		}
	} catch (std::exception &) {
		// e.g. a correlated column reference or a reference to a select-list alias
	}
	if (result.id() == LogicalTypeId::INVALID && expr.GetExpressionClass() == ExpressionClass::COLUMN_REF) {
		result = InferOuterColumnType(expr.Cast<ColumnRefExpression>());
	}
	type_cache[&expr] = result;
	return result;
}

vector<LogicalType> PostgresSQLWriter::InferQueryTypes(const QueryNode &node) {
	auto probe = node.Copy();
	probe->modifiers.clear();
	for (idx_t i = cte_scopes.size(); i > 0; i--) {
		for (auto &cte : cte_scopes[i - 1].get().map) {
			if (probe->cte_map.map.find(cte.first) == probe->cte_map.map.end()) {
				probe->cte_map.map.insert(cte.first, cte.second->Copy());
			}
		}
	}
	identifier_set_t cte_names;
	CollectCTENames(*probe, cte_names);
	QualifyTables(*probe, catalog.GetName(), cte_names);
	try {
		auto binder = Binder::CreateBinder(context);
		return binder->Bind(*probe).types;
	} catch (std::exception &) {
		// e.g. a correlated column reference
		return vector<LogicalType>();
	}
}

LogicalType PostgresSQLWriter::InferOuterColumnType(const ColumnRefExpression &column_ref) {
	auto &names = column_ref.ColumnNames();
	if (names.size() == 2) {
		auto table = FindBindingTable(names[0]);
		if (table && table->ColumnExists(names[1])) {
			return table->GetColumn(names[1]).Type();
		}
		return LogicalType::INVALID;
	}
	if (names.size() != 1 || table_scopes.size() < 2) {
		return LogicalType::INVALID;
	}
	// a correlated reference - the innermost enclosing query that has the column decides
	for (idx_t i = table_scopes.size() - 1; i > 0; i--) {
		vector<reference<const BaseTableRef>> tables;
		bool only_base_tables = true;
		for (auto &table_ref : table_scopes[i - 1]) {
			if (table_ref.get()) {
				CollectScopeTables(*table_ref.get(), tables, only_base_tables);
			}
		}
		optional_ptr<TableCatalogEntry> found;
		idx_t matches = 0;
		for (auto &base : tables) {
			auto table = LookupTable(base.get());
			if (!table) {
				only_base_tables = false;
				continue;
			}
			if (table->ColumnExists(names[0])) {
				found = table;
				matches++;
			}
		}
		if (matches == 1) {
			// the query was bound by DuckDB - the reference is not ambiguous
			return found->GetColumn(names[0]).Type();
		}
		if (matches > 1 || !only_base_tables) {
			// the column might be defined by a subquery of this scope
			return LogicalType::INVALID;
		}
	}
	return LogicalType::INVALID;
}

//===--------------------------------------------------------------------===//
// Identifier spelling
//===--------------------------------------------------------------------===//
void PostgresSQLWriter::AddCatalogColumnName(const string &name) {
	catalog_columns.insert(name);
	AddCatalogName(name);
}

void PostgresSQLWriter::AddCatalogName(const string &name) {
	if (ambiguous_identifiers.count(name) > 0) {
		return;
	}
	auto entry = identifier_spelling.find(name);
	if (entry == identifier_spelling.end()) {
		identifier_spelling[name] = name;
	} else if (entry->second != name) {
		// e.g. a table with both a column "a" and a column "A"
		identifier_spelling.erase(entry);
		ambiguous_identifiers.insert(name);
	}
}

void PostgresSQLWriter::AddQueryName(const Identifier &name) {
	auto &str = name.GetIdentifierName();
	if (str.empty() || ambiguous_identifiers.count(str) > 0) {
		return;
	}
	// names defined in the query (aliases, CTE names, ...) never override catalog names - the first
	// spelling wins
	if (identifier_spelling.find(str) == identifier_spelling.end()) {
		identifier_spelling[str] = str;
	}
}

optional_ptr<TableCatalogEntry> PostgresSQLWriter::LookupTable(const BaseTableRef &ref) {
	return LookupTable(ref.GetQualifiedName());
}

optional_ptr<TableCatalogEntry> PostgresSQLWriter::LookupTable(const QualifiedName &qualified_name) {
	if (qualified_name.Path().size() > 2) {
		// the catalog name has been stripped by the optimizer, nested schemas are not supported
		return nullptr;
	}
	Identifier schema_name = qualified_name.Schema();
	if (schema_name.empty()) {
		auto default_schema = catalog.GetDefaultSchema();
		schema_name = default_schema ? *default_schema : Identifier("public");
	}
	EntryLookupInfo lookup(CatalogType::TABLE_ENTRY,
	                       QualifiedName(catalog.GetName(), schema_name, qualified_name.Name()));
	auto entry = Catalog::GetEntry(context, lookup, OnEntryNotFound::RETURN_NULL);
	if (!entry || entry->type != CatalogType::TABLE_ENTRY) {
		return nullptr;
	}
	return &entry->Cast<TableCatalogEntry>();
}

bool PostgresSQLWriter::IsCTEReference(const BaseTableRef &ref) {
	if (ref.GetQualifiedName().Path().size() > 1) {
		return false;
	}
	for (auto &scope : cte_scopes) {
		if (scope.get().map.find(ref.Table()) != scope.get().map.end()) {
			return true;
		}
	}
	return false;
}

void PostgresSQLWriter::CollectNames(ParsedExpression &expr) {
	AddQueryName(expr.GetAlias());
	if (expr.GetExpressionClass() == ExpressionClass::SUBQUERY) {
		auto &subquery = expr.Cast<SubqueryExpression>();
		CollectNames(*subquery.SubqueryMutable()->node);
	}
	ParsedExpressionIterator::EnumerateChildren(expr, [&](ParsedExpression &child) { CollectNames(child); });
}

void PostgresSQLWriter::CollectNames(TableRef &ref) {
	AddQueryName(ref.alias);
	if (!ref.alias.empty()) {
		table_bindings.insert(ref.alias.GetIdentifierName());
	} else if (ref.type == TableReferenceType::BASE_TABLE) {
		table_bindings.insert(ref.Cast<BaseTableRef>().Table().GetIdentifierName());
	} else if (ref.type == TableReferenceType::TABLE_FUNCTION &&
	           ref.Cast<TableFunctionRef>().function->GetExpressionClass() == ExpressionClass::FUNCTION) {
		// the single column of the table function is named after the function
		table_bindings.insert(
		    ref.Cast<TableFunctionRef>().function->Cast<FunctionExpression>().FunctionName().GetIdentifierName());
	} else if (ref.type == TableReferenceType::EXPRESSION_LIST) {
		table_bindings.insert("valueslist");
	}
	for (auto &alias : ref.column_name_alias) {
		AddQueryName(alias);
	}
	if (ref.type == TableReferenceType::TABLE_FUNCTION && !ref.alias.empty() && ref.column_name_alias.empty() &&
	    ref.Cast<TableFunctionRef>().function->GetExpressionClass() == ExpressionClass::FUNCTION) {
		// DuckDB names the column of an aliased table function after the function and the alias, Postgres only
		// after the alias
		auto &function_name = ref.Cast<TableFunctionRef>().function->Cast<FunctionExpression>().FunctionName();
		table_function_aliases[ref.alias.GetIdentifierName()] = function_name.GetIdentifierName();
	}
	if (ref.type == TableReferenceType::BASE_TABLE) {
		auto table = LookupTable(ref.Cast<BaseTableRef>());
		if (table) {
			AddTableNames(*table);
			auto binding = ref.alias.empty() ? ref.Cast<BaseTableRef>().Table() : ref.alias;
			binding_tables.emplace(binding.GetIdentifierName(), *table);
		}
	}
}

static string PostgresNativeColumnType(TableCatalogEntry &table, const ColumnDefinition &column, bool &known);

//! DuckDB reads some Postgres types as different types (e.g. JSONB or INET as VARCHAR, unconstrained NUMERIC as DOUBLE)
//! - references to these columns are cast to the type DuckDB reads them as, so they behave the same
static string PostgresColumnReadType(TableCatalogEntry &table, const ColumnDefinition &column) {
	auto &pg_table = table.Cast<PostgresTableEntry>();
	auto index = column.Logical().index;
	if (index < pg_table.postgres_types.size() &&
	    pg_table.postgres_types[index].info == PostgresTypeAnnotation::NUMERIC_AS_DOUBLE) {
		return "DOUBLE PRECISION";
	}
	if (column.Type().id() == LogicalTypeId::VARCHAR) {
		bool known;
		if (!PostgresNativeColumnType(table, column, known).empty()) {
			return "TEXT";
		}
	}
	return string();
}

void PostgresSQLWriter::AddTableNames(TableCatalogEntry &table) {
	AddCatalogName(table.ParentSchema().name.GetIdentifierName());
	AddCatalogName(table.name.GetIdentifierName());
	auto &pg_table = table.Cast<PostgresTableEntry>();
	for (auto &column : table.GetColumns().Logical()) {
		auto &name = column.Name().GetIdentifierName();
		auto index = column.Logical().index;
		if (TypeVisitor::Contains(column.Type(), [](const LogicalType &type) {
			    return type.id() == LogicalTypeId::TIMESTAMP_TZ || type.id() == LogicalTypeId::TIME_TZ;
		    })) {
			// operations on the column depend on the time zone
			requires_time_zone = true;
		}
		auto read_type = PostgresColumnReadType(table, column);
		auto entry = column_read_types.find(name);
		if (entry == column_read_types.end()) {
			column_read_types[name] = read_type;
		} else if (entry->second != read_type) {
			// columns with this name are read differently in different tables
			entry->second = "?";
		}
		if (index < pg_table.postgres_names.size() && pg_table.postgres_names[index] != name) {
			// DuckDB renames columns whose names only differ in case (e.g. "ID" and "id" -> "ID" and "id:1")
			identifier_spelling[name] = pg_table.postgres_names[index];
			catalog_columns.insert(name);
			continue;
		}
		AddCatalogColumnName(name);
	}
}

string PostgresSQLWriter::WriteColumnName(TableCatalogEntry &table, const ColumnDefinition &column) {
	auto &pg_table = table.Cast<PostgresTableEntry>();
	auto index = column.Logical().index;
	auto &name =
	    index < pg_table.postgres_names.size() ? pg_table.postgres_names[index] : column.Name().GetIdentifierName();
	return PostgresUtils::WriteIdentifier(name);
}

void PostgresSQLWriter::CollectNames(QueryNode &node) {
	if (node.type == QueryNodeType::INSERT_QUERY_NODE) {
		auto table = LookupTable(node.Cast<InsertQueryNode>().qualified_name);
		if (table) {
			AddTableNames(*table);
			table_bindings.insert(table->name.GetIdentifierName());
		}
		table_bindings.insert("excluded");
	}
	for (auto &cte : node.cte_map.map) {
		AddQueryName(cte.first);
		table_bindings.insert(cte.first.GetIdentifierName());
		for (auto &alias : cte.second->aliases) {
			AddQueryName(alias);
		}
	}
	ParsedExpressionIterator::EnumerateQueryNodeChildren(
	    node, [&](unique_ptr<ParsedExpression> &child) { CollectNames(*child); },
	    [&](TableRef &ref) { CollectNames(ref); });
}

string PostgresSQLWriter::WriteIdentifier(const string &identifier) {
	auto entry = identifier_spelling.find(identifier);
	if (entry != identifier_spelling.end()) {
		return PostgresUtils::WriteIdentifier(entry->second);
	}
	return PostgresUtils::WriteIdentifier(identifier);
}

string PostgresSQLWriter::WriteIdentifier(const Identifier &identifier) {
	return WriteIdentifier(identifier.GetIdentifierName());
}

void PostgresSQLWriter::CollectResultNodes(const QueryNode &node) {
	switch (node.type) {
	case QueryNodeType::SELECT_NODE:
		result_nodes.insert(&node);
		break;
	case QueryNodeType::SET_OPERATION_NODE:
		for (auto &child : node.Cast<SetOperationNode>().children) {
			CollectResultNodes(*child);
		}
		break;
	default:
		break;
	}
}

//! Postgres produces unconstrained NUMERIC values for e.g. sum(BIGINT) or arithmetic on decimals, which are read back
//! as DOUBLE - cast these to the exact NUMERIC type that corresponds to the type DuckDB binds the expression to
static string PostgresExactNumericType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::HUGEINT:
		return "NUMERIC(38,0)";
	case LogicalTypeId::UHUGEINT:
		return "NUMERIC(39,0)";
	case LogicalTypeId::DECIMAL:
		return StringUtil::Format("NUMERIC(%d,%d)", DecimalType::GetWidth(type), DecimalType::GetScale(type));
	case LogicalTypeId::LIST: {
		auto child = PostgresExactNumericType(ListType::GetChildType(type));
		return child.empty() ? child : child + "[]";
	}
	default:
		return string();
	}
}

string PostgresSQLWriter::WriteResultCast(const string &expr, const LogicalType &type) {
	auto exact_type = PostgresExactNumericType(type);
	return exact_type.empty() ? expr : "CAST(" + expr + " AS " + exact_type + ")";
}

string PostgresSQLWriter::WriteResultCast(const string &expr, idx_t column_index) {
	if (!result_types || column_index >= result_types->size()) {
		return expr;
	}
	return WriteResultCast(expr, (*result_types)[column_index]);
}

//===--------------------------------------------------------------------===//
// Constants / Types
//===--------------------------------------------------------------------===//
string PostgresSQLWriter::WriteConstant(const Value &value) {
	if (value.IsNull()) {
		if (value.type().id() == LogicalTypeId::SQLNULL) {
			return "NULL";
		}
		// Postgres resolves untyped NULLs to TEXT in e.g. subqueries - keep the type
		try {
			return "CAST(NULL AS " + WriteType(value.type()) + ")";
		} catch (PostgresPushdownNotSupported &) {
			return "NULL";
		}
	}
	if (value.type().id() == LogicalTypeId::TIMESTAMP_TZ) {
		requires_time_zone = true;
	}
	switch (value.type().id()) {
	case LogicalTypeId::BOOLEAN:
		return BooleanValue::Get(value) ? "TRUE" : "FALSE";
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::UHUGEINT: {
		// literals beyond the BIGINT range are NUMERIC in Postgres
		auto str = value.ToString();
		return StringUtil::StartsWith(str, "-") ? "(" + str + ")" : str;
	}
	case LogicalTypeId::DECIMAL:
		// keep the width and scale - an unconstrained NUMERIC is read back as DOUBLE
		return "CAST(" + PostgresUtils::WriteLiteral(value.ToString()) + " AS " + WriteType(value.type()) + ")";
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
		// Postgres reads literals with a decimal point as NUMERIC - write them as typed literals
		// (this also covers inf and nan)
		return "CAST(" + PostgresUtils::WriteLiteral(value.ToString()) + " AS " + WriteType(value.type()) + ")";
	case LogicalTypeId::VARCHAR: {
		// untyped string literals are coerced to the type of the other operand, like DuckDB does
		auto &str = StringValue::Get(value);
		if (str.find('\\') != string::npos) {
			// escape string syntax - independent of the standard_conforming_strings setting
			return "E" + PostgresUtils::WriteLiteral(StringUtil::Replace(str, "\\", "\\\\"));
		}
		return PostgresUtils::WriteLiteral(str);
	}
	case LogicalTypeId::BLOB: {
		static const char *HEX_DIGITS = "0123456789abcdef";
		auto &data = StringValue::Get(value);
		string hex;
		for (auto c : data) {
			auto byte = static_cast<uint8_t>(c);
			hex += HEX_DIGITS[byte >> 4];
			hex += HEX_DIGITS[byte & 0x0F];
		}
		return "decode('" + hex + "', 'hex')";
	}
	case LogicalTypeId::DATE:
		return "DATE " + PostgresUtils::WriteLiteral(value.ToString());
	case LogicalTypeId::TIME:
		return "TIME " + PostgresUtils::WriteLiteral(value.ToString());
	case LogicalTypeId::TIMESTAMP:
		return "TIMESTAMP " + PostgresUtils::WriteLiteral(value.ToString());
	case LogicalTypeId::TIMESTAMP_TZ:
		// without a client context the value is formatted with an explicit UTC offset
		return "TIMESTAMPTZ " + PostgresUtils::WriteLiteral(value.ToString());
	case LogicalTypeId::INTERVAL:
		return "INTERVAL " + PostgresUtils::WriteLiteral(value.ToString());
	case LogicalTypeId::UUID:
		return "UUID " + PostgresUtils::WriteLiteral(value.ToString());
	default:
		throw InternalException("PostgresSQLWriter: unsupported constant type %s - should have been blocked by "
		                        "SupportsPushdown",
		                        value.type().ToString());
	}
}

string PostgresSQLWriter::WriteType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		return "BOOLEAN";
	case LogicalTypeId::SMALLINT:
		return "SMALLINT";
	case LogicalTypeId::INTEGER:
		return "INTEGER";
	case LogicalTypeId::BIGINT:
		return "BIGINT";
	case LogicalTypeId::FLOAT:
		return "REAL";
	case LogicalTypeId::DOUBLE:
		return "DOUBLE PRECISION";
	case LogicalTypeId::DECIMAL:
		return StringUtil::Format("NUMERIC(%d,%d)", DecimalType::GetWidth(type), DecimalType::GetScale(type));
	case LogicalTypeId::VARCHAR:
		return "TEXT";
	case LogicalTypeId::DATE:
		return "DATE";
	case LogicalTypeId::TIME:
		return "TIME";
	case LogicalTypeId::TIMESTAMP:
		return "TIMESTAMP";
	case LogicalTypeId::TIMESTAMP_TZ:
		return "TIMESTAMPTZ";
	case LogicalTypeId::INTERVAL:
		return "INTERVAL";
	case LogicalTypeId::UUID:
		return "UUID";
	case LogicalTypeId::BLOB:
		return "BYTEA";
	default:
		// e.g. user-defined types, which are verified when writing assignments
		throw PostgresPushdownNotSupported("unsupported type " + type.ToString());
	}
}

//===--------------------------------------------------------------------===//
// Type dependent translation
//===--------------------------------------------------------------------===//
static bool PostgresIsIntegerType(const LogicalType &type) {
	switch (type.id()) {
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
		return true;
	default:
		return false;
	}
}

static bool PostgresIsNumericType(const LogicalType &type) {
	return PostgresIsIntegerType(type) || type.id() == LogicalTypeId::DECIMAL || type.id() == LogicalTypeId::FLOAT ||
	       type.id() == LogicalTypeId::DOUBLE;
}

static bool PostgresIsDateTimeType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		return true;
	default:
		return false;
	}
}

//! Whether Postgres formats values of this type as text exactly like DuckDB casts them to VARCHAR - when used as
//! a concat / || argument (the output function is used) or explicitly cast to TEXT (booleans are cast to
//! 'true' / 'false', but output as 't' / 'f')
static bool PostgresHasIdenticalTextFormat(const LogicalType &type, bool explicit_cast = false) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		return explicit_cast;
	case LogicalTypeId::SQLNULL:
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::UUID:
	case LogicalTypeId::ENUM:
		return true;
	default:
		// doubles (DuckDB writes 3.0, Postgres 3), intervals (2 months vs 2 mons), blobs (escapes vs hex) differ
		// date / time types require the ISO DateStyle, see WriteCast
		return PostgresIsIntegerType(type);
	}
}

string PostgresSQLWriter::WriteCast(const ParsedExpression &child, const LogicalType &target_type) {
	auto source_type = InferType(child);
	if (source_type.id() == LogicalTypeId::INVALID) {
		throw PostgresPushdownNotSupported("cannot determine the source type of a cast");
	}
	return WriteCast(WriteExpression(child), source_type, target_type);
}

string PostgresSQLWriter::WriteCast(const string &child_str, const LogicalType &source_type,
                                    const LogicalType &target_type) {
	if (source_type == target_type) {
		return child_str;
	}
	auto cast = [&](const string &expr) {
		return "CAST(" + expr + " AS " + WriteType(target_type) + ")";
	};
	auto source_id = source_type.id();
	if (source_id == LogicalTypeId::SQLNULL) {
		return cast(child_str);
	}
	switch (target_type.id()) {
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
		if (source_id == LogicalTypeId::BOOLEAN) {
			// Postgres only has a BOOLEAN -> INTEGER cast
			return cast("CAST(" + child_str + " AS INTEGER)");
		}
		// note: both DuckDB and Postgres round doubles to the nearest even integer
		if (PostgresIsNumericType(source_type) || source_id == LogicalTypeId::VARCHAR) {
			return cast(child_str);
		}
		break;
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DECIMAL:
		if (PostgresIsNumericType(source_type) || source_id == LogicalTypeId::VARCHAR) {
			return cast(child_str);
		}
		break;
	case LogicalTypeId::BOOLEAN:
		if (PostgresIsIntegerType(source_type)) {
			return "(" + child_str + " <> 0)";
		}
		if (source_id == LogicalTypeId::VARCHAR) {
			return cast(child_str);
		}
		break;
	case LogicalTypeId::VARCHAR:
		if (PostgresHasIdenticalTextFormat(source_type, true)) {
			return cast(child_str);
		}
		if (PostgresIsDateTimeType(source_type)) {
			// DuckDB formats dates and times like Postgres' ISO DateStyle
			requires_iso_date_style = true;
			return cast(child_str);
		}
		break;
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		if (source_id == LogicalTypeId::DATE || source_id == LogicalTypeId::TIMESTAMP ||
		    source_id == LogicalTypeId::TIMESTAMP_TZ || source_id == LogicalTypeId::VARCHAR) {
			return cast(child_str);
		}
		break;
	case LogicalTypeId::TIME:
		if (source_id == LogicalTypeId::TIMESTAMP || source_id == LogicalTypeId::TIMESTAMP_TZ ||
		    source_id == LogicalTypeId::VARCHAR) {
			return cast(child_str);
		}
		break;
	case LogicalTypeId::UUID:
		if (source_id == LogicalTypeId::VARCHAR) {
			return cast(child_str);
		}
		break;
	default:
		break;
	}
	throw PostgresPushdownNotSupported("unsupported cast from " + source_type.ToString() + " to " +
	                                   target_type.ToString());
}

string PostgresSQLWriter::WriteLength(const string &name, const ParsedExpression &child) {
	auto type = InferType(child);
	auto child_str = WriteExpression(child);
	switch (type.id()) {
	case LogicalTypeId::VARCHAR:
		return name + "(" + child_str + ")";
	case LogicalTypeId::LIST:
		if (name == "length" && ListType::GetChildType(type).id() != LogicalTypeId::LIST) {
			// the number of elements of a one-dimensional array
			return "cardinality(" + child_str + ")";
		}
		break;
	default:
		break;
	}
	throw PostgresPushdownNotSupported("unsupported length function argument");
}

//===--------------------------------------------------------------------===//
// Scalar function translation
//===--------------------------------------------------------------------===//
static string PostgresFloat8(const string &expr) {
	return "CAST(" + expr + " AS DOUBLE PRECISION)";
}

//! Normalize a DuckDB date part name, returns an empty string for unsupported parts
static string PostgresNormalizeDatePart(const string &part) {
	static const case_insensitive_map_t<string> DATE_PARTS = {
	    {"year", "year"},
	    {"years", "year"},
	    {"month", "month"},
	    {"months", "month"},
	    {"day", "day"},
	    {"days", "day"},
	    {"hour", "hour"},
	    {"hours", "hour"},
	    {"minute", "minute"},
	    {"minutes", "minute"},
	    {"second", "second"},
	    {"seconds", "second"},
	    {"millisecond", "millisecond"},
	    {"milliseconds", "millisecond"},
	    {"microsecond", "microsecond"},
	    {"microseconds", "microsecond"},
	    {"quarter", "quarter"},
	    {"quarters", "quarter"},
	    {"week", "week"},
	    {"weeks", "week"},
	    {"weekofyear", "week"},
	    {"dow", "dow"},
	    {"dayofweek", "dow"},
	    {"isodow", "isodow"},
	    {"doy", "doy"},
	    {"dayofyear", "doy"},
	    {"isoyear", "isoyear"},
	    {"decade", "decade"},
	    {"decades", "decade"},
	    {"century", "century"},
	    {"centuries", "century"},
	    {"millennium", "millennium"},
	    {"millennia", "millennium"},
	    {"millenniums", "millennium"},
	    {"epoch", "epoch"},
	    {"julian", "julian"},
	};
	auto entry = DATE_PARTS.find(part);
	return entry == DATE_PARTS.end() ? string() : entry->second;
}

static bool PostgresIsTimestampType(const LogicalType &type) {
	return type.id() == LogicalTypeId::DATE || type.id() == LogicalTypeId::TIMESTAMP ||
	       type.id() == LogicalTypeId::TIMESTAMP_TZ;
}

string PostgresSQLWriter::WriteDatePart(const string &part, const ParsedExpression &arg) {
	auto type = InferType(arg);
	auto arg_str = WriteExpression(arg);
	bool supported;
	switch (type.id()) {
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		supported = true;
		break;
	case LogicalTypeId::TIME:
		supported = part == "hour" || part == "minute" || part == "second" || part == "millisecond" ||
		            part == "microsecond" || part == "epoch";
		break;
	case LogicalTypeId::INTERVAL:
		supported = part == "year" || part == "month" || part == "day" || part == "hour" || part == "minute" ||
		            part == "second" || part == "millisecond" || part == "microsecond" || part == "epoch";
		break;
	default:
		supported = false;
		break;
	}
	if (!supported) {
		throw PostgresPushdownNotSupported("unsupported date part");
	}
	if (part == "epoch" || part == "julian") {
		// DuckDB returns a DOUBLE
		return PostgresFloat8("EXTRACT(" + StringUtil::Upper(part) + " FROM " + arg_str + ")");
	}
	string field = part;
	if (part == "millisecond" || part == "microsecond") {
		field += "s";
	}
	string extract = "EXTRACT(" + StringUtil::Upper(field) + " FROM " + arg_str + ")";
	if (part == "second" || part == "millisecond") {
		// Postgres includes the fractional part
		extract = "trunc(" + extract + ")";
	}
	if (part == "year" && type.id() != LogicalTypeId::INTERVAL) {
		// DuckDB uses astronomical year numbering (0 is 1 BC), Postgres uses negative years for BC years
		extract = "(CASE WHEN " + extract + " < 0 THEN " + extract + " + 1 ELSE " + extract + " END)";
	}
	// DuckDB's date parts are BIGINTs, EXTRACT returns NUMERIC
	return "CAST(" + extract + " AS BIGINT)";
}

string PostgresSQLWriter::WriteDateDiff(const string &part, const ParsedExpression &start,
                                        const ParsedExpression &end) {
	auto start_type = InferType(start);
	auto end_type = InferType(end);
	auto is_supported = [](const LogicalType &type) {
		return type.id() == LogicalTypeId::DATE || type.id() == LogicalTypeId::TIMESTAMP;
	};
	if (!is_supported(start_type) || !is_supported(end_type)) {
		// note: DuckDB computes differences of TIMESTAMP WITH TIME ZONE values with calendar arithmetic
		throw PostgresPushdownNotSupported("unsupported date_diff argument");
	}
	auto start_str = WriteExpression(start);
	auto end_str = WriteExpression(end);
	auto extract = [](const string &field, const string &expr) {
		return "CAST(EXTRACT(" + field + " FROM " + expr + ") AS BIGINT)";
	};
	auto diff = [&](const string &field) {
		return "(" + extract(field, end_str) + " - " + extract(field, start_str) + ")";
	};
	auto months = [&](const string &expr) {
		return "(" + extract("YEAR", expr) + " * 12 + " + extract("MONTH", expr) + " - 1)";
	};
	// DuckDB counts the boundaries crossed - for units of time with a floor division of the epoch
	auto epoch_units = [&](const string &units) {
		return "CAST(floor(EXTRACT(EPOCH FROM " + end_str + ") * " + units + ") - floor(EXTRACT(EPOCH FROM " +
		       start_str + ") * " + units + ") AS BIGINT)";
	};
	auto days = "(CAST(" + end_str + " AS DATE) - CAST(" + start_str + " AS DATE))";
	if (part == "year" || part == "isoyear") {
		return diff(StringUtil::Upper(part));
	}
	if (part == "decade" || part == "century" || part == "millennium") {
		auto divisor = part == "decade" ? "10" : part == "century" ? "100" : "1000";
		return "(" + extract("YEAR", end_str) + " / " + divisor + " - " + extract("YEAR", start_str) + " / " + divisor +
		       ")";
	}
	if (part == "month") {
		return "(" + months(end_str) + " - " + months(start_str) + ")";
	}
	if (part == "quarter") {
		return "(" + months(end_str) + " / 3 - " + months(start_str) + " / 3)";
	}
	if (part == "day") {
		return "CAST(" + days + " AS BIGINT)";
	}
	if (part == "week") {
		// weeks count the distance in days, not the Monday crossings
		return "CAST(" + days + " / 7 AS BIGINT)";
	}
	if (part == "hour") {
		return epoch_units("1 / 3600.0");
	}
	if (part == "minute") {
		return epoch_units("1 / 60.0");
	}
	if (part == "second") {
		return epoch_units("1");
	}
	if (part == "millisecond") {
		return epoch_units("1000");
	}
	if (part == "microsecond") {
		return epoch_units("1000000");
	}
	throw PostgresPushdownNotSupported("unsupported date_diff part");
}

static const char *PostgresDateTruncParts[] = {"microseconds", "milliseconds", "second",     "minute",  "hour",
                                               "day",          "week",         "month",      "quarter", "year",
                                               "decade",       "century",      "millennium", nullptr};

string PostgresSQLWriter::WriteRound(const vector<FunctionArgument> &children, bool half_even) {
	auto &arg = children[0].GetExpression();
	auto type = InferType(arg);
	int64_t precision = 0;
	if (children.size() == 2) {
		Value value;
		if (!PostgresCapabilities::TryGetConstantValue(children[1].GetExpression(), value)) {
			throw InternalException("PostgresSQLWriter: round precision must be a constant");
		}
		precision = BigIntValue::Get(value.DefaultCastAs(LogicalType::BIGINT));
	}
	auto arg_str = WriteExpression(arg);
	if (PostgresIsIntegerType(type)) {
		if (precision >= 0) {
			return arg_str;
		}
		if (half_even) {
			throw PostgresPushdownNotSupported("unsupported round_even argument");
		}
		// NUMERIC rounding rounds half away from zero, like DuckDB - the result keeps the integer type
		return "CAST(round(" + arg_str + ", " + to_string(precision) + ") AS " + WriteType(type) + ")";
	}
	if (type.id() == LogicalTypeId::DECIMAL && !half_even) {
		return "round(" + arg_str + ", " + to_string(precision) + ")";
	}
	if (type.id() != LogicalTypeId::DOUBLE || precision > 22 || precision < -22) {
		throw PostgresPushdownNotSupported("unsupported round argument");
	}
	// DuckDB rounds doubles as std::round(x * 10^p) / 10^p - Postgres' round(DOUBLE PRECISION) rounds half to
	// even (like DuckDB's round_even), half away from zero is emulated on the (exact) fractional part
	string modifier = "CAST('1e" + to_string(precision < 0 ? -precision : precision) + "' AS DOUBLE PRECISION)";
	string scaled = precision == 0 ? arg_str : "(" + arg_str + (precision > 0 ? " * " : " / ") + modifier + ")";
	string rounded;
	if (half_even) {
		rounded = "round(" + scaled + ")";
	} else {
		rounded = "(CASE WHEN abs(" + scaled + " - trunc(" + scaled + ")) >= 0.5 THEN trunc(" + scaled + ") + sign(" +
		          scaled + ") ELSE trunc(" + scaled + ") END)";
	}
	if (precision == 0) {
		return rounded;
	}
	return "(" + rounded + (precision > 0 ? " / " : " * ") + modifier + ")";
}

//! DuckDB's division of numbers is a DOUBLE division (Postgres performs integer division on integers) - and
//! dividing by zero yields inf / NaN where Postgres raises an error
bool PostgresSQLWriter::HasDefaultDivisionSemantics() {
	Value ieee_floating_point_ops;
	if (context.TryGetCurrentSetting("ieee_floating_point_ops", ieee_floating_point_ops) &&
	    !ieee_floating_point_ops.IsNull() && !BooleanValue::Get(ieee_floating_point_ops)) {
		return false;
	}
	Value error_on_division_by_zero;
	if (context.TryGetCurrentSetting("error_on_division_by_zero", error_on_division_by_zero) &&
	    !error_on_division_by_zero.IsNull() && !BooleanValue::Get(error_on_division_by_zero)) {
		return false;
	}
	return true;
}

string PostgresSQLWriter::WriteDivision(const ParsedExpression &left, const ParsedExpression &right) {
	if (!HasDefaultDivisionSemantics()) {
		throw PostgresPushdownNotSupported("non-default division semantics");
	}
	auto x = PostgresFloat8(WriteExpression(left));
	auto y = WriteExpression(right);
	Value value;
	if (PostgresCapabilities::TryGetConstantValue(right, value)) {
		auto double_value = value.DefaultTryCastAs(LogicalType::DOUBLE);
		if (double_value && !double_value->IsNull() && DoubleValue::Get(*double_value) != 0) {
			// division by a non-zero constant
			return "(" + x + " / " + y + ")";
		}
	}
	y = PostgresFloat8(y);
	// note: Postgres considers NaN equal to itself and larger than any other value
	return "(CASE WHEN " + y + " = 0 THEN (CASE WHEN " + x + " = 'NaN' OR " + x +
	       " = 0 THEN CAST('NaN' AS DOUBLE PRECISION) WHEN " + x +
	       " > 0 THEN CAST('Infinity' AS DOUBLE PRECISION) WHEN " + x +
	       " < 0 THEN CAST('-Infinity' AS DOUBLE PRECISION) END) ELSE " + x + " / " + y + " END)";
}

bool PostgresSQLWriter::TryWriteSpecialFunction(const string &name, const vector<FunctionArgument> &children,
                                                string &result) {
	auto arg = [&](idx_t i) -> const ParsedExpression & {
		return children[i].GetExpression();
	};
	auto require_types = [&](std::function<bool(const LogicalType &)> check) {
		for (auto &child : children) {
			if (!check(InferType(child.GetExpression()))) {
				throw PostgresPushdownNotSupported("unsupported argument type for " + name);
			}
		}
	};
	auto is_string = [](const LogicalType &type) {
		return type.id() == LogicalTypeId::VARCHAR || type.id() == LogicalTypeId::SQLNULL;
	};
	auto is_number = [](const LogicalType &type) {
		return PostgresIsNumericType(type) || type.id() == LogicalTypeId::SQLNULL;
	};
	auto is_integer = [](const LogicalType &type) {
		return PostgresIsIntegerType(type) || type.id() == LogicalTypeId::SQLNULL;
	};
	auto literal_string = [&](idx_t i) {
		Value value;
		if (!PostgresCapabilities::TryGetConstantValue(arg(i), value) || value.IsNull()) {
			throw InternalException("PostgresSQLWriter: expected a string literal");
		}
		return StringValue::Get(value.DefaultCastAs(LogicalType::VARCHAR));
	};
	if (name == "lower" || name == "lcase") {
		require_types(is_string);
		result = "lower(" + WriteExpression(arg(0)) + ")";
		return true;
	}
	if (name == "~~*" || name == "!~~*") {
		require_types(is_string);
		result = "(" + WriteExpression(arg(0)) + (name == "~~*" ? " ILIKE " : " NOT ILIKE ") + WriteExpression(arg(1)) +
		         " ESCAPE '')";
		return true;
	}
	if (name == "contains") {
		// DuckDB's contains also checks list / map membership
		require_types(is_string);
		result = "(strpos(" + WriteExpression(arg(0)) + ", " + WriteExpression(arg(1)) + ") > 0)";
		return true;
	}
	if (name == "ends_with" || name == "suffix") {
		require_types(is_string);
		auto str = WriteExpression(arg(0));
		auto suffix = WriteExpression(arg(1));
		result = "(right(" + str + ", length(" + suffix + ")) = " + suffix + ")";
		return true;
	}
	if (name == "^@") {
		require_types(is_string);
		result = WriteFunctionCall("starts_with", children, false, string());
		return true;
	}
	if (name == "translate") {
		require_types(is_string);
		result = name + "(" + WriteArgumentList(children) + ")";
		return true;
	}
	if (name == "unicode") {
		require_types(is_string);
		// DuckDB returns -1 for an empty string, Postgres' ascii returns 0
		auto x = WriteExpression(arg(0));
		result = "(CASE WHEN " + x + " = '' THEN -1 ELSE ascii(" + x + ") END)";
		return true;
	}
	if (name == "trunc" && children.size() == 1) {
		auto type = InferType(arg(0));
		if (PostgresIsIntegerType(type)) {
			// DuckDB returns the integer itself - Postgres would compute in DOUBLE PRECISION
			result = WriteExpression(arg(0));
			return true;
		}
		return false;
	}
	if (name == "chr") {
		require_types(is_integer);
		result = "chr(CAST(" + WriteExpression(arg(0)) + " AS INTEGER))";
		return true;
	}
	if (name == "sha256") {
		auto type = InferType(arg(0));
		auto arg_str = WriteExpression(arg(0));
		if (type.id() == LogicalTypeId::VARCHAR) {
			arg_str = "convert_to(" + arg_str + ", 'UTF8')";
		} else if (type.id() != LogicalTypeId::BLOB) {
			throw PostgresPushdownNotSupported("unsupported sha256 argument");
		}
		result = "encode(sha256(" + arg_str + "), 'hex')";
		return true;
	}
	if (name == "sqrt") {
		require_types(is_number);
		// Postgres raises an error where DuckDB yields NaN
		auto x = PostgresFloat8(WriteExpression(arg(0)));
		result = "(CASE WHEN " + x + " < 0 THEN CAST('NaN' AS DOUBLE PRECISION) ELSE sqrt(" + x + ") END)";
		return true;
	}
	if (name == "ln" || ((name == "log" || name == "log10") && children.size() == 1)) {
		require_types(is_number);
		// Postgres raises an error where DuckDB yields -inf / NaN
		auto x = PostgresFloat8(WriteExpression(arg(0)));
		result = "(CASE WHEN " + x + " = 0 THEN CAST('-Infinity' AS DOUBLE PRECISION) WHEN " + x +
		         " < 0 THEN CAST('NaN' AS DOUBLE PRECISION) ELSE " + (name == "ln" ? "ln(" : "log(") + x + ") END)";
		return true;
	}
	if (name == "log") {
		// the two argument variant is only available for NUMERIC in Postgres
		throw PostgresPushdownNotSupported("unsupported log");
	}
	if (name == "exp") {
		require_types(is_number);
		// Postgres raises an error on underflow / overflow where DuckDB yields 0 / inf
		auto x = PostgresFloat8(WriteExpression(arg(0)));
		result = "(CASE WHEN " + x + " < -745.2 THEN CAST(0 AS DOUBLE PRECISION) WHEN " + x + " > 709.79 AND " + x +
		         " <> 'NaN' THEN CAST('Infinity' AS DOUBLE PRECISION) ELSE exp(" + x + ") END)";
		return true;
	}
	if (name == "pow" || name == "power" || name == "^" || name == "**") {
		require_types(is_number);
		auto x = PostgresFloat8(WriteExpression(arg(0)));
		auto y = PostgresFloat8(WriteExpression(arg(1)));
		// Postgres raises an error where DuckDB yields inf / NaN
		// (C's pow(-inf, y) is inf for a positive and 0 for a negative non-integer y)
		auto fractional = y + " <> trunc(" + y + ")";
		result = "(CASE WHEN " + x + " = 0 AND " + y + " < 0 THEN CAST('Infinity' AS DOUBLE PRECISION) WHEN " + x +
		         " = '-Infinity' AND " + fractional + " THEN (CASE WHEN " + y +
		         " > 0 THEN CAST('Infinity' AS DOUBLE PRECISION) ELSE CAST(0 AS DOUBLE PRECISION) END) WHEN " + x +
		         " < 0 AND " + fractional + " THEN CAST('NaN' AS DOUBLE PRECISION) ELSE power(" + x + ", " + y +
		         ") END)";
		return true;
	}
	if (name == "isnan" || name == "isinf" || name == "isfinite") {
		auto type = InferType(arg(0));
		auto x = WriteExpression(arg(0));
		if (type.id() == LogicalTypeId::FLOAT || type.id() == LogicalTypeId::DOUBLE) {
			// Postgres considers NaN equal to itself
			auto is_nan = "(" + x + " = 'NaN')";
			auto is_inf = "(abs(" + x + ") = 'Infinity')";
			if (name == "isnan") {
				result = is_nan;
			} else if (name == "isinf") {
				result = is_inf;
			} else {
				result = "(NOT (" + is_nan + " OR " + is_inf + "))";
			}
			return true;
		}
		if (PostgresIsTimestampType(type) && name != "isnan") {
			result = name == "isfinite" ? "isfinite(" + x + ")" : "(NOT isfinite(" + x + "))";
			return true;
		}
		throw PostgresPushdownNotSupported("unsupported argument for " + name);
	}
	if (name == "gcd" || name == "lcm" || name == "greatest_common_divisor" || name == "least_common_multiple") {
		require_types(is_integer);
		result = string(name == "gcd" || name == "greatest_common_divisor" ? "gcd(" : "lcm(") +
		         WriteArgumentList(children) + ")";
		return true;
	}
	if (name == "xor" || name == "&" || name == "|" || name == "~") {
		require_types(is_integer);
		if (children.size() == 1) {
			result = "(~" + WriteExpression(arg(0)) + ")";
		} else {
			auto op = name == "xor" ? string("#") : name;
			result = "(" + WriteExpression(arg(0)) + " " + op + " " + WriteExpression(arg(1)) + ")";
		}
		return true;
	}
	if (name == "round" || name == "round_even" || name == "roundbankers") {
		result = WriteRound(children, name != "round");
		return true;
	}
	if (name == "date_part" || name == "datepart") {
		auto part = PostgresNormalizeDatePart(literal_string(0));
		if (part.empty()) {
			throw PostgresPushdownNotSupported("unsupported date part");
		}
		result = WriteDatePart(part, arg(1));
		return true;
	}
	static const case_insensitive_map_t<string> PART_FUNCTIONS = {{"year", "year"},
	                                                              {"month", "month"},
	                                                              {"day", "day"},
	                                                              {"dayofmonth", "day"},
	                                                              {"hour", "hour"},
	                                                              {"minute", "minute"},
	                                                              {"second", "second"},
	                                                              {"millisecond", "millisecond"},
	                                                              {"microsecond", "microsecond"},
	                                                              {"quarter", "quarter"},
	                                                              {"dayofweek", "dow"},
	                                                              {"isodow", "isodow"},
	                                                              {"dayofyear", "doy"},
	                                                              {"week", "week"},
	                                                              {"weekofyear", "week"},
	                                                              {"isoyear", "isoyear"},
	                                                              {"decade", "decade"},
	                                                              {"century", "century"},
	                                                              {"millennium", "millennium"},
	                                                              {"epoch", "epoch"}};
	auto part_entry = PART_FUNCTIONS.find(name);
	if (part_entry != PART_FUNCTIONS.end()) {
		result = WriteDatePart(part_entry->second, arg(0));
		return true;
	}
	if (name == "epoch_ms") {
		auto type = InferType(arg(0));
		if (!PostgresIsTimestampType(type)) {
			// epoch_ms(BIGINT) converts milliseconds to a timestamp
			throw PostgresPushdownNotSupported("unsupported epoch_ms argument");
		}
		result = "CAST(floor(EXTRACT(EPOCH FROM " + WriteExpression(arg(0)) + ") * 1000) AS BIGINT)";
		return true;
	}
	if (name == "date_diff" || name == "datediff") {
		auto part = PostgresNormalizeDatePart(literal_string(0));
		result = WriteDateDiff(part, arg(1), arg(2));
		return true;
	}
	if (name == "date_trunc" || name == "datetrunc") {
		auto part = PostgresNormalizeDatePart(literal_string(0));
		if (part == "millisecond" || part == "microsecond") {
			part += "s";
		}
		bool supported_part = false;
		for (idx_t i = 0; PostgresDateTruncParts[i]; i++) {
			if (part == PostgresDateTruncParts[i]) {
				supported_part = true;
			}
		}
		auto type = InferType(arg(1));
		auto x = WriteExpression(arg(1));
		if (!supported_part || !PostgresIsTimestampType(type)) {
			throw PostgresPushdownNotSupported("unsupported date_trunc");
		}
		if (type.id() == LogicalTypeId::DATE) {
			// DuckDB truncates dates to a TIMESTAMP - Postgres would pick the TIMESTAMP WITH TIME ZONE overload
			x = "CAST(" + x + " AS TIMESTAMP)";
		}
		result = "date_trunc('" + part + "', " + x + ")";
		return true;
	}
	if (name == "last_day") {
		auto type = InferType(arg(0));
		if (type.id() != LogicalTypeId::DATE && type.id() != LogicalTypeId::TIMESTAMP) {
			throw PostgresPushdownNotSupported("unsupported last_day argument");
		}
		result = "CAST(date_trunc('month', CAST(" + WriteExpression(arg(0)) +
		         " AS TIMESTAMP)) + INTERVAL '1 month' - INTERVAL '1 day' AS DATE)";
		return true;
	}
	if (name == "make_date") {
		require_types(is_integer);
		if (children.size() != 3) {
			throw PostgresPushdownNotSupported("unsupported make_date");
		}
		// DuckDB uses astronomical year numbering (0 is 1 BC), Postgres uses negative years for BC years (-1 is 1 BC)
		auto year = "CAST(" + WriteExpression(arg(0)) + " AS INTEGER)";
		result = "make_date((CASE WHEN " + year + " <= 0 THEN " + year + " - 1 ELSE " + year + " END), CAST(" +
		         WriteExpression(arg(1)) + " AS INTEGER), CAST(" + WriteExpression(arg(2)) + " AS INTEGER))";
		return true;
	}
	if (name == "to_timestamp") {
		require_types(is_number);
		result = "to_timestamp(" + PostgresFloat8(WriteExpression(arg(0))) + ")";
		return true;
	}
	return false;
}

//===--------------------------------------------------------------------===//
// Expressions
//===--------------------------------------------------------------------===//
string PostgresSQLWriter::WriteExpressionList(const vector<unique_ptr<ParsedExpression>> &expressions,
                                              const string &separator) {
	string result;
	for (auto &expr : expressions) {
		if (!result.empty()) {
			result += separator;
		}
		result += WriteExpression(*expr);
	}
	return result;
}

string PostgresSQLWriter::WriteColumnRef(const ColumnRefExpression &column_ref) {
	auto &names = column_ref.ColumnNames();
	if (names.size() == 1 && catalog_columns.count(names[0].GetIdentifierName()) == 0) {
		// SQL value functions without parentheses are parsed as column references - they are evaluated by Postgres
		// (at the start of the Postgres transaction, in the DuckDB time zone)
		static const case_insensitive_map_t<string> VALUE_FUNCTIONS = {{"current_date", "CURRENT_DATE"},
		                                                               {"current_timestamp", "CURRENT_TIMESTAMP"},
		                                                               {"localtimestamp", "LOCALTIMESTAMP"}};
		auto entry = VALUE_FUNCTIONS.find(names[0].GetIdentifierName());
		if (entry != VALUE_FUNCTIONS.end()) {
			requires_time_zone = true;
			return entry->second;
		}
	}
	if (names.size() == 2) {
		auto entry = table_function_aliases.find(names[0].GetIdentifierName());
		if (entry != table_function_aliases.end() &&
		    StringUtil::CIEquals(entry->second, names[1].GetIdentifierName())) {
			return WriteIdentifier(names[0]) + "." + WriteIdentifier(names[0]);
		}
	}
	if (names.size() == 1 && catalog_columns.count(names[0].GetIdentifierName()) == 0) {
		for (auto &entry : table_function_aliases) {
			if (StringUtil::CIEquals(entry.second, names[0].GetIdentifierName())) {
				return WriteIdentifier(entry.first);
			}
		}
	}
	if (names.size() == 1 && on_conflict_target && query_depth == on_conflict_depth &&
	    on_conflict_target->ColumnExists(names[0])) {
		// unqualified columns in ON CONFLICT DO UPDATE refer to the existing row in DuckDB - in Postgres they could
		// also refer to the excluded row
		auto &column = on_conflict_target->GetColumn(names[0]);
		auto result = on_conflict_binding + "." + WriteColumnName(*on_conflict_target, column);
		auto read_type = PostgresColumnReadType(*on_conflict_target, column);
		return read_type.empty() ? result : "CAST(" + result + " AS " + read_type + ")";
	}
	// figure out how many leading names qualify the column (schema.table.column / table.column) - any
	// remaining names are struct (composite type) field accesses
	idx_t column_index = 0;
	if (names.size() >= 2 && table_bindings.count(names[0].GetIdentifierName()) > 0) {
		column_index = 1;
	} else if (names.size() >= 3 && table_bindings.count(names[1].GetIdentifierName()) > 0) {
		column_index = 2;
	}
	optional_ptr<TableCatalogEntry> binding_table;
	if (column_index > 0) {
		binding_table = FindBindingTable(names[column_index - 1]);
		if (binding_table && !binding_table->ColumnExists(names[column_index])) {
			binding_table = nullptr;
		}
	} else if (ambiguous_identifiers.count(names[0].GetIdentifierName()) > 0) {
		// the column has different spellings in different tables - which one is referenced is not known
		throw PostgresPushdownNotSupported("ambiguous column name " + names[0].GetIdentifierName());
	}
	string result;
	for (idx_t i = 0; i <= column_index; i++) {
		if (i > 0) {
			result += ".";
		}
		if (i == column_index && binding_table) {
			result += WriteColumnName(*binding_table, binding_table->GetColumn(names[i]));
		} else {
			result += WriteIdentifier(names[i]);
		}
	}
	for (idx_t i = column_index + 1; i < names.size(); i++) {
		// Postgres requires parentheses for field accesses
		result = "(" + result + ")." + WriteIdentifier(names[i]);
	}
	if (column_index + 1 == names.size()) {
		string read_type;
		auto &column_name = names[column_index];
		if (column_index > 0) {
			if (binding_table) {
				read_type = PostgresColumnReadType(*binding_table, binding_table->GetColumn(column_name));
			}
		} else {
			auto entry = column_read_types.find(column_name.GetIdentifierName());
			if (entry != column_read_types.end()) {
				read_type = entry->second;
			}
		}
		if (read_type == "?") {
			throw PostgresPushdownNotSupported("ambiguous column reference " + column_name.GetIdentifierName());
		}
		if (!read_type.empty()) {
			result = "CAST(" + result + " AS " + read_type + ")";
		}
	}
	return result;
}

string PostgresSQLWriter::WriteArgumentList(const vector<FunctionArgument> &arguments) {
	string result;
	for (auto &argument : arguments) {
		if (!result.empty()) {
			result += ", ";
		}
		result += WriteExpression(argument.GetExpression());
	}
	return result;
}

static bool PostgresIsAggregateName(const string &name) {
	static const case_insensitive_set_t REWRITTEN_AGGREGATES = {"count_if",      "countif",       "median",  "quantile",
	                                                            "quantile_cont", "quantile_disc", "arg_min", "arg_max",
	                                                            "min_by",        "max_by"};
	return REWRITTEN_AGGREGATES.count(name) > 0;
}

//! Whether Postgres arrays of this type are read back as the corresponding DuckDB list
static bool PostgresSupportsArrayElement(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::UUID:
		return true;
	default:
		return false;
	}
}

//! How values of a type are ordered with respect to strings
enum class PostgresStringOrdering : uint8_t {
	//! no strings are compared
	NONE,
	//! strings (or lists of strings) - the collation can be pinned
	COLLATABLE,
	//! strings nested in other types (or an unknown type) - the collation cannot be pinned
	NOT_COLLATABLE
};

static PostgresStringOrdering PostgresGetStringOrdering(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::VARCHAR:
		return PostgresStringOrdering::COLLATABLE;
	case LogicalTypeId::LIST:
		// arrays of text are collatable
		return PostgresGetStringOrdering(ListType::GetChildType(type));
	case LogicalTypeId::ARRAY:
		return PostgresGetStringOrdering(ArrayType::GetChildType(type));
	case LogicalTypeId::INVALID:
	case LogicalTypeId::UNKNOWN:
	case LogicalTypeId::ANY:
		return PostgresStringOrdering::NOT_COLLATABLE;
	default:
		if (TypeVisitor::Contains(type, LogicalTypeId::VARCHAR)) {
			return PostgresStringOrdering::NOT_COLLATABLE;
		}
		return PostgresStringOrdering::NONE;
	}
}

string PostgresSQLWriter::WriteFunctionCall(const string &function_name, const vector<FunctionArgument> &children,
                                            bool distinct, const string &order_by) {
	auto name = StringUtil::Lower(function_name);
	static const case_insensitive_map_t<vector<idx_t>> INTEGER_ARGUMENTS = {
	    {"left", {1}},   {"right", {1}},        {"lpad", {1}},     {"rpad", {1}},
	    {"repeat", {1}}, {"substring", {1, 2}}, {"substr", {1, 2}}};
	auto integer_arguments = INTEGER_ARGUMENTS.find(name);
	if (integer_arguments != INTEGER_ARGUMENTS.end()) {
		// these functions only accept INTEGER arguments in Postgres
		if ((name == "lpad" || name == "rpad") && children.size() == 3) {
			Value fill;
			if (!PostgresCapabilities::TryGetConstantValue(children[2].GetExpression(), fill) || fill.IsNull() ||
			    StringValue::Get(fill.DefaultCastAs(LogicalType::VARCHAR)).empty()) {
				// DuckDB raises an error for an empty fill string, Postgres returns the string
				throw PostgresPushdownNotSupported("unsupported padding");
			}
		}
		vector<string> args;
		for (idx_t i = 0; i < children.size(); i++) {
			auto &child = children[i].GetExpression();
			auto arg = WriteExpression(child);
			bool is_integer_argument = false;
			for (auto index : integer_arguments->second) {
				is_integer_argument = is_integer_argument || index == i;
			}
			if (is_integer_argument) {
				Value value;
				if (PostgresCapabilities::TryGetConstantValue(child, value)) {
					auto integer_value = value.DefaultTryCastAs(LogicalType::INTEGER);
					if (!integer_value) {
						throw PostgresPushdownNotSupported("argument out of range");
					}
					arg = WriteConstant(*integer_value);
				} else {
					auto type = InferType(child);
					if (!PostgresIsIntegerType(type)) {
						throw PostgresPushdownNotSupported("unsupported argument type for " + name);
					}
					if (type.id() != LogicalTypeId::INTEGER) {
						arg = "CAST(" + arg + " AS INTEGER)";
					}
				}
			}
			args.push_back(std::move(arg));
		}
		if (name == "substring") {
			name = "substr";
		}
		return name + "(" + StringUtil::Join(args, ", ") + ")";
	}
	if ((name == "count_star" || name == "count") && children.empty()) {
		return "count(*)";
	}
	if (name == "string_agg" || name == "group_concat" || name == "listagg") {
		if (InferType(children[0].GetExpression()).id() != LogicalTypeId::VARCHAR) {
			throw PostgresPushdownNotSupported("unsupported string_agg argument");
		}
		string result =
		    "string_agg(" + string(distinct ? "DISTINCT " : "") + WriteExpression(children[0].GetExpression()) + ", ";
		// DuckDB's default separator is a comma
		result += children.size() > 1 ? WriteExpression(children[1].GetExpression()) : "','";
		return result + order_by + ")";
	}
	if (name == "array_agg" || name == "list") {
		if (!PostgresSupportsArrayElement(InferType(children[0].GetExpression()))) {
			throw PostgresPushdownNotSupported("unsupported array_agg argument");
		}
		name = "array_agg";
	}
	static const case_insensitive_set_t TYPE_CHECKED_AGGREGATES = {
	    "min",         "max",       "sum",       "avg",        "stddev",         "variance", "stddev_pop",
	    "stddev_samp", "var_pop",   "var_samp",  "bool_and",   "bool_or",        "corr",     "covar_pop",
	    "covar_samp",  "regr_avgx", "regr_avgy", "regr_count", "regr_intercept", "regr_r2",  "regr_slope",
	    "regr_sxx",    "regr_sxy",  "regr_syy"};
	if (TYPE_CHECKED_AGGREGATES.count(name) > 0 && !children.empty()) {
		// Postgres does not provide all aggregates for all types (e.g. min(BOOLEAN), avg(TIMESTAMP))
		for (auto &child : children) {
			auto type = InferType(child.GetExpression());
			bool supported;
			if (name == "min" || name == "max") {
				switch (type.id()) {
				case LogicalTypeId::VARCHAR:
				case LogicalTypeId::DATE:
				case LogicalTypeId::TIME:
				case LogicalTypeId::TIMESTAMP:
				case LogicalTypeId::TIMESTAMP_TZ:
				case LogicalTypeId::INTERVAL:
				case LogicalTypeId::ENUM:
					supported = true;
					break;
				case LogicalTypeId::BOOLEAN:
					// DuckDB orders false before true
					name = name == "min" ? "bool_and" : "bool_or";
					supported = true;
					break;
				default:
					supported = PostgresIsNumericType(type);
					break;
				}
			} else if (name == "sum" || name == "avg") {
				supported = PostgresIsNumericType(type) || type.id() == LogicalTypeId::INTERVAL;
			} else if (name == "bool_and" || name == "bool_or") {
				supported = type.id() == LogicalTypeId::BOOLEAN;
			} else {
				supported = PostgresIsNumericType(type);
			}
			if (!supported) {
				throw PostgresPushdownNotSupported("unsupported argument type for " + name);
			}
		}
		if (name == "sum" && children.size() == 1 &&
		    InferType(children[0].GetExpression()).id() == LogicalTypeId::FLOAT) {
			// Postgres sums REAL values in single precision, DuckDB in double precision
			return "sum(" + string(distinct ? "DISTINCT " : "") + "CAST(" +
			       WriteExpression(children[0].GetExpression()) + " AS DOUBLE PRECISION)" + order_by + ")";
		}
	}
	if (name == "bit_and" || name == "bit_or" || name == "bit_xor") {
		auto type = InferType(children[0].GetExpression());
		if (type.id() != LogicalTypeId::SMALLINT && type.id() != LogicalTypeId::INTEGER &&
		    type.id() != LogicalTypeId::BIGINT) {
			throw PostgresPushdownNotSupported("unsupported bitwise aggregate argument");
		}
	}
	if ((name == "length" || name == "len" || name == "char_length" || name == "character_length") &&
	    children.size() == 1) {
		return WriteLength("length", children[0].GetExpression());
	}
	if ((name == "strlen" || name == "octet_length") && children.size() == 1) {
		auto type = InferType(children[0].GetExpression());
		if (type.id() != LogicalTypeId::VARCHAR && type.id() != LogicalTypeId::BLOB) {
			throw PostgresPushdownNotSupported("unsupported octet_length argument");
		}
		return "octet_length(" + WriteExpression(children[0].GetExpression()) + ")";
	}
	if ((name == "concat" || name == "concat_ws") && !children.empty()) {
		for (auto &child : children) {
			if (!PostgresHasIdenticalTextFormat(InferType(child.GetExpression()))) {
				// non-string arguments are formatted as text, which differs for e.g. doubles
				throw PostgresPushdownNotSupported("unsupported concat argument");
			}
		}
	}
	if ((name == "starts_with" || name == "prefix") && children.size() == 2) {
		auto version = catalog.GetPostgresVersion();
		if (version < PostgresVersion(11, 0)) {
			return "(strpos(" + WriteArgumentList(children) + ") = 1)";
		}
		name = "starts_with";
	} else if (name == "trim" && children.size() == 2) {
		// DuckDB's trim(string, characters) is btrim in Postgres
		name = "btrim";
	} else if (name == "grouping_id") {
		name = "grouping";
	} else if (name == "rank_dense") {
		name = "dense_rank";
	} else if (name == "instr" || name == "position") {
		name = "strpos";
	} else if (name == "ifnull") {
		name = "coalesce";
	} else if (name == "substring") {
		name = "substr";
	}
	string result = name + "(";
	if (distinct) {
		result += "DISTINCT ";
	}
	if ((name == "min" || name == "max" || name == "greatest" || name == "least") && !catalog.HasBinaryCollation()) {
		// the result depends on the string ordering
		for (idx_t i = 0; i < children.size(); i++) {
			result += (i > 0 ? ", " : "") + WriteOrderedExpression(children[i].GetExpression());
		}
	} else {
		result += WriteArgumentList(children);
	}
	result += order_by;
	result += ")";
	return result;
}

string PostgresSQLWriter::WriteAggregate(const FunctionExpression &func, const string &name) {
	auto &children = func.GetArguments();
	vector<string> filters;
	if (func.Filter()) {
		filters.push_back(WriteExpression(*func.Filter()));
	}
	string order_by;
	if (func.OrderBy() && !func.OrderBy()->orders.empty()) {
		if (func.Distinct() && !catalog.HasBinaryCollation()) {
			// the ORDER BY expressions of a DISTINCT aggregate must appear in its arguments - the collation of
			// string keys cannot be pinned
			for (auto &order : func.OrderBy()->orders) {
				if (PostgresGetStringOrdering(InferType(*order.expression)) != PostgresStringOrdering::NONE) {
					throw PostgresPushdownNotSupported("the string ordering of the database differs from DuckDB's");
				}
			}
		}
		order_by = " ORDER BY " + WriteOrderList(func.OrderBy()->orders);
	}
	string result;
	if (name == "count_if" || name == "countif") {
		result = "count(CASE WHEN " + WriteExpression(children[0].GetExpression()) + " THEN 1 END)";
	} else if (name == "median" || name == "quantile" || name == "quantile_cont" || name == "quantile_disc") {
		auto &arg = children[0].GetExpression();
		auto type = InferType(arg);
		// DuckDB interpolates decimals (with truncation) and temporal types, while Postgres only interpolates
		// doubles - and DuckDB's median is only discrete for types that cannot be interpolated
		bool interpolated_as_double = PostgresIsNumericType(type) && type.id() != LogicalTypeId::DECIMAL;
		bool continuous = name == "quantile_cont" || name == "median";
		if (continuous && !interpolated_as_double && !(name == "median" && type.id() == LogicalTypeId::VARCHAR)) {
			throw PostgresPushdownNotSupported("unsupported quantile argument");
		}
		if (type.id() == LogicalTypeId::INVALID) {
			throw PostgresPushdownNotSupported("unsupported quantile argument");
		}
		continuous = continuous && interpolated_as_double;
		auto quantile = name == "median" ? string("0.5") : WriteExpression(children[1].GetExpression());
		result = string(continuous ? "percentile_cont(" : "percentile_disc(") + quantile + ") WITHIN GROUP (ORDER BY " +
		         WriteOrderedOperand(WriteExpression(arg), type) + ")";
	} else if (name == "arg_min" || name == "arg_max" || name == "min_by" || name == "max_by") {
		// the argument of the row with the minimum / maximum value - rows where either is NULL are ignored
		auto &arg = children[0].GetExpression();
		auto &val = children[1].GetExpression();
		if (!PostgresSupportsArrayElement(InferType(arg))) {
			throw PostgresPushdownNotSupported("unsupported arg_min / arg_max argument");
		}
		auto arg_str = WriteExpression(arg);
		auto val_str = WriteExpression(val);
		bool is_max = name == "arg_max" || name == "max_by";
		filters.push_back("(" + arg_str + " IS NOT NULL)");
		filters.push_back("(" + val_str + " IS NOT NULL)");
		result = "array_agg(" + arg_str + " ORDER BY " + WriteOrderedOperand(val_str, InferType(val)) +
		         (is_max ? " DESC" : " ASC") + ")";
		result += " FILTER (WHERE " + StringUtil::Join(filters, " AND ") + ")";
		return "(" + result + ")[1]";
	} else {
		result = WriteFunctionCall(name, children, func.Distinct(), order_by);
	}
	if (!filters.empty()) {
		result += " FILTER (WHERE " + StringUtil::Join(filters, " AND ") + ")";
	}
	return result;
}

string PostgresSQLWriter::WriteFunction(const FunctionExpression &func) {
	auto &children = func.GetArguments();
	auto name = StringUtil::Lower(func.FunctionName().GetIdentifierName());
	if (func.Filter() || (func.OrderBy() && !func.OrderBy()->orders.empty()) || PostgresIsAggregateName(name)) {
		return WriteAggregate(func, name);
	}
	string special_result;
	if (TryWriteSpecialFunction(name, children, special_result)) {
		return special_result;
	}
	if (name == "~~" || name == "!~~") {
		// [NOT] LIKE - DuckDB's LIKE has no default escape character, while Postgres' default
		// escape character is backslash; disable it with an empty ESCAPE clause
		D_ASSERT(children.size() == 2);
		return "(" + WriteStringOperand(children[0].GetExpression()) + (name == "~~" ? " LIKE " : " NOT LIKE ") +
		       WriteStringOperand(children[1].GetExpression()) + " ESCAPE '')";
	}
	static const case_insensitive_set_t OPERATOR_NAMES = {"+", "-", "*", "/", "%", "||"};
	if (func.IsOperator() || OPERATOR_NAMES.count(name) > 0) {
		if (children.size() == 1) {
			return "(" + name + WriteExpression(children[0].GetExpression()) + ")";
		}
		if (children.size() == 2) {
			auto &left_expr = children[0].GetExpression();
			auto &right_expr = children[1].GetExpression();
			if (name == "/" || name == "%" || name == "||") {
				auto left_type = InferType(left_expr);
				auto right_type = InferType(right_expr);
				if (name == "/") {
					if (!PostgresIsNumericType(left_type) || !PostgresIsNumericType(right_type)) {
						throw PostgresPushdownNotSupported("unsupported division");
					}
					return WriteDivision(left_expr, right_expr);
				}
				if (name == "%") {
					if (!HasDefaultDivisionSemantics()) {
						throw PostgresPushdownNotSupported("non-default division semantics");
					}
					// Postgres has no modulo for floating point numbers
					auto is_exact = [](const LogicalType &type) {
						return PostgresIsIntegerType(type) || type.id() == LogicalTypeId::DECIMAL;
					};
					if (!is_exact(left_type) || !is_exact(right_type)) {
						throw PostgresPushdownNotSupported("unsupported modulo");
					}
				}
				if (name == "||") {
					// non-string operands are formatted as text, which differs for e.g. doubles - and DuckDB's
					// || on lists concatenates the lists
					if (!PostgresHasIdenticalTextFormat(left_type) || !PostgresHasIdenticalTextFormat(right_type) ||
					    (left_type.id() != LogicalTypeId::VARCHAR && right_type.id() != LogicalTypeId::VARCHAR)) {
						throw PostgresPushdownNotSupported("unsupported concatenation");
					}
				}
			}
			return "(" + WriteExpression(left_expr) + " " + name + " " + WriteExpression(right_expr) + ")";
		}
		throw InternalException("PostgresSQLWriter: unsupported operator %s - should have been blocked by "
		                        "SupportsPushdown",
		                        name);
	}
	return WriteFunctionCall(func.FunctionName().GetIdentifierName(), children, func.Distinct(), string());
}

static string PostgresWriteWindowBoundary(WindowBoundary boundary, const string &boundary_expr) {
	switch (boundary) {
	case WindowBoundary::UNBOUNDED_PRECEDING:
		return "UNBOUNDED PRECEDING";
	case WindowBoundary::UNBOUNDED_FOLLOWING:
		return "UNBOUNDED FOLLOWING";
	case WindowBoundary::CURRENT_ROW_ROWS:
	case WindowBoundary::CURRENT_ROW_RANGE:
	case WindowBoundary::CURRENT_ROW_GROUPS:
		return "CURRENT ROW";
	case WindowBoundary::EXPR_PRECEDING_ROWS:
	case WindowBoundary::EXPR_PRECEDING_RANGE:
	case WindowBoundary::EXPR_PRECEDING_GROUPS:
		return boundary_expr + " PRECEDING";
	case WindowBoundary::EXPR_FOLLOWING_ROWS:
	case WindowBoundary::EXPR_FOLLOWING_RANGE:
	case WindowBoundary::EXPR_FOLLOWING_GROUPS:
		return boundary_expr + " FOLLOWING";
	default:
		throw InternalException("PostgresSQLWriter: unsupported window boundary - should have been blocked by "
		                        "SupportsPushdown");
	}
}

static bool PostgresIsRowsBoundary(WindowBoundary boundary) {
	return boundary == WindowBoundary::CURRENT_ROW_ROWS || boundary == WindowBoundary::EXPR_PRECEDING_ROWS ||
	       boundary == WindowBoundary::EXPR_FOLLOWING_ROWS;
}

static bool PostgresIsGroupsBoundary(WindowBoundary boundary) {
	return boundary == WindowBoundary::CURRENT_ROW_GROUPS || boundary == WindowBoundary::EXPR_PRECEDING_GROUPS ||
	       boundary == WindowBoundary::EXPR_FOLLOWING_GROUPS;
}

//! Postgres requires RANGE offsets to have a type that matches the ORDER BY key (e.g. an integer offset for an integer
//! key, an interval for a date) - other offsets are cast to the type of the key like DuckDB does
string PostgresSQLWriter::WriteFrameOffset(const WindowExpression &window, const ParsedExpression &offset,
                                           const string &mode) {
	if (mode != "RANGE") {
		return WriteExpression(offset);
	}
	if (window.OrderBy().size() != 1) {
		throw PostgresPushdownNotSupported("RANGE frame with an offset requires a single ORDER BY key");
	}
	auto key_type = InferType(*window.OrderBy()[0].expression);
	Value value;
	interval_t interval;
	if (PostgresCapabilities::TryGetIntervalLiteral(offset, interval)) {
		value = Value::INTERVAL(interval);
	} else if (!PostgresCapabilities::TryGetConstantValue(offset, value)) {
		throw PostgresPushdownNotSupported("non-constant RANGE frame offset");
	}
	if (value.type().id() == LogicalTypeId::INTERVAL) {
		if (!PostgresIsDateTimeType(key_type) && key_type.id() != LogicalTypeId::INTERVAL) {
			throw PostgresPushdownNotSupported("unsupported RANGE frame offset");
		}
		return WriteConstant(value);
	}
	if (!PostgresIsNumericType(key_type)) {
		throw PostgresPushdownNotSupported("unsupported RANGE frame offset");
	}
	auto cast_value = value.TryCastAs(context, key_type);
	if (!cast_value || !PostgresCapabilities::SupportsValue(*cast_value)) {
		throw PostgresPushdownNotSupported("unsupported RANGE frame offset");
	}
	return WriteConstant(*cast_value);
}

string PostgresSQLWriter::WriteWindow(const WindowExpression &window) {
	string result = WriteFunctionCall(window.FunctionName().GetIdentifierName(), window.GetArguments(),
	                                  window.Distinct(), string());
	if (window.Filter()) {
		result += " FILTER (WHERE " + WriteExpression(*window.Filter()) + ")";
	}
	result += " OVER (";
	string sep;
	if (!window.Partitions().empty()) {
		result += "PARTITION BY " + WriteExpressionList(window.Partitions());
		sep = " ";
	}
	if (!window.OrderBy().empty()) {
		result += sep + "ORDER BY " + WriteOrderList(window.OrderBy());
		sep = " ";
	}
	auto window_start = window.WindowStart();
	auto window_end = window.WindowEnd();
	if (!(window_start == WindowBoundary::UNBOUNDED_PRECEDING && window_end == WindowBoundary::CURRENT_ROW_RANGE &&
	      window.WindowExclude() == WindowExcludeMode::NO_OTHER)) {
		// non-default frame
		string mode = "RANGE";
		if (PostgresIsRowsBoundary(window_start) || PostgresIsRowsBoundary(window_end)) {
			mode = "ROWS";
		} else if (PostgresIsGroupsBoundary(window_start) || PostgresIsGroupsBoundary(window_end)) {
			mode = "GROUPS";
		}
		string start_expr = window.StartExpr() ? WriteFrameOffset(window, *window.StartExpr(), mode) : string();
		string end_expr = window.EndExpr() ? WriteFrameOffset(window, *window.EndExpr(), mode) : string();
		result += sep + mode;
		result += " BETWEEN " + PostgresWriteWindowBoundary(window_start, start_expr);
		result += " AND " + PostgresWriteWindowBoundary(window_end, end_expr);
		switch (window.WindowExclude()) {
		case WindowExcludeMode::NO_OTHER:
			break;
		case WindowExcludeMode::CURRENT_ROW:
			result += " EXCLUDE CURRENT ROW";
			break;
		case WindowExcludeMode::GROUP:
			result += " EXCLUDE GROUP";
			break;
		case WindowExcludeMode::TIES:
			result += " EXCLUDE TIES";
			break;
		}
	}
	result += ")";
	return result;
}

string PostgresSQLWriter::WriteOperator(const OperatorExpression &op) {
	auto &children = op.GetChildren();
	switch (op.GetExpressionType()) {
	case ExpressionType::OPERATOR_IS_NULL:
		return "(" + WriteExpression(*children[0]) + " IS NULL)";
	case ExpressionType::OPERATOR_IS_NOT_NULL:
		return "(" + WriteExpression(*children[0]) + " IS NOT NULL)";
	case ExpressionType::OPERATOR_NOT:
		return "(NOT " + WriteExpressionList(children) + ")";
	case ExpressionType::OPERATOR_COALESCE:
		return "COALESCE(" + WriteExpressionList(children) + ")";
	case ExpressionType::OPERATOR_NULLIF:
		return "NULLIF(" + WriteExpressionList(children) + ")";
	case ExpressionType::GROUPING_FUNCTION:
		return "GROUPING(" + WriteExpressionList(children) + ")";
	case ExpressionType::COMPARE_IN:
	case ExpressionType::COMPARE_NOT_IN: {
		vector<reference<const ParsedExpression>> operands;
		for (auto &child : children) {
			operands.push_back(*child);
		}
		auto written = WriteComparisonOperands(operands);
		string result = "(" + written[0];
		result += op.GetExpressionType() == ExpressionType::COMPARE_IN ? " IN (" : " NOT IN (";
		for (idx_t i = 1; i < written.size(); i++) {
			if (i > 1) {
				result += ", ";
			}
			result += written[i];
		}
		result += "))";
		return result;
	}
	default:
		throw InternalException("PostgresSQLWriter: unsupported operator type - should have been blocked by "
		                        "SupportsPushdown");
	}
}

string PostgresSQLWriter::WriteCase(const CaseExpression &case_expr) {
	string result = "CASE";
	for (auto &check : case_expr.CaseChecks()) {
		result += " WHEN " + WriteExpression(*check.when_expr);
		result += " THEN " + WriteExpression(*check.then_expr);
	}
	result += " ELSE " + WriteExpression(case_expr.Else());
	result += " END";
	return result;
}

static string PostgresComparisonOperator(ExpressionType type) {
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
	case ExpressionType::COMPARE_DISTINCT_FROM:
		return "IS DISTINCT FROM";
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		return "IS NOT DISTINCT FROM";
	default:
		throw InternalException("PostgresSQLWriter: unsupported comparison type - should have been blocked by "
		                        "SupportsPushdown");
	}
}

//! Write the operands of a comparison (or of IN / BETWEEN) - constants that are compared with an expression of a
//! different type are cast to the type DuckDB compares them as (with DuckDB's cast semantics)
vector<string> PostgresSQLWriter::WriteComparisonOperands(const vector<reference<const ParsedExpression>> &operands,
                                                          bool ordering) {
	// ordered strings require the type the operands are compared as
	bool pin_collation = ordering && !catalog.HasBinaryCollation();
	vector<Value> values(operands.size());
	vector<bool> is_constant(operands.size());
	bool any_constant = false;
	bool any_expression = false;
	for (idx_t i = 0; i < operands.size(); i++) {
		interval_t interval;
		if (PostgresCapabilities::TryGetIntervalLiteral(operands[i].get(), interval)) {
			values[i] = Value::INTERVAL(interval);
			is_constant[i] = true;
		} else {
			is_constant[i] = PostgresCapabilities::TryGetConstantValue(operands[i].get(), values[i]);
		}
		any_constant = any_constant || is_constant[i];
		any_expression = any_expression || !is_constant[i];
	}
	vector<string> result;
	if ((!any_constant || !any_expression) && !pin_collation) {
		for (auto &operand : operands) {
			result.push_back(WriteExpression(operand.get()));
		}
		return result;
	}
	vector<LogicalType> types;
	for (idx_t i = 0; i < operands.size(); i++) {
		types.push_back(is_constant[i] ? values[i].type() : InferType(operands[i].get()));
		if (types.back().id() == LogicalTypeId::INVALID) {
			throw PostgresPushdownNotSupported("cannot determine the type of a comparison operand");
		}
	}
	// the type DuckDB compares the operands as
	LogicalType target = types[0];
	for (idx_t i = 1; i < types.size(); i++) {
		if (!BoundComparisonExpression::TryBindComparison(context, target, types[i], target,
		                                                  ExpressionType::COMPARE_EQUAL)) {
			throw PostgresPushdownNotSupported("operands cannot be compared");
		}
	}
	for (idx_t i = 0; i < operands.size(); i++) {
		if (is_constant[i]) {
			if (values[i].type() == target || values[i].IsNull()) {
				result.push_back(WriteExpression(operands[i].get()));
				continue;
			}
			auto cast_value = values[i].TryCastAs(context, target);
			if (!cast_value || !PostgresCapabilities::SupportsValue(*cast_value)) {
				// DuckDB raises an error
				throw PostgresPushdownNotSupported("constant cannot be converted to the compared type");
			}
			result.push_back(WriteConstant(*cast_value));
		} else {
			auto expr_str = WriteExpression(operands[i].get());
			result.push_back(types[i] == target ? expr_str : WriteCast(expr_str, types[i], target));
		}
	}
	if (pin_collation) {
		// the collation of one operand applies to the comparison
		result[0] = WriteOrderedOperand(result[0], target);
	}
	return result;
}

//! Write an operand of a string operation (e.g. LIKE) - DuckDB converts other types to VARCHAR
string PostgresSQLWriter::WriteStringOperand(const ParsedExpression &expr) {
	auto type = InferType(expr);
	auto expr_str = WriteExpression(expr);
	if (type.id() == LogicalTypeId::VARCHAR || type.id() == LogicalTypeId::SQLNULL) {
		return expr_str;
	}
	if (type.id() == LogicalTypeId::INVALID) {
		throw PostgresPushdownNotSupported("cannot determine the type of a string operand");
	}
	return WriteCast(expr_str, type, LogicalType::VARCHAR);
}

string PostgresSQLWriter::WriteOrderedOperand(const string &expr_str, const LogicalType &type) {
	if (catalog.HasBinaryCollation()) {
		// the database orders strings like DuckDB
		return expr_str;
	}
	switch (PostgresGetStringOrdering(type)) {
	case PostgresStringOrdering::NONE:
		return expr_str;
	case PostgresStringOrdering::COLLATABLE: {
		// the cast makes the expression collatable - values that DuckDB reads as VARCHAR are not necessarily
		// strings in Postgres (e.g. JSON or enum columns)
		bool is_list = type.id() == LogicalTypeId::LIST || type.id() == LogicalTypeId::ARRAY;
		return "CAST(" + expr_str + (is_list ? " AS TEXT[])" : " AS TEXT)") + " COLLATE \"C\"";
	}
	default:
		throw PostgresPushdownNotSupported("the string ordering of the database differs from DuckDB's");
	}
}

string PostgresSQLWriter::WriteOrderedExpression(const ParsedExpression &expr) {
	auto expr_str = WriteExpression(expr);
	if (catalog.HasBinaryCollation()) {
		return expr_str;
	}
	return WriteOrderedOperand(expr_str, InferType(expr));
}

string PostgresSQLWriter::WriteSubquery(const SubqueryExpression &subquery) {
	auto &node = *subquery.Subquery()->node;
	switch (subquery.GetSubqueryType()) {
	case SubqueryType::EXISTS:
		return "EXISTS(" + WriteQueryNode(node) + ")";
	case SubqueryType::NOT_EXISTS:
		return "NOT EXISTS(" + WriteQueryNode(node) + ")";
	case SubqueryType::SCALAR:
		return "(" + WriteQueryNode(node) + ")";
	case SubqueryType::ANY: {
		auto comparison_type = subquery.GetComparisonType();
		bool ordering =
		    comparison_type != ExpressionType::COMPARE_EQUAL && comparison_type != ExpressionType::COMPARE_NOTEQUAL;
		auto child = ordering ? WriteOrderedExpression(*subquery.GetChild()) : WriteExpression(*subquery.GetChild());
		return "(" + child + " " + PostgresComparisonOperator(comparison_type) + " ANY(" + WriteQueryNode(node) + "))";
	}
	default:
		throw InternalException("PostgresSQLWriter: unsupported subquery type - should have been blocked by "
		                        "SupportsPushdown");
	}
}

string PostgresSQLWriter::WriteExpression(const ParsedExpression &expr) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::CONSTANT:
		return WriteConstant(expr.Cast<ConstantExpression>().GetLiteral().ToValue());
	case ExpressionClass::COLUMN_REF:
		return WriteColumnRef(expr.Cast<ColumnRefExpression>());
	case ExpressionClass::STAR: {
		auto &star = expr.Cast<StarExpression>();
		if (!star.RelationName().empty()) {
			// qualified star (e.g. t.*)
			return WriteIdentifier(star.RelationName()) + ".*";
		}
		return "*";
	}
	case ExpressionClass::FUNCTION: {
		interval_t interval;
		if (PostgresCapabilities::TryGetIntervalLiteral(expr, interval)) {
			return WriteConstant(Value::INTERVAL(interval));
		}
		return WriteFunction(expr.Cast<FunctionExpression>());
	}
	case ExpressionClass::WINDOW:
		return WriteWindow(expr.Cast<WindowExpression>());
	case ExpressionClass::COMPARISON: {
		auto &comparison = expr.Cast<ComparisonExpression>();
		vector<reference<const ParsedExpression>> operands {comparison.Left(), comparison.Right()};
		bool ordering = expr.GetExpressionType() != ExpressionType::COMPARE_EQUAL &&
		                expr.GetExpressionType() != ExpressionType::COMPARE_NOTEQUAL &&
		                expr.GetExpressionType() != ExpressionType::COMPARE_DISTINCT_FROM &&
		                expr.GetExpressionType() != ExpressionType::COMPARE_NOT_DISTINCT_FROM;
		auto written = WriteComparisonOperands(operands, ordering);
		return "(" + written[0] + " " + PostgresComparisonOperator(expr.GetExpressionType()) + " " + written[1] + ")";
	}
	case ExpressionClass::CONJUNCTION: {
		auto &conjunction = expr.Cast<ConjunctionExpression>();
		auto op = expr.GetExpressionType() == ExpressionType::CONJUNCTION_AND ? " AND " : " OR ";
		return "(" + WriteExpressionList(conjunction.GetChildren(), op) + ")";
	}
	case ExpressionClass::OPERATOR:
		return WriteOperator(expr.Cast<OperatorExpression>());
	case ExpressionClass::CASE:
		return WriteCase(expr.Cast<CaseExpression>());
	case ExpressionClass::CAST: {
		auto &cast_expr = expr.Cast<CastExpression>();
		auto target_type = PostgresCapabilities::ResolveType(cast_expr.TargetType());
		if (cast_expr.Child().GetExpressionClass() == ExpressionClass::CONSTANT) {
			// casts of literals are evaluated locally and serialized as the resulting value
			auto value = cast_expr.Child().Cast<ConstantExpression>().GetLiteral().ToValue();
			auto cast_result = value.DefaultTryCastAs(target_type);
			if (!cast_result) {
				throw InternalException("PostgresSQLWriter: cast of literal failed - should have been blocked by "
				                        "SupportsPushdown");
			}
			return WriteConstant(*cast_result);
		}
		return WriteCast(cast_expr.Child(), target_type);
	}
	case ExpressionClass::BETWEEN: {
		auto &between = expr.Cast<BetweenExpression>();
		vector<reference<const ParsedExpression>> operands {between.Input(), between.LowerBound(),
		                                                    between.UpperBound()};
		auto written = WriteComparisonOperands(operands, true);
		return "(" + written[0] + " BETWEEN " + written[1] + " AND " + written[2] + ")";
	}
	case ExpressionClass::SUBQUERY:
		return WriteSubquery(expr.Cast<SubqueryExpression>());
	default:
		throw InternalException("PostgresSQLWriter: unsupported expression class - should have been blocked by "
		                        "SupportsPushdown");
	}
}

//===--------------------------------------------------------------------===//
// Table references
//===--------------------------------------------------------------------===//
string PostgresSQLWriter::WriteTableAlias(const TableRef &ref) {
	string result;
	if (!ref.alias.empty()) {
		result += " AS " + WriteIdentifier(ref.alias);
	}
	if (!ref.column_name_alias.empty()) {
		result += "(";
		for (idx_t i = 0; i < ref.column_name_alias.size(); i++) {
			if (i > 0) {
				result += ", ";
			}
			result += WriteIdentifier(ref.column_name_alias[i]);
		}
		result += ")";
	}
	return result;
}

string PostgresSQLWriter::WriteTableRef(const TableRef &ref, bool lateral) {
	switch (ref.type) {
	case TableReferenceType::BASE_TABLE: {
		auto &base = ref.Cast<BaseTableRef>();
		string result;
		if (IsCTEReference(base)) {
			result = WriteIdentifier(base.Table());
		} else {
			auto table = LookupTable(base);
			if (table) {
				result = PostgresUtils::WriteIdentifier(table->ParentSchema().name.GetIdentifierName()) + "." +
				         PostgresUtils::WriteIdentifier(table->name.GetIdentifierName());
			} else {
				if (!base.GetQualifiedName().Schema().empty()) {
					result += WriteIdentifier(base.GetQualifiedName().Schema()) + ".";
				}
				result += WriteIdentifier(base.Table());
			}
		}
		auto alias = WriteTableAlias(ref);
		if (alias.empty() && !ref.column_name_alias.empty()) {
			// a column alias list requires a table alias
			alias = " AS " + WriteIdentifier(base.Table()) + alias;
		}
		return result + alias;
	}
	case TableReferenceType::JOIN: {
		auto &join = ref.Cast<JoinRef>();
		if (join.type == JoinType::SEMI || join.type == JoinType::ANTI) {
			// a SEMI / ANTI join filters the left side - which keeps the name it is bound to
			auto &left = *join.left;
			Identifier binding_name = left.alias.empty() && left.type == TableReferenceType::BASE_TABLE
			                              ? left.Cast<BaseTableRef>().Table()
			                              : left.alias;
			string result = "(SELECT * FROM " + WriteTableRef(left) + " WHERE ";
			result += join.type == JoinType::SEMI ? "EXISTS" : "NOT EXISTS";
			result += " (SELECT 1 FROM " + WriteTableRef(*join.right, true) + " WHERE " +
			          WriteExpression(*join.condition) + ")) AS " + WriteIdentifier(binding_name);
			if (!left.column_name_alias.empty()) {
				throw PostgresPushdownNotSupported("SEMI / ANTI join with column aliases");
			}
			return result + WriteTableAlias(ref);
		}
		string result = "(" + WriteTableRef(*join.left) + " ";
		bool lateral_right = false;
		if (join.ref_type == JoinRefType::CROSS) {
			result += "CROSS JOIN ";
			lateral_right = true;
		} else if (join.ref_type == JoinRefType::REGULAR || join.ref_type == JoinRefType::NATURAL) {
			if (join.ref_type == JoinRefType::NATURAL) {
				result += "NATURAL ";
			}
			switch (join.type) {
			case JoinType::INNER:
				result += "INNER JOIN ";
				lateral_right = true;
				break;
			case JoinType::LEFT:
				result += "LEFT JOIN ";
				lateral_right = true;
				break;
			case JoinType::RIGHT:
				result += "RIGHT JOIN ";
				break;
			case JoinType::OUTER:
				result += "FULL OUTER JOIN ";
				break;
			default:
				throw InternalException("PostgresSQLWriter: unsupported join type - should have been blocked by "
				                        "SupportsPushdown");
			}
		} else {
			throw InternalException("PostgresSQLWriter: unsupported join ref type - should have been blocked by "
			                        "SupportsPushdown");
		}
		// DuckDB detects lateral references automatically - Postgres requires the LATERAL keyword
		result += WriteTableRef(*join.right, lateral_right);
		if (join.condition) {
			result += " ON " + WriteExpression(*join.condition);
		} else if (!join.using_columns.empty()) {
			result += " USING (";
			for (idx_t i = 0; i < join.using_columns.size(); i++) {
				if (i > 0) {
					result += ", ";
				}
				result += WriteIdentifier(join.using_columns[i]);
			}
			result += ")";
		} else if (join.ref_type == JoinRefType::REGULAR) {
			// an INNER JOIN without a condition
			result += " ON TRUE";
		}
		result += ")";
		return result + WriteTableAlias(ref);
	}
	case TableReferenceType::SUBQUERY: {
		auto &subquery = ref.Cast<SubqueryRef>();
		string result = lateral ? "LATERAL (" : "(";
		result += WriteQueryNode(*subquery.subquery->node) + ")";
		if (subquery.alias.empty()) {
			// older Postgres versions require an alias for every subquery
			string alias = "unnamed_subquery";
			if (unnamed_subquery_index > 0) {
				alias += to_string(unnamed_subquery_index);
			}
			unnamed_subquery_index++;
			result += " AS " + WriteIdentifier(alias);
		}
		return result + WriteTableAlias(ref);
	}
	case TableReferenceType::EXPRESSION_LIST: {
		auto &expression_list = ref.Cast<ExpressionListRef>();
		string result = "(VALUES ";
		for (idx_t row = 0; row < expression_list.values.size(); row++) {
			if (row > 0) {
				result += ", ";
			}
			result += "(" + WriteExpressionList(expression_list.values[row]) + ")";
		}
		result += ")";
		// name the relation and its columns the way DuckDB does (Postgres names the columns column1, ...)
		result += " AS " + WriteIdentifier(ref.alias.empty() ? Identifier("valueslist") : ref.alias) + "(";
		auto column_count = expression_list.values[0].size();
		for (idx_t col = 0; col < column_count; col++) {
			if (col > 0) {
				result += ", ";
			}
			if (col < ref.column_name_alias.size()) {
				result += WriteIdentifier(ref.column_name_alias[col]);
			} else if (col < expression_list.expected_names.size()) {
				result += WriteIdentifier(expression_list.expected_names[col]);
			} else {
				result += WriteIdentifier("col" + to_string(col));
			}
		}
		return result + ")";
	}
	case TableReferenceType::TABLE_FUNCTION: {
		auto &table_function = ref.Cast<TableFunctionRef>();
		auto &func = table_function.function->Cast<FunctionExpression>();
		auto name = StringUtil::Lower(func.FunctionName().GetIdentifierName());
		auto &children = func.GetArguments();
		vector<LogicalType> types;
		for (auto &child : children) {
			types.push_back(InferType(child.GetExpression()));
		}
		bool integers = true;
		for (auto &type : types) {
			integers = integers && PostgresIsIntegerType(type);
		}
		auto bigint = [&](idx_t i) {
			return "CAST(" + WriteExpression(children[i].GetExpression()) + " AS BIGINT)";
		};
		string result = lateral ? "LATERAL " : "";
		if (name == "range" && integers) {
			// DuckDB's range excludes the upper bound
			string start = children.size() == 1 ? "CAST(0 AS BIGINT)" : bigint(0);
			string end = children.size() == 1 ? bigint(0) : bigint(1);
			int64_t step = 1;
			if (children.size() == 3) {
				Value step_value;
				PostgresCapabilities::TryGetConstantValue(children[2].GetExpression(), step_value);
				step = BigIntValue::Get(step_value.DefaultCastAs(LogicalType::BIGINT));
			}
			result += "generate_series(" + start + ", " + end + (step > 0 ? " - 1" : " + 1");
			result += children.size() == 3 ? ", " + bigint(2) + ")" : ")";
		} else if (name == "generate_series" && integers) {
			result += "generate_series(" + bigint(0) + ", " + bigint(1) +
			          (children.size() == 3 ? ", " + bigint(2) : "") + ")";
		} else if (name == "generate_series" && children.size() == 3 && types[0].id() == LogicalTypeId::TIMESTAMP &&
		           types[1].id() == LogicalTypeId::TIMESTAMP && types[2].id() == LogicalTypeId::INTERVAL) {
			result += "generate_series(" + WriteArgumentList(children) + ")";
		} else {
			throw PostgresPushdownNotSupported("unsupported table function arguments");
		}
		if (table_function.with_ordinality == OrdinalityType::WITH_ORDINALITY) {
			result += " WITH ORDINALITY";
		}
		// a single-column table function is named after the function by default
		result += " AS " + WriteIdentifier(ref.alias.empty() ? Identifier(name) : ref.alias);
		if (!ref.column_name_alias.empty()) {
			result += "(";
			for (idx_t i = 0; i < ref.column_name_alias.size(); i++) {
				if (i > 0) {
					result += ", ";
				}
				result += WriteIdentifier(ref.column_name_alias[i]);
			}
			result += ")";
		}
		return result;
	}
	default:
		throw InternalException("PostgresSQLWriter: unsupported table ref type - should have been blocked by "
		                        "SupportsPushdown");
	}
}

//===--------------------------------------------------------------------===//
// Result modifiers
//===--------------------------------------------------------------------===//
//! Integer literals in ORDER BY / GROUP BY / DISTINCT ON are positional references - other constants (e.g. a cast of
//! an integer literal) are written as typed literals, so that Postgres does not interpret them as positions either
string PostgresSQLWriter::WriteOrderingKey(const ParsedExpression &expr) {
	Value value;
	if (expr.GetExpressionClass() != ExpressionClass::CONSTANT &&
	    PostgresCapabilities::TryGetConstantValue(expr, value)) {
		return "CAST(" + WriteConstant(value) + " AS " + WriteType(value.type()) + ")";
	}
	return WriteExpression(expr);
}

//! The select-list entry an ORDER BY key refers to (a position or an alias), nullptr if it does not refer to one or
//! if the entry cannot be determined ("is_reference" is set then)
static optional_ptr<const ParsedExpression>
PostgresOrderedSelectEntry(const ParsedExpression &key, const vector<unique_ptr<ParsedExpression>> &select_list,
                           bool &is_reference) {
	is_reference = false;
	if (key.GetExpressionClass() == ExpressionClass::CONSTANT) {
		auto value = key.Cast<ConstantExpression>().GetLiteral().ToValue();
		if (!value.type().IsIntegral()) {
			return nullptr;
		}
		is_reference = true;
		auto position = value.GetValue<int64_t>();
		for (auto &entry : select_list) {
			if (entry->GetExpressionClass() == ExpressionClass::STAR) {
				// the position refers to an expanded column
				return nullptr;
			}
		}
		if (position < 1 || position > static_cast<int64_t>(select_list.size())) {
			return nullptr;
		}
		return select_list[static_cast<idx_t>(position - 1)].get();
	}
	if (key.GetExpressionClass() == ExpressionClass::COLUMN_REF && !key.Cast<ColumnRefExpression>().IsQualified()) {
		auto &name = key.Cast<ColumnRefExpression>().GetColumnName();
		for (auto &entry : select_list) {
			if (!entry->GetAlias().empty() && entry->GetAlias() == name) {
				is_reference = true;
				return entry.get();
			}
		}
		for (auto &entry : select_list) {
			// a result column named after the column it selects
			if (entry->GetAlias().empty() && entry->GetExpressionClass() == ExpressionClass::COLUMN_REF &&
			    entry->Cast<ColumnRefExpression>().GetColumnName() == name) {
				is_reference = true;
				return entry.get();
			}
		}
	}
	return nullptr;
}

//! The leftmost SELECT of a set operation, which determines the names and types of its result columns
static optional_ptr<const SelectNode> PostgresLeftmostSelect(const QueryNode &node) {
	switch (node.type) {
	case QueryNodeType::SELECT_NODE:
		return &node.Cast<SelectNode>();
	case QueryNodeType::SET_OPERATION_NODE: {
		auto &setop = node.Cast<SetOperationNode>();
		return setop.children.empty() ? nullptr : PostgresLeftmostSelect(*setop.children[0]);
	}
	case QueryNodeType::RECURSIVE_CTE_NODE:
		return PostgresLeftmostSelect(*node.Cast<RecursiveCTENode>().left);
	default:
		return nullptr;
	}
}

string PostgresSQLWriter::WriteOrderList(const vector<OrderByNode> &orders, optional_ptr<const QueryNode> node) {
	auto &config = DBConfig::GetConfig(context);
	optional_ptr<const vector<unique_ptr<ParsedExpression>>> select_list;
	bool is_select = node && node->type == QueryNodeType::SELECT_NODE;
	bool has_distinct = false;
	// the result types of the leftmost SELECT of a set operation - its scope is not active anymore
	vector<LogicalType> leftmost_types;
	optional_ptr<const SelectNode> leftmost;
	if (node) {
		leftmost = PostgresLeftmostSelect(*node);
		if (leftmost) {
			select_list = &leftmost->select_list;
		}
		for (auto &modifier : node->modifiers) {
			has_distinct = has_distinct || modifier->type == ResultModifierType::DISTINCT_MODIFIER;
		}
	}
	// the type of a select-list entry - binding the whole SELECT also determines the types of aggregates and
	// GROUPING() entries
	bool types_inferred = false;
	auto entry_type = [&](const ParsedExpression &entry) {
		if (!types_inferred) {
			types_inferred = true;
			bool has_star = false;
			for (auto &select_entry : *select_list) {
				has_star = has_star || select_entry->GetExpressionClass() == ExpressionClass::STAR;
			}
			if (!has_star) {
				leftmost_types = InferQueryTypes(*leftmost);
			}
		}
		for (idx_t i = 0; i < select_list->size() && i < leftmost_types.size(); i++) {
			if ((*select_list)[i].get() == &entry) {
				return leftmost_types[i];
			}
		}
		return is_select ? InferType(entry) : LogicalType(LogicalType::INVALID);
	};
	string result;
	for (auto &order : orders) {
		auto order_type = config.ResolveOrder(context, order.type);
		auto null_order = config.ResolveNullOrder(context, order_type, order.null_order);
		vector<string> keys;
		if (order.expression->GetExpressionClass() == ExpressionClass::STAR) {
			// ORDER BY ALL - order by all select-list positions
			if (!select_list || !is_select) {
				throw PostgresPushdownNotSupported("ORDER BY ALL without a select list");
			}
			for (idx_t i = 0; i < select_list->size(); i++) {
				auto &entry = *(*select_list)[i];
				if (catalog.HasBinaryCollation()) {
					keys.push_back(to_string(i + 1));
					continue;
				}
				auto type = entry.GetExpressionClass() == ExpressionClass::STAR ? LogicalType(LogicalType::INVALID)
				                                                                : entry_type(entry);
				if (PostgresGetStringOrdering(type) == PostgresStringOrdering::NONE) {
					keys.push_back(to_string(i + 1));
				} else if (has_distinct || entry.GetExpressionClass() == ExpressionClass::STAR) {
					throw PostgresPushdownNotSupported("the string ordering of the database differs from DuckDB's");
				} else {
					// a position cannot be combined with a collation - order by the expression
					keys.push_back(WriteOrderedOperand(WriteExpression(entry), type));
				}
			}
		} else if (catalog.HasBinaryCollation()) {
			keys.push_back(WriteOrderingKey(*order.expression));
		} else if (!node) {
			// window / aggregate ORDER BY
			keys.push_back(WriteOrderedOperand(WriteOrderingKey(*order.expression), InferType(*order.expression)));
		} else {
			bool is_reference = false;
			optional_ptr<const ParsedExpression> entry;
			if (select_list) {
				entry = PostgresOrderedSelectEntry(*order.expression, *select_list, is_reference);
			}
			if (!is_select && !is_reference) {
				throw PostgresPushdownNotSupported("unsupported ORDER BY of a set operation");
			}
			optional_ptr<const ParsedExpression> ordered = is_reference ? entry : order.expression.get();
			LogicalType ordered_type = LogicalType::INVALID;
			if (ordered) {
				ordered_type = is_reference ? entry_type(*ordered) : InferType(*ordered);
			}
			auto ordering = PostgresGetStringOrdering(ordered_type);
			if (ordering == PostgresStringOrdering::NONE) {
				keys.push_back(WriteOrderingKey(*order.expression));
			} else if (!is_select || has_distinct) {
				// a set operation can only be ordered by its result columns, with DISTINCT the ORDER BY expressions
				// must appear in the select list - neither allows pinning the collation
				throw PostgresPushdownNotSupported("the string ordering of the database differs from DuckDB's");
			} else {
				// a position / alias cannot be combined with a collation - order by the referenced expression
				auto &ordered_expr = *ordered;
				keys.push_back(WriteOrderedOperand(
				    is_reference ? WriteExpression(ordered_expr) : WriteOrderingKey(ordered_expr), ordered_type));
			}
		}
		for (auto &key : keys) {
			if (!result.empty()) {
				result += ", ";
			}
			result += key;
			result += order_type == OrderType::DESCENDING ? " DESC" : " ASC";
			// Postgres sorts NULLs as larger than any value, DuckDB sorts them last by default
			result += null_order == OrderByNullType::NULLS_FIRST ? " NULLS FIRST" : " NULLS LAST";
		}
	}
	return result;
}

string PostgresSQLWriter::WriteResultModifiers(const QueryNode &node,
                                               optional_ptr<const vector<unique_ptr<ParsedExpression>>> select_list) {
	string result;
	for (auto &modifier : node.modifiers) {
		switch (modifier->type) {
		case ResultModifierType::ORDER_MODIFIER: {
			auto &order_modifier = modifier->Cast<OrderModifier>();
			result += " ORDER BY " + WriteOrderList(order_modifier.orders, &node);
			break;
		}
		case ResultModifierType::LIMIT_MODIFIER: {
			auto &limit_modifier = modifier->Cast<LimitModifier>();
			if (limit_modifier.limit) {
				result += " LIMIT " + WriteExpression(*limit_modifier.limit);
			}
			if (limit_modifier.offset) {
				result += " OFFSET " + WriteExpression(*limit_modifier.offset);
			}
			break;
		}
		case ResultModifierType::DISTINCT_MODIFIER:
			// handled in WriteSelectNode
			break;
		default:
			throw InternalException("PostgresSQLWriter: unsupported result modifier - should have been blocked by "
			                        "SupportsPushdown");
		}
	}
	return result;
}

//===--------------------------------------------------------------------===//
// DML
//===--------------------------------------------------------------------===//
//! Postgres truncates identifiers to 63 bytes - longer names are made unique instead
static string PostgresColumnAlias(const string &name, idx_t index) {
	static constexpr idx_t MAX_IDENTIFIER_LENGTH = 63;
	if (name.size() <= MAX_IDENTIFIER_LENGTH) {
		return name;
	}
	auto suffix = "_" + to_string(index);
	idx_t length = MAX_IDENTIFIER_LENGTH - suffix.size();
	// do not cut a UTF-8 sequence in half
	while (length > 0 && (static_cast<uint8_t>(name[length]) & 0xC0) == 0x80) {
		length--;
	}
	return name.substr(0, length) + suffix;
}

//! The Postgres type of a column, if DuckDB reads it with a type that does not correspond to its Postgres type
//! (e.g. JSON columns read as VARCHAR, enums, composite types) - values assigned to these columns must be
//! converted to the Postgres type. Returns an empty string for regular columns.
static string PostgresNativeColumnType(TableCatalogEntry &table, const ColumnDefinition &column, bool &known) {
	auto &pg_table = table.Cast<PostgresTableEntry>();
	auto index = column.Logical().index;
	known = index < pg_table.postgres_type_names.size();
	string type_name = known ? pg_table.postgres_type_names[index] : string();
	bool requires_native = false;
	if (index < pg_table.postgres_types.size()) {
		auto info = pg_table.postgres_types[index].info;
		requires_native = info != PostgresTypeAnnotation::STANDARD && info != PostgresTypeAnnotation::FIXED_LENGTH_CHAR;
	}
	if (column.Type().id() == LogicalTypeId::VARCHAR && known) {
		static const case_insensitive_set_t TEXT_TYPES = {"\"pg_catalog\".\"text\"",
		                                                  "\"pg_catalog\".\"varchar\"",
		                                                  "\"pg_catalog\".\"bpchar\"",
		                                                  "\"pg_catalog\".\"name\"",
		                                                  "\"text\"",
		                                                  "\"varchar\"",
		                                                  "\"bpchar\""};
		requires_native = requires_native || TEXT_TYPES.count(type_name) == 0;
	}
	requires_native =
	    requires_native || TypeVisitor::Contains(column.Type(), [](const LogicalType &type) {
		    return type.HasAlias() || type.id() == LogicalTypeId::ENUM || type.id() == LogicalTypeId::STRUCT;
	    });
	if (!requires_native) {
		return string();
	}
	if (!known || type_name.empty()) {
		// the Postgres type is not known
		known = false;
		return "?";
	}
	return type_name;
}

string PostgresSQLWriter::WriteAssignment(const ParsedExpression &expr, TableCatalogEntry &table,
                                          const ColumnDefinition &column,
                                          optional_ptr<const LogicalType> source_type_override) {
	if (expr.GetExpressionClass() == ExpressionClass::DEFAULT) {
		return "DEFAULT";
	}
	auto &target_type = column.Type();
	Value value;
	if (PostgresCapabilities::TryGetConstantValue(expr, value) && value.IsNull()) {
		// NULL is converted to the column type
		return "NULL";
	}
	auto expr_str = WriteExpression(expr);
	if (PostgresCapabilities::TryGetConstantValue(expr, value)) {
		bool known;
		if (value.IsNull() || !PostgresNativeColumnType(table, column, known).empty()) {
			// untyped literals are converted to the column type by Postgres - verify that DuckDB can convert
			// the value as well (e.g. that it is a valid enum value)
			if (!value.IsNull() && !value.TryCastAs(context, target_type)) {
				throw PostgresPushdownNotSupported("invalid value for a column of type " + target_type.ToString());
			}
			if (value.IsNull() || value.type().id() == LogicalTypeId::VARCHAR) {
				return expr_str;
			}
		} else {
			// cast constants locally - with DuckDB's cast semantics
			auto cast_result = value.TryCastAs(context, target_type);
			if (cast_result && PostgresCapabilities::SupportsValue(*cast_result)) {
				return WriteConstant(*cast_result);
			}
		}
	}
	auto source_type = source_type_override ? *source_type_override : InferType(expr);
	if (source_type.id() == LogicalTypeId::INVALID) {
		throw PostgresPushdownNotSupported("cannot determine the type of an assigned value");
	}
	return WriteAssignmentCast(expr_str, source_type, table, column);
}

string PostgresSQLWriter::WriteAssignmentCast(const string &expr_str, const LogicalType &source_type,
                                              TableCatalogEntry &table, const ColumnDefinition &column) {
	auto &target_type = column.Type();
	bool known;
	auto native_type = PostgresNativeColumnType(table, column, known);
	if (!native_type.empty()) {
		if (source_type == target_type && (target_type.id() != LogicalTypeId::VARCHAR || source_type.HasAlias())) {
			// e.g. an enum value of the same type, or excluded.<column>
			return expr_str;
		}
		if (!known || (target_type.id() != LogicalTypeId::VARCHAR && target_type.id() != LogicalTypeId::ENUM)) {
			throw PostgresPushdownNotSupported("unsupported assignment to a column of type " + target_type.ToString());
		}
		// DuckDB converts the value to a string (or enum value) - Postgres parses the text representation
		auto text = source_type.id() == LogicalTypeId::VARCHAR ? expr_str
		                                                       : WriteCast(expr_str, source_type, LogicalType::VARCHAR);
		return "CAST(" + text + " AS " + native_type + ")";
	}
	if (source_type == target_type) {
		return expr_str;
	}
	// DuckDB casts the value to the column type - Postgres would apply an assignment cast
	return WriteCast(expr_str, source_type, target_type);
}

string PostgresSQLWriter::WriteReturningList(const vector<unique_ptr<ParsedExpression>> &returning_list) {
	if (returning_list.empty()) {
		return string();
	}
	bool cast_results = true;
	for (auto &expr : returning_list) {
		if (expr->GetExpressionClass() == ExpressionClass::STAR) {
			cast_results = false;
		}
	}
	string result = " RETURNING ";
	for (idx_t i = 0; i < returning_list.size(); i++) {
		if (i > 0) {
			result += ", ";
		}
		auto &expr = *returning_list[i];
		auto expr_str = WriteExpression(expr);
		result += cast_results ? WriteResultCast(expr_str, i) : expr_str;
		if (!expr.GetAlias().empty()) {
			result += " AS " + WriteIdentifier(expr.GetAlias());
		} else if (expr.GetExpressionClass() != ExpressionClass::STAR &&
		           expr.GetExpressionClass() != ExpressionClass::COLUMN_REF) {
			result += " AS " + PostgresUtils::WriteIdentifier(PostgresColumnAlias(expr.ToString(), i));
		}
	}
	return result;
}

TableCatalogEntry &PostgresSQLWriter::GetTargetTable(const TableRef &ref) {
	if (ref.type != TableReferenceType::BASE_TABLE || IsCTEReference(ref.Cast<BaseTableRef>())) {
		throw PostgresPushdownNotSupported("unsupported DML target");
	}
	auto table = LookupTable(ref.Cast<BaseTableRef>());
	if (!table) {
		throw PostgresPushdownNotSupported("DML target table not found");
	}
	return *table;
}

string PostgresSQLWriter::WriteSetClause(const UpdateSetInfo &set_info, TableCatalogEntry &table, bool on_conflict) {
	string result;
	for (idx_t i = 0; i < set_info.columns.size(); i++) {
		if (i > 0) {
			result += ", ";
		}
		auto &column_name = set_info.columns[i];
		if (!table.ColumnExists(column_name)) {
			throw PostgresPushdownNotSupported("column not found");
		}
		auto &column = table.GetColumn(column_name);
		auto &expr = *set_info.expressions[i];
		optional_ptr<const LogicalType> source_type;
		if (on_conflict && expr.GetExpressionClass() == ExpressionClass::COLUMN_REF) {
			// excluded.<column> has the type of the column
			auto &colref = expr.Cast<ColumnRefExpression>();
			auto &names = colref.ColumnNames();
			if (names.size() == 2 && names[0] == "excluded" && table.ColumnExists(names[1])) {
				source_type = &table.GetColumn(names[1]).Type();
			}
		}
		result += WriteColumnName(table, column) + " = " + WriteAssignment(expr, table, column, source_type);
	}
	return result;
}

string PostgresSQLWriter::WriteUpdateNode(const UpdateQueryNode &node) {
	auto &table = GetTargetTable(*node.table);
	string result = WriteCTEMap(node.cte_map);
	table_scopes.emplace_back();
	table_scopes.back().push_back(node.table);
	if (node.from_table) {
		table_scopes.back().push_back(node.from_table);
	}
	result += "UPDATE " + WriteTableRef(*node.table);
	result += " SET " + WriteSetClause(*node.set_info, table, false);
	if (node.from_table) {
		result += " FROM " + WriteTableRef(*node.from_table);
	}
	if (node.set_info->condition) {
		result += " WHERE " + WriteExpression(*node.set_info->condition);
	}
	result += WriteReturningList(node.returning_list);
	table_scopes.pop_back();
	return result;
}

string PostgresSQLWriter::WriteDeleteNode(const DeleteQueryNode &node) {
	GetTargetTable(*node.table);
	string result = WriteCTEMap(node.cte_map);
	table_scopes.emplace_back();
	table_scopes.back().push_back(node.table);
	for (auto &using_clause : node.using_clauses) {
		table_scopes.back().push_back(using_clause);
	}
	result += "DELETE FROM " + WriteTableRef(*node.table);
	for (idx_t i = 0; i < node.using_clauses.size(); i++) {
		result += i == 0 ? " USING " : ", ";
		result += WriteTableRef(*node.using_clauses[i]);
	}
	if (node.condition) {
		result += " WHERE " + WriteExpression(*node.condition);
	}
	result += WriteReturningList(node.returning_list);
	table_scopes.pop_back();
	return result;
}

//! The types of the result columns of a query, as bound by DuckDB
static vector<LogicalType> PostgresBindQueryTypes(ClientContext &context, const Identifier &catalog_name,
                                                  const QueryNode &node, const CommonTableExpressionMap &cte_map) {
	auto bind_node = node.Copy();
	for (auto &cte : cte_map.map) {
		bind_node->cte_map.map.insert(cte.first, cte.second->Copy());
	}
	identifier_set_t cte_names;
	PostgresSQLWriter::CollectCTENames(*bind_node, cte_names);
	PostgresSQLWriter::QualifyTables(*bind_node, catalog_name, cte_names);
	try {
		auto binder = Binder::CreateBinder(context);
		return binder->Bind(*bind_node).types;
	} catch (std::exception &) {
		throw PostgresPushdownNotSupported("cannot determine the types of the inserted values");
	}
}

static PostgresColumnDefault PostgresGetColumnDefault(TableCatalogEntry &table, const ColumnDefinition &column) {
	auto &pg_table = table.Cast<PostgresTableEntry>();
	auto index = column.Logical().index;
	return index < pg_table.column_defaults.size() ? pg_table.column_defaults[index] : PostgresColumnDefault::NONE;
}

static bool PostgresIsNullOrDefault(const ParsedExpression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::DEFAULT) {
		return true;
	}
	Value value;
	return PostgresCapabilities::TryGetConstantValue(expr, value) && value.IsNull();
}

string PostgresSQLWriter::WriteInsertNode(const InsertQueryNode &node) {
	auto table_ptr = LookupTable(node.qualified_name);
	if (!table_ptr) {
		throw PostgresPushdownNotSupported("INSERT target table not found");
	}
	auto &table = *table_ptr;
	// the columns that are inserted into - like the regular INSERT into a Postgres table, values for generated
	// columns must be NULL (and are not sent), and NULL values for columns with a Postgres-side default are
	// replaced with the default
	vector<reference<const ColumnDefinition>> columns;
	if (node.columns.empty()) {
		for (auto &column : table.GetColumns().Physical()) {
			columns.push_back(column);
		}
	} else {
		for (auto &column_name : node.columns) {
			if (!table.ColumnExists(column_name)) {
				throw PostgresPushdownNotSupported("INSERT column not found");
			}
			columns.push_back(table.GetColumn(column_name));
		}
	}
	vector<PostgresColumnDefault> defaults;
	vector<reference<const ColumnDefinition>> target_columns;
	for (auto &column : columns) {
		defaults.push_back(PostgresGetColumnDefault(table, column));
		if (defaults.back() != PostgresColumnDefault::GENERATED) {
			target_columns.push_back(column);
		}
	}
	string result = WriteCTEMap(node.cte_map);
	result += "INSERT INTO " + PostgresUtils::WriteIdentifier(table.ParentSchema().name.GetIdentifierName()) + "." +
	          PostgresUtils::WriteIdentifier(table.name.GetIdentifierName());
	if (node.table_ref && !node.table_ref->alias.empty()) {
		result += " AS " + WriteIdentifier(node.table_ref->alias);
	}
	auto values_list = node.GetValuesList();
	if (!node.default_values && !target_columns.empty()) {
		result += " (";
		for (idx_t i = 0; i < target_columns.size(); i++) {
			result += (i > 0 ? ", " : "") + WriteColumnName(table, target_columns[i]);
		}
		result += ")";
	}
	if (node.default_values) {
		result += " DEFAULT VALUES";
	} else if (values_list) {
		// a VALUES list - every value is converted to the type of its column (this also supports DEFAULT)
		vector<string> rows;
		table_scopes.emplace_back();
		for (auto &values : values_list->values) {
			if (values.size() != columns.size()) {
				throw PostgresPushdownNotSupported("INSERT column count mismatch");
			}
			string row;
			for (idx_t col = 0; col < values.size(); col++) {
				auto &value = *values[col];
				string value_str;
				switch (defaults[col]) {
				case PostgresColumnDefault::GENERATED:
					if (!PostgresIsNullOrDefault(value)) {
						// an error is raised by the regular INSERT
						throw PostgresPushdownNotSupported("INSERT into a generated column");
					}
					continue;
				case PostgresColumnDefault::DEFAULT_VALUE:
					if (PostgresIsNullOrDefault(value)) {
						value_str = "DEFAULT";
						break;
					}
					if (value.GetExpressionClass() != ExpressionClass::CONSTANT &&
					    value.GetExpressionClass() != ExpressionClass::CAST) {
						// the value could be NULL - which is replaced with the default
						throw PostgresPushdownNotSupported(
						    "INSERT of a non-constant value into a column with a default");
					}
					value_str = WriteAssignment(value, table, columns[col]);
					break;
				default:
					value_str = WriteAssignment(value, table, columns[col]);
					break;
				}
				row += (row.empty() ? "" : ", ") + value_str;
			}
			rows.push_back("(" + row + ")");
		}
		table_scopes.pop_back();
		if (target_columns.empty()) {
			result += " DEFAULT VALUES";
			if (rows.size() != 1) {
				throw PostgresPushdownNotSupported("INSERT of multiple rows of default values");
			}
		} else {
			result += " VALUES " + StringUtil::Join(rows, ", ");
		}
	} else {
		for (auto column_default : defaults) {
			if (column_default != PostgresColumnDefault::NONE) {
				// NULL values would have to be replaced with the Postgres-side default
				throw PostgresPushdownNotSupported("INSERT ... SELECT into a column with a default");
			}
		}
		auto &select = *node.select_statement->node;
		auto source_types = PostgresBindQueryTypes(context, catalog.GetName(), select, node.cte_map);
		if (source_types.size() != columns.size()) {
			throw PostgresPushdownNotSupported("INSERT column count mismatch");
		}
		bool requires_casts = false;
		for (idx_t i = 0; i < columns.size(); i++) {
			bool known;
			if (source_types[i] != columns[i].get().Type() ||
			    !PostgresNativeColumnType(table, columns[i], known).empty()) {
				requires_casts = true;
			}
		}
		auto select_str = WriteQueryNode(select);
		if (!requires_casts) {
			result += " " + select_str;
		} else {
			// DuckDB casts the inserted values to the column types - Postgres would apply assignment casts
			string select_list;
			string aliases;
			for (idx_t i = 0; i < columns.size(); i++) {
				auto name = PostgresUtils::WriteIdentifier("c" + to_string(i));
				select_list += (i > 0 ? ", " : "") + WriteAssignmentCast(name, source_types[i], table, columns[i]);
				aliases += (i > 0 ? ", " : "") + name;
			}
			result += " SELECT " + select_list + " FROM (" + select_str + ") AS " +
			          PostgresUtils::WriteIdentifier("insert_values") + "(" + aliases + ")";
		}
	}
	if (node.on_conflict_info) {
		auto target_binding = node.table_ref ? node.table_ref->alias : Identifier();
		result +=
		    WriteOnConflict(*node.on_conflict_info, table, target_binding, target_columns, values_list.get(), columns);
	}
	table_scopes.emplace_back();
	table_scopes.back().push_back(node.table_ref);
	result += WriteReturningList(node.returning_list);
	table_scopes.pop_back();
	return result;
}

string PostgresSQLWriter::WriteOnConflict(const OnConflictInfo &info, TableCatalogEntry &table,
                                          const Identifier &target_binding,
                                          const vector<reference<const ColumnDefinition>> &columns,
                                          optional_ptr<const ExpressionListRef> values_list,
                                          const vector<reference<const ColumnDefinition>> &value_columns) {
	if (info.action_type == OnConflictAction::THROW) {
		return string();
	}
	// the conflict target - DuckDB defaults to the primary key, Postgres requires it for DO UPDATE
	vector<string> conflict_columns;
	for (auto &column_name : info.indexed_columns) {
		if (!table.ColumnExists(column_name)) {
			throw PostgresPushdownNotSupported("ON CONFLICT column not found");
		}
		conflict_columns.push_back(WriteColumnName(table, table.GetColumn(column_name)));
	}
	if (conflict_columns.empty() && info.action_type != OnConflictAction::NOTHING) {
		for (auto &constraint : table.GetConstraints()) {
			if (constraint->type != ConstraintType::UNIQUE) {
				continue;
			}
			auto &unique = constraint->Cast<UniqueConstraint>();
			if (!unique.is_primary_key) {
				continue;
			}
			if (unique.index.index != DConstants::INVALID_INDEX) {
				conflict_columns.push_back(WriteColumnName(table, table.GetColumn(unique.index)));
			} else {
				for (auto &column_name : unique.columns) {
					conflict_columns.push_back(WriteColumnName(table, table.GetColumn(column_name)));
				}
			}
		}
		if (conflict_columns.empty()) {
			throw PostgresPushdownNotSupported("ON CONFLICT DO UPDATE without a primary key");
		}
	}
	string result = " ON CONFLICT";
	if (!conflict_columns.empty()) {
		result += " (" + StringUtil::Join(conflict_columns, ", ") + ")";
	}
	if (info.action_type == OnConflictAction::NOTHING) {
		return result + " DO NOTHING";
	}
	// Postgres raises an error when a row is updated twice by the same statement - only push down VALUES lists
	// where the values of the conflict target are constants that are distinct
	if (!values_list) {
		throw PostgresPushdownNotSupported("ON CONFLICT DO UPDATE with a query");
	}
	vector<idx_t> key_indexes;
	for (auto &conflict_column : conflict_columns) {
		bool found = false;
		for (idx_t i = 0; i < value_columns.size(); i++) {
			if (WriteColumnName(table, value_columns[i]) == conflict_column) {
				key_indexes.push_back(i);
				found = true;
			}
		}
		if (!found) {
			throw PostgresPushdownNotSupported("ON CONFLICT column is not inserted");
		}
	}
	vector<vector<Value>> keys;
	for (auto &row : values_list->values) {
		vector<Value> key;
		for (auto index : key_indexes) {
			Value value;
			if (index >= row.size() || !PostgresCapabilities::TryGetConstantValue(*row[index], value)) {
				throw PostgresPushdownNotSupported("ON CONFLICT DO UPDATE with non-constant keys");
			}
			auto cast_value = value.TryCastAs(context, value_columns[index].get().Type());
			if (!cast_value) {
				throw PostgresPushdownNotSupported("ON CONFLICT DO UPDATE with invalid keys");
			}
			key.push_back(std::move(*cast_value));
		}
		for (auto &other : keys) {
			bool equal = true;
			for (idx_t k = 0; k < key.size(); k++) {
				equal = equal && !key[k].IsNull() && Value::NotDistinctFrom(key[k], other[k]);
			}
			if (equal) {
				throw PostgresPushdownNotSupported("ON CONFLICT DO UPDATE with duplicate keys");
			}
		}
		keys.push_back(std::move(key));
	}
	if (info.action_type == OnConflictAction::REPLACE) {
		// INSERT OR REPLACE updates all inserted columns that are not part of the conflict target
		string set_clause;
		for (auto &column : columns) {
			auto name = WriteColumnName(table, column);
			bool is_conflict_column = false;
			for (auto &conflict_column : conflict_columns) {
				is_conflict_column = is_conflict_column || conflict_column == name;
			}
			if (is_conflict_column) {
				continue;
			}
			set_clause += set_clause.empty() ? "" : ", ";
			set_clause += name + " = " + PostgresUtils::WriteIdentifier("excluded") + "." + name;
		}
		if (set_clause.empty()) {
			return result + " DO NOTHING";
		}
		return result + " DO UPDATE SET " + set_clause;
	}
	// DO UPDATE - the SET expressions can reference the target table and the excluded values
	auto target_ref = make_uniq<BaseTableRef>();
	target_ref->SetQualifiedName(QualifiedName(Identifier(), table.ParentSchema().name, table.name));
	auto excluded_ref = target_ref->Copy();
	excluded_ref->alias = Identifier("excluded");
	unique_ptr<TableRef> excluded_table = std::move(excluded_ref);
	unique_ptr<TableRef> target_table = std::move(target_ref);
	table_scopes.emplace_back();
	table_scopes.back().push_back(target_table);
	table_scopes.back().push_back(excluded_table);
	on_conflict_target = &table;
	on_conflict_depth = query_depth;
	on_conflict_binding = target_binding.empty() ? PostgresUtils::WriteIdentifier(table.name.GetIdentifierName())
	                                             : WriteIdentifier(target_binding);
	result += " DO UPDATE SET " + WriteSetClause(*info.set_info, table, true);
	if (info.set_info->condition) {
		result += " WHERE " + WriteExpression(*info.set_info->condition);
	}
	on_conflict_target = nullptr;
	table_scopes.pop_back();
	return result;
}

string PostgresSQLWriter::WriteMergeNode(const MergeQueryNode &node) {
	auto &table = GetTargetTable(*node.target);
	string result = WriteCTEMap(node.cte_map);
	table_scopes.emplace_back();
	table_scopes.back().push_back(node.target);
	table_scopes.back().push_back(node.source);
	result += "MERGE INTO " + WriteTableRef(*node.target) + " USING " + WriteTableRef(*node.source) + " ON ";
	if (node.join_condition) {
		result += WriteExpression(*node.join_condition);
	} else if (!node.using_columns.empty()) {
		// Postgres has no USING clause for MERGE
		auto target_binding =
		    node.target->alias.empty() ? node.target->Cast<BaseTableRef>().Table() : node.target->alias;
		if (node.source->alias.empty()) {
			throw PostgresPushdownNotSupported("MERGE USING with an unnamed source");
		}
		for (idx_t i = 0; i < node.using_columns.size(); i++) {
			auto &column = node.using_columns[i];
			result += (i > 0 ? " AND " : "") + WriteIdentifier(target_binding) + "." + WriteIdentifier(column) + " = " +
			          WriteIdentifier(node.source->alias) + "." + WriteIdentifier(column);
		}
	} else {
		result += "TRUE";
	}
	for (auto &entry : node.actions) {
		for (auto &action : entry.second) {
			switch (entry.first) {
			case MergeActionCondition::WHEN_MATCHED:
				result += " WHEN MATCHED";
				break;
			case MergeActionCondition::WHEN_NOT_MATCHED_BY_TARGET:
				result += " WHEN NOT MATCHED";
				break;
			case MergeActionCondition::WHEN_NOT_MATCHED_BY_SOURCE:
				result += " WHEN NOT MATCHED BY SOURCE";
				break;
			}
			if (action->condition) {
				result += " AND " + WriteExpression(*action->condition);
			}
			result += " THEN ";
			switch (action->action_type) {
			case MergeActionType::MERGE_UPDATE:
				result += "UPDATE SET " + WriteSetClause(*action->update_info, table, false);
				break;
			case MergeActionType::MERGE_DELETE:
				result += "DELETE";
				break;
			case MergeActionType::MERGE_DO_NOTHING:
				result += "DO NOTHING";
				break;
			case MergeActionType::MERGE_INSERT: {
				if (action->default_values) {
					result += "INSERT DEFAULT VALUES";
					break;
				}
				vector<reference<const ColumnDefinition>> columns;
				if (action->insert_columns.empty()) {
					for (auto &column : table.GetColumns().Physical()) {
						columns.push_back(column);
					}
				} else {
					for (auto &column_name : action->insert_columns) {
						if (!table.ColumnExists(column_name)) {
							throw PostgresPushdownNotSupported("MERGE INSERT column not found");
						}
						columns.push_back(table.GetColumn(column_name));
					}
				}
				if (columns.size() != action->expressions.size()) {
					throw PostgresPushdownNotSupported("MERGE INSERT column count mismatch");
				}
				string column_list;
				string value_list;
				for (idx_t i = 0; i < columns.size(); i++) {
					auto &value = *action->expressions[i];
					string value_str;
					switch (PostgresGetColumnDefault(table, columns[i])) {
					case PostgresColumnDefault::GENERATED:
						if (!PostgresIsNullOrDefault(value)) {
							throw PostgresPushdownNotSupported("MERGE INSERT into a generated column");
						}
						continue;
					case PostgresColumnDefault::DEFAULT_VALUE:
						if (PostgresIsNullOrDefault(value)) {
							value_str = "DEFAULT";
							break;
						}
						if (value.GetExpressionClass() != ExpressionClass::CONSTANT &&
						    value.GetExpressionClass() != ExpressionClass::CAST) {
							// the value could be NULL - which is replaced with the default by the regular INSERT
							throw PostgresPushdownNotSupported(
							    "MERGE INSERT of a non-constant value into a column with "
							    "a default");
						}
						value_str = WriteAssignment(value, table, columns[i]);
						break;
					default:
						value_str = WriteAssignment(value, table, columns[i]);
						break;
					}
					column_list += (column_list.empty() ? "" : ", ") + WriteColumnName(table, columns[i]);
					value_list += (value_list.empty() ? "" : ", ") + value_str;
				}
				if (column_list.empty()) {
					result += "INSERT DEFAULT VALUES";
				} else {
					result += "INSERT (" + column_list + ") VALUES (" + value_list + ")";
				}
				break;
			}
			default:
				throw InternalException("PostgresSQLWriter: unsupported MERGE action - should have been blocked by "
				                        "SupportsPushdown");
			}
		}
	}
	result += WriteReturningList(node.returning_list);
	table_scopes.pop_back();
	return result;
}

//===--------------------------------------------------------------------===//
// Statements
//===--------------------------------------------------------------------===//
PostgresWriterResult PostgresSQLWriter::WriteStatement(ClientContext &context, PostgresCatalog &catalog,
                                                       SQLStatement &statement) {
	PostgresSQLWriter writer(context, catalog);
	PostgresWriterResult result;
	switch (statement.type) {
	case StatementType::CREATE_STATEMENT: {
		auto &create = statement.Cast<CreateStatement>();
		switch (create.info->type) {
		case CatalogType::TABLE_ENTRY:
			writer.WriteCreateTable(create, result);
			break;
		case CatalogType::VIEW_ENTRY:
			writer.WriteCreateView(create.info->Cast<CreateViewInfo>(), result);
			break;
		default:
			throw InternalException("PostgresSQLWriter: unsupported CREATE statement - should have been blocked by "
			                        "SupportsPushdown");
		}
		break;
	}
	case StatementType::DROP_STATEMENT:
		result.sql = writer.WriteDrop(*statement.Cast<DropStatement>().info, result);
		break;
	case StatementType::ALTER_STATEMENT:
		result.sql = writer.WriteAlter(*statement.Cast<AlterStatement>().info, result);
		break;
	default:
		throw InternalException("PostgresSQLWriter: unsupported statement type - should have been blocked by "
		                        "SupportsPushdown");
	}
	result.requires_time_zone = writer.requires_time_zone;
	if (writer.requires_iso_date_style) {
		result.preliminary_queries.push_back("SET LOCAL DateStyle = 'ISO'");
	}
	return result;
}

static optional_ptr<LogicalCreateTable> PostgresFindCreateTable(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_CREATE_TABLE) {
		return op.Cast<LogicalCreateTable>();
	}
	for (auto &child : op.children) {
		auto result = PostgresFindCreateTable(*child);
		if (result) {
			return result;
		}
	}
	return nullptr;
}

void PostgresSQLWriter::WriteCreateTable(CreateStatement &statement, PostgresWriterResult &result) {
	auto &info = statement.info->Cast<CreateTableInfo>();
	// bind the statement to resolve the column types (and the types of the query of a CREATE TABLE AS)
	auto bind_statement = statement.Copy();
	auto &bind_info = bind_statement->Cast<CreateStatement>().info->Cast<CreateTableInfo>();
	vector<Identifier> schema_path;
	if (!info.GetQualifiedName().Schema().empty()) {
		schema_path.push_back(info.GetQualifiedName().Schema());
	}
	bind_info.SetQualifiedName(
	    QualifiedName::FromCatalogSchema(catalog.GetName(), std::move(schema_path), info.GetQualifiedName().Name()));
	if (bind_info.query) {
		identifier_set_t cte_names;
		CollectCTENames(*bind_info.query->node, cte_names);
		QualifyTables(*bind_info.query->node, catalog.GetName(), cte_names);
	}
	auto binder = Binder::CreateBinder(context);
	auto bound = binder->Bind(*bind_statement);
	auto create_table = PostgresFindCreateTable(*bound.plan);
	if (!create_table) {
		throw InternalException("PostgresSQLWriter: CREATE TABLE statement was not bound to a LogicalCreateTable");
	}
	auto &bound_info = create_table->info->Base();
	vector<LogicalType> source_types;
	for (auto &column : bound_info.columns.Logical()) {
		source_types.push_back(column.Type());
	}
	auto table_name = PostgresUtils::WriteIdentifier(create_table->schema.name.GetIdentifierName()) + "." +
	                  PostgresUtils::WriteIdentifier(bound_info.GetTableName().GetIdentifierName());
	if (info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		result.preliminary_queries.push_back("DROP TABLE IF EXISTS " + table_name);
	}
	// the tables of the schema are reloaded - so that the table is also visible when scanning the schema
	result.invalidations.push_back({CatalogType::TABLE_ENTRY, create_table->schema.name.GetIdentifierName(),
	                                bound_info.GetTableName().GetIdentifierName(), true});
	// generate the same CREATE TABLE statement as a regular CREATE TABLE
	bound_info.on_conflict = info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT
	                             ? OnCreateConflict::IGNORE_ON_CONFLICT
	                             : OnCreateConflict::ERROR_ON_CONFLICT;
	auto create_sql = GetPostgresCreateTable(bound_info);
	if (StringUtil::EndsWith(create_sql, ";")) {
		create_sql = create_sql.substr(0, create_sql.size() - 1);
	}
	if (!info.query) {
		throw InternalException("PostgresSQLWriter: CREATE TABLE without a query should have been blocked by "
		                        "SupportsPushdown");
	}
	// CREATE TABLE AS - create the table, and fill it with the result of the query
	result.preliminary_queries.push_back(std::move(create_sql));
	auto &query = *info.query->node;
	CollectNames(query);
	auto query_str = WriteQueryNode(query);
	string columns;
	string select_list;
	bool requires_casts = false;
	idx_t index = 0;
	for (auto &column : bound_info.columns.Logical()) {
		auto name = PostgresUtils::WriteIdentifier(column.Name().GetIdentifierName());
		auto alias = PostgresUtils::WriteIdentifier("c" + to_string(index));
		columns += (index > 0 ? ", " : "") + name;
		// the column types are converted to types that are supported by Postgres (e.g. HUGEINT -> DOUBLE)
		auto &source_type = source_types[index];
		auto &target_type = column.Type();
		string value = alias;
		if (source_type != target_type) {
			requires_casts = true;
			value = WriteCast(alias, source_type, target_type);
		}
		select_list += (index > 0 ? ", " : "") + value;
		index++;
	}
	result.sql = "INSERT INTO " + table_name + " (" + columns + ") ";
	if (!requires_casts) {
		result.sql += query_str;
	} else {
		string aliases;
		for (idx_t i = 0; i < index; i++) {
			aliases += (i > 0 ? ", " : "") + PostgresUtils::WriteIdentifier("c" + to_string(i));
		}
		result.sql += "SELECT " + select_list + " FROM (" + query_str + ") AS " +
		              PostgresUtils::WriteIdentifier("insert_values") + "(" + aliases + ")";
	}
	result.changed_rows = true;
}

void PostgresSQLWriter::WriteCreateView(CreateViewInfo &info, PostgresWriterResult &result) {
	auto &query = *info.query->node;
	// bind the query to determine the types DuckDB binds the result to - the view returns these types
	auto result_types = PostgresBindQueryTypes(context, catalog.GetName(), query, CommonTableExpressionMap());
	CollectNames(query);
	// all tables must be tables of this database
	identifier_set_t cte_names;
	auto query_copy = query.Copy();
	CollectCTENames(*query_copy, cte_names);
	ParsedExpressionIterator::EnumerateQueryNodeChildren(
	    *query_copy, [&](unique_ptr<ParsedExpression> &) {},
	    [&](TableRef &ref) {
		    if (ref.type != TableReferenceType::BASE_TABLE) {
			    return;
		    }
		    auto &base = ref.Cast<BaseTableRef>();
		    if (base.GetQualifiedName().Path().size() == 1 && cte_names.count(base.Table()) > 0) {
			    return;
		    }
		    if (!LookupTable(base)) {
			    throw BinderException("Views in Postgres can only reference tables of the same database - table \"%s\" "
			                          "is not a table of database \"%s\"",
			                          base.Table(), catalog.GetName());
		    }
	    });
	CollectResultNodes(query);
	this->result_types = &result_types;
	auto entry_name = ResolveEntryName(info.GetQualifiedName());
	auto view_name =
	    PostgresUtils::WriteIdentifier(entry_name.first) + "." + PostgresUtils::WriteIdentifier(entry_name.second);
	if (info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		// DuckDB replaces the view regardless of its columns
		result.preliminary_queries.push_back("DROP VIEW IF EXISTS " + view_name);
	}
	// the tables of the schema are reloaded - so that the view is also visible when scanning the schema
	result.invalidations.push_back({CatalogType::VIEW_ENTRY, entry_name.first, entry_name.second, true});
	result.sql = "CREATE VIEW " + view_name;
	if (!info.aliases.empty()) {
		result.sql += " (";
		for (idx_t i = 0; i < info.aliases.size(); i++) {
			result.sql += (i > 0 ? ", " : "") + PostgresUtils::WriteIdentifier(info.aliases[i].GetIdentifierName());
		}
		result.sql += ")";
	}
	result.sql += " AS " + WriteQueryNode(query);
	this->result_types = nullptr;
}

pair<string, string> PostgresSQLWriter::ResolveEntryName(const QualifiedName &name) {
	auto table = LookupTable(name);
	if (table) {
		return make_pair(table->ParentSchema().name.GetIdentifierName(), table->name.GetIdentifierName());
	}
	Identifier schema_name = name.Schema();
	if (schema_name.empty()) {
		auto default_schema = catalog.GetDefaultSchema();
		schema_name = default_schema ? *default_schema : Identifier("public");
	}
	return make_pair(schema_name.GetIdentifierName(), name.Name().GetIdentifierName());
}

string PostgresSQLWriter::WriteQualifiedTableName(const QualifiedName &name) {
	auto entry_name = ResolveEntryName(name);
	return PostgresUtils::WriteIdentifier(entry_name.first) + "." + PostgresUtils::WriteIdentifier(entry_name.second);
}

void PostgresSQLWriter::VerifyEntryExists(CatalogType type, const QualifiedName &name) {
	// raise the same error as the regular statement when the entry does not exist
	if (type == CatalogType::SCHEMA_ENTRY) {
		Catalog::GetSchema(context, catalog.GetName(), name.Name(), OnEntryNotFound::THROW_EXCEPTION);
		return;
	}
	Identifier schema_name = name.Schema();
	if (schema_name.empty()) {
		auto default_schema = catalog.GetDefaultSchema();
		schema_name = default_schema ? *default_schema : Identifier("public");
	}
	EntryLookupInfo lookup(type, QualifiedName(catalog.GetName(), schema_name, name.Name()));
	Catalog::GetEntry(context, lookup, OnEntryNotFound::THROW_EXCEPTION);
}

string PostgresSQLWriter::WriteDrop(const DropInfo &info, PostgresWriterResult &write_result) {
	if (info.if_not_found != OnEntryNotFound::RETURN_NULL) {
		VerifyEntryExists(info.type, info.GetQualifiedName());
	}
	PostgresCacheInvalidation invalidation;
	invalidation.type = info.type;
	if (info.type == CatalogType::SCHEMA_ENTRY) {
		invalidation.name = info.GetQualifiedName().Name().GetIdentifierName();
	} else {
		auto entry_name = ResolveEntryName(info.GetQualifiedName());
		invalidation.schema = entry_name.first;
		invalidation.name = entry_name.second;
	}
	if (info.type != CatalogType::SEQUENCE_ENTRY) {
		// sequences are not cached
		write_result.invalidations.push_back(std::move(invalidation));
	}
	string result = "DROP ";
	switch (info.type) {
	case CatalogType::TABLE_ENTRY:
		result += "TABLE ";
		break;
	case CatalogType::VIEW_ENTRY:
		result += "VIEW ";
		break;
	case CatalogType::SCHEMA_ENTRY:
		result += "SCHEMA ";
		break;
	case CatalogType::INDEX_ENTRY:
		result += "INDEX ";
		break;
	case CatalogType::SEQUENCE_ENTRY:
		result += "SEQUENCE ";
		break;
	case CatalogType::TYPE_ENTRY:
		result += "TYPE ";
		break;
	default:
		throw InternalException("PostgresSQLWriter: unsupported DROP statement - should have been blocked by "
		                        "SupportsPushdown");
	}
	if (info.if_not_found == OnEntryNotFound::RETURN_NULL) {
		result += "IF EXISTS ";
	}
	if (info.type == CatalogType::SCHEMA_ENTRY) {
		result += PostgresUtils::WriteIdentifier(info.GetQualifiedName().Name().GetIdentifierName());
	} else {
		result += WriteQualifiedTableName(info.GetQualifiedName());
	}
	if (info.cascade) {
		result += " CASCADE";
	}
	return result;
}

string PostgresSQLWriter::WriteAlter(const AlterInfo &info, PostgresWriterResult &write_result) {
	auto &alter_info = info.Cast<AlterTableInfo>();
	if (alter_info.if_not_found != OnEntryNotFound::RETURN_NULL) {
		VerifyEntryExists(CatalogType::TABLE_ENTRY, alter_info.GetQualifiedName());
	}
	// like a regular ALTER TABLE, all tables of the schema are reloaded (e.g. because of renamed tables)
	PostgresCacheInvalidation invalidation;
	invalidation.type = CatalogType::TABLE_ENTRY;
	invalidation.schema = ResolveEntryName(alter_info.GetQualifiedName()).first;
	invalidation.all_tables = true;
	write_result.invalidations.push_back(std::move(invalidation));
	auto table = LookupTable(alter_info.GetQualifiedName());
	auto column_name = [&](const Identifier &name) {
		if (table && table->ColumnExists(name)) {
			return WriteColumnName(*table, table->GetColumn(name));
		}
		return PostgresUtils::WriteIdentifier(name.GetIdentifierName());
	};
	string result = "ALTER TABLE ";
	if (alter_info.if_not_found == OnEntryNotFound::RETURN_NULL) {
		result += "IF EXISTS ";
	}
	result += WriteQualifiedTableName(alter_info.GetQualifiedName());
	switch (alter_info.alter_table_type) {
	case AlterTableType::RENAME_TABLE:
		return result + " RENAME TO " +
		       PostgresUtils::WriteIdentifier(alter_info.Cast<RenameTableInfo>().new_table_name.GetIdentifierName());
	case AlterTableType::RENAME_COLUMN: {
		auto &rename_info = alter_info.Cast<RenameColumnInfo>();
		return result + " RENAME COLUMN " + column_name(rename_info.old_name) + " TO " +
		       PostgresUtils::WriteIdentifier(rename_info.new_name.GetIdentifierName());
	}
	case AlterTableType::ADD_COLUMN: {
		auto &add_info = alter_info.Cast<AddColumnInfo>();
		auto &column = add_info.new_column;
		LogicalType type = column.Type();
		if (type.id() == LogicalTypeId::UNBOUND) {
			type = PostgresCapabilities::ResolveType(UnboundType::GetTypeExpression(type)->Cast<TypeExpression>());
		}
		result += " ADD COLUMN ";
		if (add_info.if_column_not_exists) {
			result += "IF NOT EXISTS ";
		}
		return result + PostgresUtils::WriteIdentifier(column.Name().GetIdentifierName()) + " " +
		       PostgresUtils::TypeToString(PostgresUtils::ToPostgresType(type));
	}
	case AlterTableType::REMOVE_COLUMN: {
		auto &remove_info = alter_info.Cast<RemoveColumnInfo>();
		result += " DROP COLUMN ";
		if (remove_info.if_column_exists) {
			result += "IF EXISTS ";
		}
		result += column_name(remove_info.removed_column);
		if (remove_info.cascade) {
			result += " CASCADE";
		}
		return result;
	}
	case AlterTableType::SET_NOT_NULL:
		return result + " ALTER COLUMN " + column_name(alter_info.Cast<SetNotNullInfo>().column_path[0]) +
		       " SET NOT NULL";
	case AlterTableType::DROP_NOT_NULL:
		return result + " ALTER COLUMN " + column_name(alter_info.Cast<DropNotNullInfo>().column_path[0]) +
		       " DROP NOT NULL";
	default:
		throw InternalException("PostgresSQLWriter: unsupported ALTER statement - should have been blocked by "
		                        "SupportsPushdown");
	}
}

//===--------------------------------------------------------------------===//
// Query nodes
//===--------------------------------------------------------------------===//
string PostgresSQLWriter::WriteCTEMap(const CommonTableExpressionMap &cte_map) {
	if (cte_map.map.empty()) {
		return string();
	}
	bool has_recursive = false;
	for (auto &kv : cte_map.map) {
		if (kv.second->query_node && kv.second->query_node->type == QueryNodeType::RECURSIVE_CTE_NODE) {
			has_recursive = true;
			break;
		}
	}
	string result = "WITH ";
	if (has_recursive) {
		result += "RECURSIVE ";
	}
	bool first_cte = true;
	for (auto &kv : cte_map.map) {
		if (!first_cte) {
			result += ", ";
		}
		auto &cte = *kv.second;
		result += WriteIdentifier(kv.first);
		if (!cte.aliases.empty()) {
			result += " (";
			for (idx_t k = 0; k < cte.aliases.size(); k++) {
				if (k > 0) {
					result += ", ";
				}
				result += WriteIdentifier(cte.aliases[k]);
			}
			result += ")";
		}
		result += " AS ";
		switch (cte.materialized) {
		case CTEMaterialize::CTE_MATERIALIZE_ALWAYS:
			result += "MATERIALIZED ";
			break;
		case CTEMaterialize::CTE_MATERIALIZE_NEVER:
			result += "NOT MATERIALIZED ";
			break;
		default:
			break;
		}
		result += "(" + WriteQueryNode(*cte.query_node) + ")";
		first_cte = false;
	}
	result += " ";
	return result;
}

string PostgresSQLWriter::WriteSelectNode(const SelectNode &node) {
	string result = WriteCTEMap(node.cte_map);
	table_scopes.emplace_back();
	if (node.from_table) {
		table_scopes.back().push_back(node.from_table);
	}
	result += "SELECT ";
	bool has_distinct = false;
	for (auto &modifier : node.modifiers) {
		if (modifier->type == ResultModifierType::DISTINCT_MODIFIER) {
			has_distinct = true;
			auto &distinct_modifier = modifier->Cast<DistinctModifier>();
			result += "DISTINCT ";
			if (!distinct_modifier.distinct_on_targets.empty()) {
				result += "ON (";
				for (idx_t i = 0; i < distinct_modifier.distinct_on_targets.size(); i++) {
					// the targets must match the leading ORDER BY keys - including their collation
					auto &target = *distinct_modifier.distinct_on_targets[i];
					auto target_str = WriteOrderingKey(target);
					if (!catalog.HasBinaryCollation()) {
						target_str = WriteOrderedOperand(target_str, InferType(target));
					}
					result += (i > 0 ? ", " : "") + target_str;
				}
				result += ") ";
			}
			break;
		}
	}
	// casts are not applied with DISTINCT if the ORDER BY contains expressions (Postgres requires the ORDER BY
	// expressions to appear in the select list then)
	bool allow_casts = !has_distinct;
	if (has_distinct) {
		allow_casts = true;
		for (auto &modifier : node.modifiers) {
			if (modifier->type != ResultModifierType::ORDER_MODIFIER) {
				continue;
			}
			for (auto &order : modifier->Cast<OrderModifier>().orders) {
				auto expr_class = order.expression->GetExpressionClass();
				if (expr_class != ExpressionClass::CONSTANT && expr_class != ExpressionClass::COLUMN_REF) {
					allow_casts = false;
				}
			}
		}
	}
	// the types DuckDB binds the result to are known for the result nodes - if the select-list entries map
	// one-to-one to the result columns
	bool use_result_types = result_nodes.count(&node) > 0;
	for (auto &select_expr : node.select_list) {
		if (select_expr->GetExpressionClass() == ExpressionClass::STAR) {
			use_result_types = false;
		}
	}
	for (idx_t i = 0; i < node.select_list.size(); i++) {
		if (i > 0) {
			result += ", ";
		}
		auto &select_expr = *node.select_list[i];
		auto expr_str = WriteExpression(select_expr);
		auto expr_class = select_expr.GetExpressionClass();
		if (!allow_casts || expr_class == ExpressionClass::STAR || expr_class == ExpressionClass::COLUMN_REF) {
			result += expr_str;
		} else if (use_result_types) {
			result += WriteResultCast(expr_str, i);
		} else {
			auto type = InferType(select_expr);
			result += type.id() == LogicalTypeId::INVALID ? expr_str : WriteResultCast(expr_str, type);
		}
		if (!select_expr.GetAlias().empty()) {
			result += " AS " + WriteIdentifier(select_expr.GetAlias());
		} else if (select_expr.GetExpressionClass() != ExpressionClass::STAR &&
		           select_expr.GetExpressionClass() != ExpressionClass::COLUMN_REF) {
			// name the column the way DuckDB does - Postgres names e.g. CAST(x AS INTEGER) after "x", which can
			// make references to "x" (e.g. in the ORDER BY) ambiguous, or break references to the column by name
			result += " AS " + PostgresUtils::WriteIdentifier(PostgresColumnAlias(select_expr.ToString(), i));
		}
	}
	if (node.from_table && node.from_table->type != TableReferenceType::EMPTY_FROM) {
		result += " FROM " + WriteTableRef(*node.from_table);
	}
	if (node.where_clause) {
		result += " WHERE " + WriteExpression(*node.where_clause);
	}
	if (!node.groups.grouping_sets.empty()) {
		result += " GROUP BY ";
		if (node.groups.grouping_sets.size() == 1) {
			bool first = true;
			for (auto &group_index : node.groups.grouping_sets[0]) {
				if (!first) {
					result += ", ";
				}
				result += WriteOrderingKey(*node.groups.group_expressions[group_index]);
				first = false;
			}
			if (node.groups.grouping_sets[0].empty()) {
				result += "()";
			}
		} else {
			result += "GROUPING SETS (";
			bool first_set = true;
			for (auto &grouping_set : node.groups.grouping_sets) {
				if (!first_set) {
					result += ", ";
				}
				result += "(";
				bool first = true;
				for (auto &group_index : grouping_set) {
					if (!first) {
						result += ", ";
					}
					result += WriteOrderingKey(*node.groups.group_expressions[group_index]);
					first = false;
				}
				result += ")";
				first_set = false;
			}
			result += ")";
		}
	}
	if (node.having) {
		result += " HAVING " + WriteExpression(*node.having);
	}
	if (node.qualify || node.sample) {
		throw InternalException("PostgresSQLWriter: QUALIFY / SAMPLE should have been blocked by SupportsPushdown");
	}
	result += WriteResultModifiers(node, &node.select_list);
	table_scopes.pop_back();
	return result;
}

string PostgresSQLWriter::WriteSetOperationNode(const SetOperationNode &node) {
	string result = WriteCTEMap(node.cte_map);
	for (idx_t i = 0; i < node.children.size(); i++) {
		if (i > 0) {
			switch (node.setop_type) {
			case SetOperationType::UNION:
				result += node.setop_all ? " UNION ALL " : " UNION ";
				break;
			case SetOperationType::EXCEPT:
				result += node.setop_all ? " EXCEPT ALL " : " EXCEPT ";
				break;
			case SetOperationType::INTERSECT:
				result += node.setop_all ? " INTERSECT ALL " : " INTERSECT ";
				break;
			default:
				throw InternalException("PostgresSQLWriter: unsupported set operation - should have been blocked by "
				                        "SupportsPushdown");
			}
		}
		result += "(" + WriteQueryNode(*node.children[i]) + ")";
	}
	result += WriteResultModifiers(node);
	return result;
}

string PostgresSQLWriter::WriteRecursiveCTENode(const RecursiveCTENode &node) {
	string result = WriteCTEMap(node.cte_map);
	result += "(" + WriteQueryNode(*node.left) + ")";
	result += node.union_all ? " UNION ALL " : " UNION ";
	result += "(" + WriteQueryNode(*node.right) + ")";
	result += WriteResultModifiers(node);
	return result;
}

string PostgresSQLWriter::WriteQueryNode(const QueryNode &node) {
	query_depth++;
	bool has_ctes = !node.cte_map.map.empty();
	if (has_ctes) {
		cte_scopes.push_back(node.cte_map);
	}
	string result;
	switch (node.type) {
	case QueryNodeType::SELECT_NODE:
		result = WriteSelectNode(node.Cast<SelectNode>());
		break;
	case QueryNodeType::SET_OPERATION_NODE:
		result = WriteSetOperationNode(node.Cast<SetOperationNode>());
		break;
	case QueryNodeType::RECURSIVE_CTE_NODE:
		result = WriteRecursiveCTENode(node.Cast<RecursiveCTENode>());
		break;
	case QueryNodeType::UPDATE_QUERY_NODE:
		result = WriteUpdateNode(node.Cast<UpdateQueryNode>());
		break;
	case QueryNodeType::DELETE_QUERY_NODE:
		result = WriteDeleteNode(node.Cast<DeleteQueryNode>());
		break;
	case QueryNodeType::INSERT_QUERY_NODE:
		result = WriteInsertNode(node.Cast<InsertQueryNode>());
		break;
	case QueryNodeType::MERGE_QUERY_NODE:
		result = WriteMergeNode(node.Cast<MergeQueryNode>());
		break;
	default:
		throw InternalException("PostgresSQLWriter: unsupported query node type - should have been blocked by "
		                        "SupportsPushdown");
	}
	if (has_ctes) {
		cte_scopes.pop_back();
	}
	query_depth--;
	return result;
}

} // namespace duckdb
