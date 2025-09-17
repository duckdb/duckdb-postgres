#include "storage/postgres_index_set.hpp"
#include "storage/postgres_schema_entry.hpp"
#include "storage/postgres_transaction.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "storage/postgres_index_entry.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"

namespace duckdb {

PostgresIndexSet::PostgresIndexSet(PostgresSchemaEntry &schema, unique_ptr<PostgresResultSlice> index_result_p)
    : PostgresInSchemaSet(schema, !index_result_p), index_result(std::move(index_result_p)) {
}

string PostgresIndexSet::GetInitializeQuery(const string &schema) {
	string base_query = R"(
SELECT pg_namespace.oid, tablename, indexname
FROM pg_indexes
JOIN pg_namespace ON (schemaname=nspname)
${CONDITION}
ORDER BY pg_namespace.oid;
)";
	string condition;
	if (!schema.empty()) {
		condition += "WHERE pg_namespace.nspname=" + KeywordHelper::WriteQuoted(schema);
	}
	return StringUtil::Replace(base_query, "${CONDITION}", condition);
}

void PostgresIndexSet::LoadEntries(PostgresTransaction &transaction) {
	if (!index_result) {
		throw InternalException("PostgresIndexSet::LoadEntries called without an index result defined");
	}
	auto &result = index_result->GetResult();
	for (idx_t row = index_result->start; row < index_result->end; row++) {
		auto table_name = result.GetString(row, 1);
		auto index_name = result.GetString(row, 2);
		CreateIndexInfo info;
		info.schema = schema.name;
		info.table = table_name;
		info.index_name = index_name;
		auto index_entry = make_shared_ptr<PostgresIndexEntry>(catalog, schema, info, table_name);
		CreateEntry(transaction, std::move(index_entry));
	}
	index_result.reset();
}

void PGUnqualifyColumnReferences(ParsedExpression &expr) {
	if (expr.type == ExpressionType::COLUMN_REF) {
		auto &colref = expr.Cast<ColumnRefExpression>();
		auto name = std::move(colref.column_names.back());
		colref.column_names = {std::move(name)};
		return;
	}
	ParsedExpressionIterator::EnumerateChildren(expr, PGUnqualifyColumnReferences);
}

string PGGetCreateIndexSQL(CreateIndexInfo &info, TableCatalogEntry &tbl) {
	string sql;
	sql = "CREATE";
	if (info.constraint_type == IndexConstraintType::UNIQUE) {
		sql += " UNIQUE";
	}
	sql += " INDEX ";
	sql += PostgresUtils::QuotePostgresIdentifier(info.index_name);
	sql += " ON ";
	sql += PostgresUtils::QuotePostgresIdentifier(tbl.schema.name) + ".";
	sql += PostgresUtils::QuotePostgresIdentifier(tbl.name);
	sql += "(";
	for (idx_t i = 0; i < info.parsed_expressions.size(); i++) {
		if (i > 0) {
			sql += ", ";
		}
		PGUnqualifyColumnReferences(*info.parsed_expressions[i]);
		sql += info.parsed_expressions[i]->ToString();
	}
	sql += ")";
	return sql;
}

optional_ptr<CatalogEntry> PostgresIndexSet::CreateIndex(PostgresTransaction &transaction, CreateIndexInfo &info,
                                                         TableCatalogEntry &table) {
	transaction.Query(PGGetCreateIndexSQL(info, table));
	auto index_entry = make_shared_ptr<PostgresIndexEntry>(schema.ParentCatalog(), schema, info, table.name);
	return CreateEntry(transaction, std::move(index_entry));
}

} // namespace duckdb
