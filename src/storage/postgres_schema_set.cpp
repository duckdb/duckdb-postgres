#include "storage/postgres_schema_set.hpp"

#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/common/shared_ptr.hpp"

#include "postgres_utils.hpp"
#include "storage/postgres_index_set.hpp"
#include "storage/postgres_table_set.hpp"
#include "storage/postgres_type_set.hpp"
#include "storage/postgres_transaction.hpp"
#include "storage/postgres_table_set.hpp"
#include "storage/postgres_catalog.hpp"

namespace duckdb {

PostgresSchemaSet::PostgresSchemaSet(Catalog &catalog, string schema_to_load_p)
    : PostgresCatalogSet(catalog, false), schema_to_load(std::move(schema_to_load_p)) {
}

vector<unique_ptr<PostgresResultSlice>> SliceResult(PostgresResult &schemas, unique_ptr<PostgresResult> to_slice_ptr) {
	auto shared_result = shared_ptr<PostgresResult>(to_slice_ptr.release());
	auto &to_slice = *shared_result;

	vector<unique_ptr<PostgresResultSlice>> result;
	idx_t current_offset = 0;
	for (idx_t schema_idx = 0; schema_idx < schemas.Count(); schema_idx++) {
		auto oid = schemas.GetInt64(schema_idx, 0);
		idx_t start = current_offset;
		for (; current_offset < to_slice.Count(); current_offset++) {
			auto current_oid = to_slice.GetInt64(current_offset, 0);
			if (current_oid != oid) {
				break;
			}
		}
		result.push_back(make_uniq<PostgresResultSlice>(shared_result, start, current_offset));
	}
	return result;
}

// Variant of SliceResult
// uses field name instead of an integer oid.
// Used by the information_schema introspection path where namespace oids are'nt available.
static vector<unique_ptr<PostgresResultSlice>> SliceResultByName(PostgresResult &schemas,
                                                                 unique_ptr<PostgresResult> to_slice_ptr) {
	auto shared_result = shared_ptr<PostgresResult>(to_slice_ptr.release());
	auto &to_slice = *shared_result;

	vector<unique_ptr<PostgresResultSlice>> result;
	idx_t current_offset = 0;
	for (idx_t schema_idx = 0; schema_idx < schemas.Count(); schema_idx++) {
		auto name = schemas.GetString(schema_idx, 0);
		idx_t start = current_offset;
		for (; current_offset < to_slice.Count(); current_offset++) {
			if (to_slice.GetString(current_offset, 0) != name) {
				break;
			}
		}
		result.push_back(make_uniq<PostgresResultSlice>(shared_result, start, current_offset));
	}
	return result;
}

string PostgresSchemaSet::GetInitializeQuery(const string &schema) {
	string base_query = R"(
SELECT oid, nspname
FROM pg_namespace
${CONDITION}
ORDER BY oid;
)";
	string condition;
	if (!schema.empty()) {
		condition += "WHERE pg_namespace.nspname=" + PostgresUtils::WriteLiteral(schema);
	}
	return StringUtil::Replace(base_query, "${CONDITION}", condition);
}

string PostgresSchemaSet::GetInitializeQueryInformationSchema(const string &schema) {
	string base_query = R"(
SELECT schema_name
FROM information_schema.schemata
WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
${CONDITION}
ORDER BY schema_name;
)";
	string condition;
	if (!schema.empty()) {
		condition += "AND schema_name=" + PostgresUtils::WriteLiteral(schema);
	}
	return StringUtil::Replace(base_query, "${CONDITION}", condition);
}

void PostgresSchemaSet::LoadEntriesInformationSchema(ClientContext &context, PostgresTransaction &transaction) {
	string schema_query = GetInitializeQueryInformationSchema(schema_to_load);
	string tables_query = PostgresTableSet::GetInitializeQueryInformationSchema(schema_to_load);
	// Reading enums, composite types and indexes differs across DBs that emulate PG
	// choice to either return those empty or have vendor specific queries for each.
	// empty for now
	string enum_types_query = "SELECT NULL, NULL, NULL, NULL LIMIT 0;\n";
	string composite_types_query = "SELECT NULL, NULL, NULL, NULL, NULL LIMIT 0;\n";
	string index_query = "SELECT NULL, NULL, NULL LIMIT 0;\n";

	auto full_query = schema_query + tables_query + enum_types_query + composite_types_query + index_query;

	auto results = transaction.ExecuteQueries(context, full_query);
	auto result = std::move(results[0]);
	results.erase(results.begin());
	auto rows = result->Count();

	auto tables = SliceResultByName(*result, std::move(results[0]));
	auto enums = SliceResultByName(*result, std::move(results[1]));
	auto composite_types = SliceResultByName(*result, std::move(results[2]));
	auto indexes = SliceResultByName(*result, std::move(results[3]));
	for (idx_t row = 0; row < rows; row++) {
		auto schema_name = result->GetString(row, 0);
		CreateSchemaInfo info;
		info.SetSchema(Identifier(schema_name));
		info.internal = PostgresSchemaEntry::SchemaIsInternal(schema_name);
		auto schema = make_shared_ptr<PostgresSchemaEntry>(catalog, info, std::move(tables[row]), std::move(enums[row]),
		                                                   std::move(composite_types[row]), std::move(indexes[row]));
		CreateEntry(transaction, std::move(schema));
	}
}

void PostgresSchemaSet::LoadEntries(ClientContext &context, PostgresTransaction &transaction) {
	if (PostgresUtils::UseInformationSchemaIntrospection(context)) {
		LoadEntriesInformationSchema(context, transaction);
		return;
	}
	auto &pg_catalog = catalog.Cast<PostgresCatalog>();
	auto pg_version = pg_catalog.GetPostgresVersion();
	string schema_query = PostgresSchemaSet::GetInitializeQuery(schema_to_load);
	string tables_query = PostgresTableSet::GetInitializeQuery(schema_to_load);
	string enum_types_query = PostgresTypeSet::GetInitializeEnumsQuery(pg_version, schema_to_load);
	string composite_types_query = PostgresTypeSet::GetInitializeCompositesQuery(schema_to_load);
	string index_query = PostgresIndexSet::GetInitializeQuery(schema_to_load);

	auto full_query = schema_query + tables_query + enum_types_query + composite_types_query + index_query;

	auto results = transaction.ExecuteQueries(context, full_query);
	auto result = std::move(results[0]);
	results.erase(results.begin());
	auto rows = result->Count();

	auto tables = SliceResult(*result, std::move(results[0]));
	auto enums = SliceResult(*result, std::move(results[1]));
	auto composite_types = SliceResult(*result, std::move(results[2]));
	auto indexes = SliceResult(*result, std::move(results[3]));
	for (idx_t row = 0; row < rows; row++) {
		auto oid = result->GetInt64(row, 0);
		auto schema_name = result->GetString(row, 1);
		CreateSchemaInfo info;
		info.SetQualifiedName(
		    QualifiedName(info.GetQualifiedName().Catalog(), Identifier(schema_name), info.GetQualifiedName().Name()));
		info.internal = PostgresSchemaEntry::SchemaIsInternal(schema_name);
		auto schema = make_shared_ptr<PostgresSchemaEntry>(catalog, info, std::move(tables[row]), std::move(enums[row]),
		                                                   std::move(composite_types[row]), std::move(indexes[row]));
		CreateEntry(transaction, std::move(schema));
	}
}

optional_ptr<CatalogEntry> PostgresSchemaSet::CreateSchema(PostgresTransaction &transaction, CreateSchemaInfo &info) {
	string create_sql = "CREATE SCHEMA ";
	if (info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
		create_sql += " IF NOT EXISTS";
	}
	create_sql += PostgresUtils::WriteIdentifier(info.GetQualifiedName().Schema().GetIdentifierName());
	transaction.Query(create_sql);
	auto info_copy = info.Copy();
	info.internal = PostgresSchemaEntry::SchemaIsInternal(info_copy->GetQualifiedName().Schema().GetIdentifierName());
	auto schema_entry = make_shared_ptr<PostgresSchemaEntry>(catalog, info_copy->Cast<CreateSchemaInfo>());
	return CreateEntry(transaction, std::move(schema_entry));
}

} // namespace duckdb
