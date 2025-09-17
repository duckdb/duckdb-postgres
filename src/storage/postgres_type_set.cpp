#include "storage/postgres_type_set.hpp"
#include "storage/postgres_transaction.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "storage/postgres_type_entry.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "storage/postgres_schema_entry.hpp"
#include "duckdb/parser/parser.hpp"

namespace duckdb {

struct PGTypeInfo {
	idx_t oid;
	idx_t typrelid;
	string name;
};

PostgresTypeSet::PostgresTypeSet(PostgresSchemaEntry &schema, unique_ptr<PostgresResultSlice> enum_result_p,
                                 unique_ptr<PostgresResultSlice> composite_type_result_p)
    : PostgresInSchemaSet(schema, !enum_result_p), enum_result(std::move(enum_result_p)),
      composite_type_result(std::move(composite_type_result_p)) {
}

string PostgresTypeSet::GetInitializeEnumsQuery(PostgresVersion version, const string &schema) {
	if (version.major_v < 8 || (version.major_v == 8 && version.minor_v < 3)) {
		// pg_enum support has been present since v8.3 - https://www.postgresql.org/docs/8.3/catalog-pg-enum.html
		// for older postgres versions we don't support enums instead
		return R"(
SELECT 0 AS oid, 0 AS enumtypid, '' AS typname, '' AS enumlabel
LIMIT 0;
)";
	}
	string base_query = R"(
SELECT n.oid, enumtypid, typname, enumlabel
FROM pg_enum e
JOIN pg_type t ON e.enumtypid = t.oid
JOIN pg_namespace AS n ON (typnamespace=n.oid)
${CONDITION}
ORDER BY n.oid, enumtypid, enumsortorder;
)";
	string condition;
	if (!schema.empty()) {
		condition += "WHERE n.nspname=" + KeywordHelper::WriteQuoted(schema);
	}
	return StringUtil::Replace(base_query, "${CONDITION}", condition);
}

void PostgresTypeSet::CreateEnum(PostgresTransaction &transaction, PostgresResult &result, idx_t start_row,
                                 idx_t end_row) {
	PostgresType postgres_type;
	CreateTypeInfo info;
	postgres_type.oid = result.GetInt64(start_row, 1);
	info.name = result.GetString(start_row, 2);
	// construct the enum
	idx_t enum_count = end_row - start_row;
	Vector duckdb_levels(LogicalType::VARCHAR, enum_count);
	for (idx_t enum_idx = 0; enum_idx < enum_count; enum_idx++) {
		duckdb_levels.SetValue(enum_idx, result.GetString(start_row + enum_idx, 3));
	}
	info.type = LogicalType::ENUM(duckdb_levels, enum_count);
	info.type.SetAlias(info.name);
	auto type_entry = make_shared_ptr<PostgresTypeEntry>(catalog, schema, info, postgres_type);
	CreateEntry(transaction, std::move(type_entry));
}

void PostgresTypeSet::InitializeEnums(PostgresTransaction &transaction, PostgresResultSlice &enums) {
	auto &result = enums.GetResult();
	idx_t start = enums.start;
	idx_t end = enums.end;
	idx_t current_oid = idx_t(-1);
	for (idx_t row = start; row < end; row++) {
		auto oid = result.GetInt64(row, 1);
		if (oid != current_oid) {
			if (row > start) {
				CreateEnum(transaction, result, start, row);
			}
			start = row;
			current_oid = oid;
		}
	}
	if (end > start) {
		CreateEnum(transaction, result, start, end);
	}
}

string PostgresTypeSet::GetInitializeCompositesQuery(const string &schema) {
	string base_query = R"(
SELECT n.oid, t.typrelid AS id, t.typname as type, pg_attribute.attname, sub_type.typname
FROM pg_type t
JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
JOIN pg_class ON pg_class.oid = t.typrelid
JOIN pg_attribute ON attrelid=t.typrelid
JOIN pg_type sub_type ON (pg_attribute.atttypid=sub_type.oid)
WHERE pg_class.relkind = 'c'
AND t.typtype='c'
${CONDITION}
ORDER BY n.oid, t.oid, attrelid, attnum;
)";
	string condition;
	if (!schema.empty()) {
		condition += "AND n.nspname=" + KeywordHelper::WriteQuoted(schema);
	}
	return StringUtil::Replace(base_query, "${CONDITION}", condition);
}

void PostgresTypeSet::CreateCompositeType(PostgresTransaction &transaction, PostgresResult &result, idx_t start_row,
                                          idx_t end_row) {
	PostgresType postgres_type;
	CreateTypeInfo info;
	postgres_type.oid = result.GetInt64(start_row, 1);
	info.name = result.GetString(start_row, 2);

	child_list_t<LogicalType> child_types;
	for (idx_t row = start_row; row < end_row; row++) {
		auto type_name = result.GetString(row, 3);
		PostgresTypeData type_data;
		type_data.type_name = result.GetString(row, 4);
		PostgresType child_type;
		child_types.push_back(
		    make_pair(type_name, PostgresUtils::TypeToLogicalType(&transaction, &schema, type_data, child_type)));
		postgres_type.children.push_back(std::move(child_type));
	}
	info.type = LogicalType::STRUCT(std::move(child_types));
	info.type.SetAlias(info.name);
	auto type_entry = make_shared_ptr<PostgresTypeEntry>(catalog, schema, info, postgres_type);
	CreateEntry(transaction, std::move(type_entry));
}

void PostgresTypeSet::InitializeCompositeTypes(PostgresTransaction &transaction, PostgresResultSlice &composite_types) {
	auto &result = composite_types.GetResult();
	idx_t start = composite_types.start;
	idx_t end = composite_types.end;
	idx_t current_oid = idx_t(-1);
	for (idx_t row = start; row < end; row++) {
		auto oid = result.GetInt64(row, 1);
		if (oid != current_oid) {
			if (row > start) {
				CreateCompositeType(transaction, result, start, row);
			}
			start = row;
			current_oid = oid;
		}
	}
	if (end > start) {
		CreateCompositeType(transaction, result, start, end);
	}
}

void PostgresTypeSet::LoadEntries(PostgresTransaction &transaction) {
	if (!enum_result || !composite_type_result) {
		throw InternalException("PostgresTypeSet::LoadEntries not defined without enum/composite type result");
	}
	InitializeEnums(transaction, *enum_result);
	InitializeCompositeTypes(transaction, *composite_type_result);
	enum_result.reset();
	composite_type_result.reset();
}

string GetCreateTypeSQL(CreateTypeInfo &info) {
	string sql = "CREATE TYPE ";
	sql += KeywordHelper::WriteQuoted(info.name, '"');
	sql += " AS ";
	switch (info.type.id()) {
	case LogicalTypeId::ENUM: {
		sql += "ENUM(";
		auto enum_size = EnumType::GetSize(info.type);
		for (idx_t i = 0; i < enum_size; i++) {
			if (i > 0) {
				sql += ", ";
			}
			auto enum_value = EnumType::GetString(info.type, i).GetString();
			sql += KeywordHelper::WriteQuoted(enum_value, '\'');
		}
		sql += ")";
		break;
	}
	case LogicalTypeId::STRUCT: {
		auto child_count = StructType::GetChildCount(info.type);
		sql += "(";
		for (idx_t c = 0; c < child_count; c++) {
			if (c > 0) {
				sql += ", ";
			}
			sql += KeywordHelper::WriteQuoted(StructType::GetChildName(info.type, c), '"');
			sql += " ";
			sql += PostgresUtils::TypeToString(StructType::GetChildType(info.type, c));
		}
		sql += ")";
		break;
	}
	default:
		throw BinderException("Unsupported type for CREATE TYPE in Postgres");
	}
	return sql;
}

optional_ptr<CatalogEntry> PostgresTypeSet::CreateType(PostgresTransaction &transaction, CreateTypeInfo &info) {
	auto &conn = transaction.GetConnection();

	auto create_sql = GetCreateTypeSQL(info);
	conn.Execute(create_sql);
	info.type.SetAlias(info.name);
	auto pg_type = PostgresUtils::CreateEmptyPostgresType(info.type);
	auto type_entry = make_shared_ptr<PostgresTypeEntry>(catalog, schema, info, pg_type);
	return CreateEntry(transaction, std::move(type_entry));
}

} // namespace duckdb
