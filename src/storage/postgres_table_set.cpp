#include "storage/postgres_table_set.hpp"

#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/common/string_util.hpp"

#include "postgres_conversion.hpp"
#include "postgres_utils.hpp"
#include "storage/postgres_transaction.hpp"
#include "storage/postgres_schema_entry.hpp"

namespace duckdb {

PostgresTableSet::PostgresTableSet(PostgresSchemaEntry &schema, unique_ptr<PostgresResultSlice> table_result_p)
    : PostgresInSchemaSet(schema, !table_result_p), table_result(std::move(table_result_p)) {
}

string PostgresTableSet::GetInitializeQuery(const string &schema, const string &table) {
	vector<string> vec;
	vec.push_back(schema);
	return GetInitializeQuery(vec, table);
}

string PostgresTableSet::GetInitializeQuery(const vector<string> &schemas, const string &table) {
	string base_query = R"(
SELECT pg_namespace.oid AS namespace_id, relname, relpages, attname,
    pg_type.typname type_name, atttypmod type_modifier, pg_attribute.attndims ndim,
    attnum, pg_attribute.attnotnull AS notnull, NULL constraint_id,
    NULL constraint_type, NULL constraint_key, type_ns.nspname AS type_schema,
    col_desc.description AS column_comment,
    tbl_desc.description AS table_comment
FROM pg_class
JOIN pg_namespace ON relnamespace = pg_namespace.oid
JOIN pg_attribute ON pg_class.oid=pg_attribute.attrelid
JOIN pg_type ON atttypid=pg_type.oid
JOIN pg_namespace type_ns ON pg_type.typnamespace = type_ns.oid
LEFT JOIN pg_description col_desc ON col_desc.objoid=pg_class.oid AND col_desc.objsubid=pg_attribute.attnum
LEFT JOIN pg_description tbl_desc ON tbl_desc.objoid=pg_class.oid AND tbl_desc.objsubid=0
WHERE attnum > 0 AND relkind IN ('r', 'v', 'm', 'f', 'p') ${CONDITION}
UNION ALL
SELECT pg_namespace.oid AS namespace_id, relname, NULL relpages, NULL attname, NULL type_name,
    NULL type_modifier, NULL ndim, NULL attnum, NULL AS notnull,
    pg_constraint.oid AS constraint_id, contype AS constraint_type,
    conkey AS constraint_key, NULL AS type_schema,
    NULL AS column_comment, NULL AS table_comment
FROM pg_class
JOIN pg_namespace ON relnamespace = pg_namespace.oid
JOIN pg_constraint ON (pg_class.oid=pg_constraint.conrelid)
WHERE relkind IN ('r', 'v', 'm', 'f', 'p') AND contype IN ('p', 'u') ${CONDITION}
ORDER BY namespace_id, relname, attnum, constraint_id;
)";
	string condition;
	if (schemas.size() > 0) {
		condition += "AND pg_namespace.nspname IN (" + PostgresUtils::WriteLiteralsCommaSeparated(schemas) + ")";
	}
	if (!table.empty()) {
		condition += "AND relname=" + PostgresUtils::WriteLiteral(table);
	}
	return StringUtil::Replace(base_query, "${CONDITION}", condition);
}

string PostgresTableSet::GetInitializeQueryInformationSchema(const string &schema, const string &table) {
	vector<string> vec;
	vec.push_back(schema);
	return GetInitializeQueryInformationSchema(vec, table);
}

string PostgresTableSet::GetInitializeQueryInformationSchema(const vector<string> &schemas, const string &table) {
	string base_query = R"(
SELECT table_schema AS namespace_id, table_name AS relname, 0 AS relpages, column_name AS attname,
    data_type AS type_name, -1 AS type_modifier, 0 AS ndim, ordinal_position AS attnum,
    CASE WHEN is_nullable = 'NO' THEN 't' ELSE 'f' END AS notnull,
    NULL AS constraint_id, NULL AS constraint_type, NULL AS constraint_key,
    NULL AS type_schema, NULL AS column_comment, NULL AS table_comment
FROM information_schema.columns
WHERE table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast') ${CONDITION}
ORDER BY table_schema, table_name, ordinal_position;
)";
	string condition;
	if (schemas.size() > 0) {
		condition += "AND table_schema IN (" + PostgresUtils::WriteLiteralsCommaSeparated(schemas) + ")";
	}
	if (!table.empty()) {
		condition += " AND table_name=" + PostgresUtils::WriteLiteral(table);
	}
	return StringUtil::Replace(base_query, "${CONDITION}", condition);
}

void PostgresTableSet::AddColumn(optional_ptr<PostgresTransaction> transaction,
                                 optional_ptr<PostgresSchemaEntry> schema, PostgresResult &result, idx_t row,
                                 PostgresTableInfo &table_info) {
	PostgresTypeData type_info;
	idx_t column_index = 3;
	auto column_name = result.GetString(row, column_index);
	type_info.type_name = PostgresUtils::DataTypeToTypeName(result.GetString(row, column_index + 1));
	type_info.type_modifier = result.GetInt64(row, column_index + 2);
	type_info.array_dimensions = result.GetInt64(row, column_index + 3);
	bool is_not_null = result.GetBool(row, column_index + 5);
	string column_comment = result.IsNull(row, 13) ? "" : result.GetString(row, 13);
	idx_t type_schema_index = column_index + 9;
	if (!result.IsNull(row, type_schema_index)) {
		type_info.type_schema = result.GetString(row, type_schema_index);
	}
	string default_value;

	PostgresType postgres_type;
	auto column_type = PostgresUtils::TypeToLogicalType(transaction, schema, type_info, postgres_type);
	table_info.postgres_types.push_back(std::move(postgres_type));
	table_info.postgres_names.push_back(column_name);
	ColumnDefinition column(Identifier(std::move(column_name)), std::move(column_type));
	if (!column_comment.empty()) {
		column.SetComment(Value(column_comment));
	}
	if (!default_value.empty()) {
		auto expressions = Parser::ParseExpressionList(default_value);
		if (expressions.empty()) {
			throw InternalException("Expression list is empty");
		}
		column.SetDefaultValue(std::move(expressions[0]));
	}
	auto &create_info = *table_info.create_info;
	if (is_not_null) {
		create_info.constraints.push_back(
		    make_uniq<NotNullConstraint>(LogicalIndex(create_info.columns.PhysicalColumnCount())));
	}
	create_info.columns.AddColumn(std::move(column));
}

void PostgresTableSet::AddConstraint(PostgresResult &result, idx_t row, PostgresTableInfo &table_info) {
	idx_t column_index = 9;
	auto constraint_type = result.GetString(row, column_index + 1);
	auto constraint_key = result.GetString(row, column_index + 2);
	if (constraint_key.empty() || constraint_key.front() != '{' || constraint_key.back() != '}') {
		// invalid constraint key
		D_ASSERT(0);
		return;
	}

	auto &create_info = *table_info.create_info;
	auto splits = StringUtil::Split(constraint_key.substr(1, constraint_key.size() - 2), ",");
	vector<Identifier> columns;
	for (auto &split : splits) {
		auto index = std::stoull(split);
		if (index <= 0 || index > create_info.columns.LogicalColumnCount()) {
			return;
		}
		columns.push_back(create_info.columns.GetColumn(LogicalIndex(index - 1)).Name());
	}

	create_info.constraints.push_back(make_uniq<UniqueConstraint>(std::move(columns), constraint_type == "p"));
}

void PostgresTableSet::AddColumnOrConstraint(optional_ptr<PostgresTransaction> transaction,
                                             optional_ptr<PostgresSchemaEntry> schema, PostgresResult &result,
                                             idx_t row, PostgresTableInfo &table_info) {
	if (result.IsNull(row, 3)) {
		// constraint
		AddConstraint(result, row, table_info);
	} else {
		AddColumn(transaction, schema, result, row, table_info);
	}
}

void PostgresTableSet::CreateEntries(PostgresTransaction &transaction, PostgresResult &result, idx_t start, idx_t end) {
	vector<unique_ptr<PostgresTableInfo>> tables;
	unique_ptr<PostgresTableInfo> info;

	for (idx_t row = start; row < end; row++) {
		auto table_name = result.GetString(row, 1);
		if (!info || info->GetTableName() != table_name) {
			if (info) {
				tables.push_back(std::move(info));
			}
			info = make_uniq<PostgresTableInfo>(schema, table_name);
			info->approx_num_pages = result.IsNull(row, 2) ? 0 : result.GetInt64(row, 2);
			// Read table-level comment from column 14
			if (!result.IsNull(row, 14)) {
				info->create_info->comment = Value(result.GetString(row, 14));
			}
		}
		AddColumnOrConstraint(&transaction, &schema, result, row, *info);
	}
	if (info) {
		tables.push_back(std::move(info));
	}
	for (auto &tbl_info : tables) {
		auto table_entry = make_shared_ptr<PostgresTableEntry>(catalog, schema, *tbl_info);
		CreateEntry(transaction, std::move(table_entry));
	}
}

void PostgresTableSet::LoadEntries(ClientContext &context, PostgresTransaction &transaction) {
	if (table_result) {
		CreateEntries(transaction, table_result->GetResult(), table_result->start, table_result->end);
		table_result.reset();
	} else {
		auto query = PostgresUtils::UseInformationSchemaIntrospection(context)
		                 ? GetInitializeQueryInformationSchema(schema.name.GetIdentifierName())
		                 : GetInitializeQuery(schema.name.GetIdentifierName());

		auto result = transaction.Query(query);
		auto rows = result->Count();

		CreateEntries(transaction, *result, 0, rows);
	}
}

string PostgresTableSet::GetStalenessQuery(ClientContext &context) const {
	if (!PostgresUtils::StalenessQueryEnabled(context)) {
		return string();
	}
	auto custom_query = PostgresUtils::GetCustomStalenessQuery(context);
	if (!custom_query.empty()) {
		if (custom_query.find("${SCHEMA}") == string::npos) {
			throw InvalidInputException("pg_staleness_query must contain a ${SCHEMA} placeholder");
		}
		return StringUtil::Replace(custom_query, "${SCHEMA}",
		                           PostgresUtils::WriteLiteral(schema.name.GetIdentifierName()));
	}
	string base_query = R"(
SELECT pg_class.oid, relname, pg_class.xmin
FROM pg_class
JOIN pg_namespace ON relnamespace = pg_namespace.oid
WHERE relkind IN ('r', 'v', 'm', 'f', 'p') AND pg_namespace.nspname = ${SCHEMA}
ORDER BY pg_class.oid;
)";
	return StringUtil::Replace(base_query, "${SCHEMA}", PostgresUtils::WriteLiteral(schema.name.GetIdentifierName()));
}

unique_ptr<PostgresTableInfo> PostgresTableSet::GetTableInfo(PostgresTransaction &transaction,
                                                             PostgresSchemaEntry &schema, const string &table_name) {
	auto query = PostgresUtils::UseInformationSchemaIntrospection(transaction.GetContext())
	                 ? GetInitializeQueryInformationSchema(schema.name.GetIdentifierName(), table_name)
	                 : GetInitializeQuery(schema.name.GetIdentifierName(), table_name);
	auto result = transaction.Query(query);
	auto rows = result->Count();
	if (rows == 0) {
		return nullptr;
	}
	auto table_info = make_uniq<PostgresTableInfo>(schema, table_name);
	for (idx_t row = 0; row < rows; row++) {
		AddColumnOrConstraint(&transaction, &schema, *result, row, *table_info);
	}
	table_info->approx_num_pages = result->IsNull(0, 2) ? 0 : result->GetInt64(0, 2);
	// Read table-level comment from 14
	if (!result->IsNull(0, 14)) {
		table_info->create_info->comment = Value(result->GetString(0, 14));
	}
	return table_info;
}

unique_ptr<PostgresTableInfo> PostgresTableSet::GetTableInfo(ClientContext &context, PostgresConnection &connection,
                                                             const string &schema_name, const string &table_name) {
	auto query = PostgresUtils::UseInformationSchemaIntrospection(context)
	                 ? GetInitializeQueryInformationSchema(schema_name, table_name)
	                 : GetInitializeQuery(schema_name, table_name);
	auto result = connection.Query(context, query);
	auto rows = result->Count();
	if (rows == 0) {
		throw InvalidInputException("Table %s does not contain any columns.", table_name);
	}
	auto table_info = make_uniq<PostgresTableInfo>(schema_name, table_name);
	for (idx_t row = 0; row < rows; row++) {
		AddColumnOrConstraint(nullptr, nullptr, *result, row, *table_info);
	}
	table_info->approx_num_pages = result->IsNull(0, 2) ? 0 : result->GetInt64(0, 2);
	// Read table-level comment
	if (!result->IsNull(0, 14)) {
		table_info->create_info->comment = Value(result->GetString(0, 14));
	}
	return table_info;
}

optional_ptr<CatalogEntry> PostgresTableSet::ReloadEntry(PostgresTransaction &transaction, const string &table_name) {
	auto table_info = GetTableInfo(transaction, schema, table_name);
	if (!table_info) {
		return nullptr;
	}
	auto table_entry = make_shared_ptr<PostgresTableEntry>(catalog, schema, *table_info);
	auto result = CreateEntry(transaction, std::move(table_entry));
	RefreshStalenessSignature(transaction, /*use_transaction_connection=*/false);
	return result;
}

// FIXME - this is almost entirely copied from TableCatalogEntry::ColumnsToSQL - should be unified
string PostgresColumnsToSQL(const ColumnList &columns, const vector<unique_ptr<Constraint>> &constraints) {
	std::stringstream ss;

	ss << "(";

	// find all columns that have NOT NULL specified, but are NOT primary key
	// columns
	logical_index_set_t not_null_columns;
	logical_index_set_t unique_columns;
	logical_index_set_t pk_columns;
	unordered_set<string> multi_key_pks;
	vector<string> extra_constraints;
	for (auto &constraint : constraints) {
		if (constraint->type == ConstraintType::NOT_NULL) {
			auto &not_null = constraint->Cast<NotNullConstraint>();
			not_null_columns.insert(not_null.index);
		} else if (constraint->type == ConstraintType::UNIQUE) {
			auto &pk = constraint->Cast<UniqueConstraint>();
			vector<Identifier> constraint_columns = pk.columns;
			if (pk.index.index != DConstants::INVALID_INDEX) {
				// no columns specified: single column constraint
				if (pk.is_primary_key) {
					pk_columns.insert(pk.index);
				} else {
					unique_columns.insert(pk.index);
				}
			} else {
				// multi-column constraint, this constraint needs to go at the end after
				// all columns
				if (pk.is_primary_key) {
					// multi key pk column: insert set of columns into multi_key_pks
					for (auto &col : pk.columns) {
						multi_key_pks.insert(col.GetIdentifierName());
					}
				}
				string base = pk.is_primary_key ? "PRIMARY KEY(" : "UNIQUE(";
				for (idx_t i = 0; i < pk.columns.size(); i++) {
					if (i > 0) {
						base += ", ";
					}
					base += PostgresUtils::WriteIdentifier(pk.columns[i].GetIdentifierName());
				}
				extra_constraints.push_back(base + ")");
			}
		} else if (constraint->type == ConstraintType::FOREIGN_KEY) {
			auto &fk = constraint->Cast<ForeignKeyConstraint>();
			if (fk.info.type == ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE ||
			    fk.info.type == ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE) {
				extra_constraints.push_back(constraint->ToString());
			}
		} else {
			extra_constraints.push_back(constraint->ToString());
		}
	}

	for (auto &column : columns.Logical()) {
		if (column.Oid() > 0) {
			ss << ", ";
		}
		ss << PostgresUtils::WriteIdentifier(column.Name().GetIdentifierName()) << " ";
		ss << PostgresUtils::TypeToString(column.Type());
		bool not_null = not_null_columns.find(column.Logical()) != not_null_columns.end();
		bool is_single_key_pk = pk_columns.find(column.Logical()) != pk_columns.end();
		bool is_multi_key_pk = multi_key_pks.find(column.Name().GetIdentifierName()) != multi_key_pks.end();
		bool is_unique = unique_columns.find(column.Logical()) != unique_columns.end();
		if (not_null && !is_single_key_pk && !is_multi_key_pk) {
			// NOT NULL but not a primary key column
			ss << " NOT NULL";
		}
		if (is_single_key_pk) {
			// single column pk: insert constraint here
			ss << " PRIMARY KEY";
		}
		if (is_unique) {
			// single column unique: insert constraint here
			ss << " UNIQUE";
		}
		if (column.Generated()) {
			ss << " GENERATED ALWAYS AS(" << column.GeneratedExpression().ToString() << ")";
		} else if (column.HasDefaultValue()) {
			ss << " DEFAULT(" << column.DefaultValue().ToString() << ")";
		}
	}
	// print any extra constraints that still need to be printed
	for (auto &extra_constraint : extra_constraints) {
		ss << ", ";
		ss << extra_constraint;
	}

	ss << ")";
	return ss.str();
}

string GetPostgresCreateTable(CreateTableInfo &info) {
	for (idx_t i = 0; i < info.columns.LogicalColumnCount(); i++) {
		auto &col = info.columns.GetColumnMutable(LogicalIndex(i));
		col.SetType(PostgresUtils::ToPostgresType(col.GetType()));
	}

	std::stringstream ss;
	ss << "CREATE TABLE ";
	if (info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
		ss << "IF NOT EXISTS ";
	}
	if (!info.GetQualifiedName().Schema().empty()) {
		ss << PostgresUtils::WriteIdentifier(info.GetQualifiedName().Schema().GetIdentifierName());
		ss << ".";
	}
	ss << PostgresUtils::WriteIdentifier(info.GetTableName().GetIdentifierName());
	ss << PostgresColumnsToSQL(info.columns, info.constraints);
	ss << ";";
	return ss.str();
}

optional_ptr<CatalogEntry> PostgresTableSet::CreateTable(PostgresTransaction &transaction, BoundCreateTableInfo &info) {
	auto create_sql = GetPostgresCreateTable(info.Base());
	transaction.Query(create_sql);
	auto tbl_entry = make_shared_ptr<PostgresTableEntry>(catalog, schema, info.Base());
	auto result = CreateEntry(transaction, std::move(tbl_entry));
	RefreshStalenessSignature(transaction, /*use_transaction_connection=*/true);
	return result;
}

string PostgresTableSet::GetAlterTablePrefix(const string &name, optional_ptr<CatalogEntry> entry) {
	string sql = "ALTER TABLE ";
	sql += PostgresUtils::WriteIdentifier(schema.name.GetIdentifierName()) + ".";
	sql += PostgresUtils::WriteIdentifier(entry ? entry->name.GetIdentifierName() : name);
	return sql;
}

string PostgresTableSet::GetAlterTableColumnName(const string &name, optional_ptr<CatalogEntry> entry) {
	if (!entry || entry->type != CatalogType::TABLE_ENTRY) {
		return name;
	}
	auto &table = entry->Cast<PostgresTableEntry>();
	Identifier column_name = Identifier(name);
	auto column_index = table.GetColumnIndex(column_name, true);
	if (!column_index.IsValid()) {
		return name;
	}
	return table.postgres_names[column_index.index];
}

string PostgresTableSet::GetAlterTablePrefix(ClientContext &context, PostgresTransaction &transaction,
                                             const string &name) {
	auto entry = GetEntry(context, transaction, name);
	return GetAlterTablePrefix(name, entry);
}

void PostgresTableSet::AlterTable(ClientContext &context, PostgresTransaction &transaction, RenameTableInfo &info) {
	string sql = GetAlterTablePrefix(context, transaction, info.GetQualifiedName().Name().GetIdentifierName());
	sql += " RENAME TO ";
	sql += PostgresUtils::WriteIdentifier(info.new_table_name.GetIdentifierName());
	transaction.Query(sql);
}

void PostgresTableSet::AlterTable(ClientContext &context, PostgresTransaction &transaction, RenameColumnInfo &info) {
	auto entry = GetEntry(context, transaction, info.GetQualifiedName().Name().GetIdentifierName());
	string sql = GetAlterTablePrefix(info.GetQualifiedName().Name().GetIdentifierName(), entry);
	sql += " RENAME COLUMN  ";
	string column_name = GetAlterTableColumnName(info.old_name.GetIdentifierName(), entry);
	sql += PostgresUtils::WriteIdentifier(column_name);
	sql += " TO ";
	sql += PostgresUtils::WriteIdentifier(info.new_name.GetIdentifierName());

	transaction.Query(sql);
}

void PostgresTableSet::AlterTable(ClientContext &context, PostgresTransaction &transaction, AddColumnInfo &info) {
	string sql = GetAlterTablePrefix(context, transaction, info.GetQualifiedName().Name().GetIdentifierName());
	sql += " ADD COLUMN  ";
	if (info.if_column_not_exists) {
		sql += "IF NOT EXISTS ";
	}
	sql += PostgresUtils::WriteIdentifier(info.new_column.Name().GetIdentifierName());
	sql += " ";
	sql += info.new_column.Type().ToString();

	if (info.new_column.HasDefaultValue()) {
		const ParsedExpression &default_expr = info.new_column.DefaultValue();
		if (default_expr.GetExpressionType() != ExpressionType::VALUE_CONSTANT) {
			throw BinderException("Unsupported ALTER TABLE DEFAULT expression type - "
			                      "only constant DEFAULT expressions are supported");
		}
		const ConstantExpression &default_const_expr = default_expr.Cast<ConstantExpression>();
		std::string sql_str = default_const_expr.GetValue().ToSQLString();
		sql += " DEFAULT ";
		sql += sql_str;
	}

	transaction.Query(sql);
}

void PostgresTableSet::AlterTable(ClientContext &context, PostgresTransaction &transaction, RemoveColumnInfo &info) {
	auto entry = GetEntry(context, transaction, info.GetQualifiedName().Name().GetIdentifierName());
	string sql = GetAlterTablePrefix(info.GetQualifiedName().Name().GetIdentifierName(), entry);
	sql += " DROP COLUMN  ";
	if (info.if_column_exists) {
		sql += "IF EXISTS ";
	}
	string column_name = GetAlterTableColumnName(info.removed_column.GetIdentifierName(), entry);
	sql += PostgresUtils::WriteIdentifier(column_name);
	transaction.Query(sql);
}

void PostgresTableSet::AlterTable(ClientContext &context, PostgresTransaction &transaction, AlterTableInfo &alter) {
	switch (alter.alter_table_type) {
	case AlterTableType::RENAME_TABLE:
		AlterTable(context, transaction, alter.Cast<RenameTableInfo>());
		break;
	case AlterTableType::RENAME_COLUMN:
		AlterTable(context, transaction, alter.Cast<RenameColumnInfo>());
		break;
	case AlterTableType::ADD_COLUMN:
		AlterTable(context, transaction, alter.Cast<AddColumnInfo>());
		break;
	case AlterTableType::REMOVE_COLUMN:
		AlterTable(context, transaction, alter.Cast<RemoveColumnInfo>());
		break;
	default:
		throw BinderException("Unsupported ALTER TABLE type - Postgres tables only "
		                      "support RENAME TABLE, RENAME COLUMN, "
		                      "ADD COLUMN and DROP COLUMN");
	}
	ClearEntries();
}
} // namespace duckdb
