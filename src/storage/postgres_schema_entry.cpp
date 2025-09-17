#include "storage/postgres_schema_entry.hpp"
#include "storage/postgres_table_entry.hpp"
#include "storage/postgres_transaction.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"

namespace duckdb {

PostgresSchemaEntry::PostgresSchemaEntry(Catalog &catalog, CreateSchemaInfo &info)
    : SchemaCatalogEntry(catalog, info), tables(*this), indexes(*this), types(*this) {
}

PostgresSchemaEntry::PostgresSchemaEntry(Catalog &catalog, CreateSchemaInfo &info,
                                         unique_ptr<PostgresResultSlice> tables, unique_ptr<PostgresResultSlice> enums,
                                         unique_ptr<PostgresResultSlice> composite_types,
                                         unique_ptr<PostgresResultSlice> indexes)
    : SchemaCatalogEntry(catalog, info), tables(*this, std::move(tables)), indexes(*this, std::move(indexes)),
      types(*this, std::move(enums), std::move(composite_types)) {
}

bool PostgresSchemaEntry::SchemaIsInternal(const string &name) {
	if (name == "information_schema" || StringUtil::StartsWith(name, "pg_")) {
		return true;
	}
	return false;
}

PostgresTransaction &GetPostgresTransaction(CatalogTransaction transaction) {
	if (!transaction.transaction) {
		throw InternalException("No transaction!?");
	}
	return transaction.transaction->Cast<PostgresTransaction>();
}

void PostgresSchemaEntry::TryDropEntry(ClientContext &context, CatalogType catalog_type, const string &name) {
	DropInfo info;
	info.type = catalog_type;
	info.name = name;
	info.cascade = false;
	info.if_not_found = OnEntryNotFound::RETURN_NULL;
	DropEntry(context, info);
}

optional_ptr<CatalogEntry> PostgresSchemaEntry::CreateTable(CatalogTransaction transaction,
                                                            BoundCreateTableInfo &info) {
	auto &postgres_transaction = GetPostgresTransaction(transaction);
	auto &base_info = info.Base();
	auto table_name = base_info.table;
	if (base_info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		// CREATE OR REPLACE - drop any existing entries first (if any)
		TryDropEntry(transaction.GetContext(), CatalogType::TABLE_ENTRY, table_name);
	}
	return tables.CreateTable(postgres_transaction, info);
}

optional_ptr<CatalogEntry> PostgresSchemaEntry::CreateFunction(CatalogTransaction transaction,
                                                               CreateFunctionInfo &info) {
	throw BinderException("Postgres databases do not support creating functions");
}

optional_ptr<CatalogEntry> PostgresSchemaEntry::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
                                                            TableCatalogEntry &table) {
	auto &postgres_transaction = GetPostgresTransaction(transaction);
	return indexes.CreateIndex(postgres_transaction, info, table);
}

string PGGetCreateViewSQL(PostgresSchemaEntry &schema, CreateViewInfo &info) {
	string sql;
	sql = "CREATE VIEW ";
	sql += PostgresUtils::QuotePostgresIdentifier(schema.name) + ".";
	sql += PostgresUtils::QuotePostgresIdentifier(info.view_name);
	sql += " ";
	if (!info.aliases.empty()) {
		sql += "(";
		for (idx_t i = 0; i < info.aliases.size(); i++) {
			if (i > 0) {
				sql += ", ";
			}
			auto &alias = info.aliases[i];
			sql += PostgresUtils::QuotePostgresIdentifier(alias);
		}
		sql += ") ";
	}
	sql += "AS ";
	sql += info.query->ToString();
	return sql;
}

optional_ptr<CatalogEntry> PostgresSchemaEntry::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	if (info.sql.empty()) {
		throw BinderException("Cannot create view in Postgres that originated from an empty SQL statement");
	}
	if (info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT ||
	    info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
		auto current_entry = GetEntry(transaction, CatalogType::VIEW_ENTRY, info.view_name);
		if (current_entry) {
			if (info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
				return current_entry;
			}
			// CREATE OR REPLACE - drop any existing entries first (if any)
			TryDropEntry(transaction.GetContext(), CatalogType::VIEW_ENTRY, info.view_name);
		}
	}
	auto &postgres_transaction = GetPostgresTransaction(transaction);
	postgres_transaction.Query(PGGetCreateViewSQL(*this, info));
	return tables.ReloadEntry(postgres_transaction, info.view_name);
}

optional_ptr<CatalogEntry> PostgresSchemaEntry::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
	auto &postgres_transaction = GetPostgresTransaction(transaction);
	auto type_name = info.name;
	if (info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		// CREATE OR REPLACE - drop any existing entries first (if any)
		TryDropEntry(transaction.GetContext(), CatalogType::TYPE_ENTRY, info.name);
	}
	return types.CreateType(postgres_transaction, info);
}

optional_ptr<CatalogEntry> PostgresSchemaEntry::CreateSequence(CatalogTransaction transaction,
                                                               CreateSequenceInfo &info) {
	throw BinderException("Postgres databases do not support creating sequences");
}

optional_ptr<CatalogEntry> PostgresSchemaEntry::CreateTableFunction(CatalogTransaction transaction,
                                                                    CreateTableFunctionInfo &info) {
	throw BinderException("Postgres databases do not support creating table functions");
}

optional_ptr<CatalogEntry> PostgresSchemaEntry::CreateCopyFunction(CatalogTransaction transaction,
                                                                   CreateCopyFunctionInfo &info) {
	throw BinderException("Postgres databases do not support creating copy functions");
}

optional_ptr<CatalogEntry> PostgresSchemaEntry::CreatePragmaFunction(CatalogTransaction transaction,
                                                                     CreatePragmaFunctionInfo &info) {
	throw BinderException("Postgres databases do not support creating pragma functions");
}

optional_ptr<CatalogEntry> PostgresSchemaEntry::CreateCollation(CatalogTransaction transaction,
                                                                CreateCollationInfo &info) {
	throw BinderException("Postgres databases do not support creating collations");
}

void PostgresSchemaEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {
	if (info.type != AlterType::ALTER_TABLE) {
		throw BinderException("Only altering tables is supported for now");
	}
	auto &postgres_transaction = GetPostgresTransaction(transaction);
	auto &alter = info.Cast<AlterTableInfo>();
	tables.AlterTable(postgres_transaction, alter);
}

bool CatalogTypeIsSupported(CatalogType type) {
	switch (type) {
	case CatalogType::INDEX_ENTRY:
	case CatalogType::TABLE_ENTRY:
	case CatalogType::TYPE_ENTRY:
	case CatalogType::VIEW_ENTRY:
		return true;
	default:
		return false;
	}
}

void PostgresSchemaEntry::Scan(ClientContext &context, CatalogType type,
                               const std::function<void(CatalogEntry &)> &callback) {
	if (!CatalogTypeIsSupported(type)) {
		return;
	}
	auto &postgres_transaction = PostgresTransaction::Get(context, catalog);
	GetCatalogSet(type).Scan(postgres_transaction, callback);
}
void PostgresSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	throw NotImplementedException("Scan without context not supported");
}

void PostgresSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
	info.schema = name;
	auto &postgres_transaction = PostgresTransaction::Get(context, catalog);
	GetCatalogSet(info.type).DropEntry(postgres_transaction, info);
}

optional_ptr<CatalogEntry> PostgresSchemaEntry::LookupEntry(CatalogTransaction transaction,
                                                            const EntryLookupInfo &lookup_info) {
	auto catalog_type = lookup_info.GetCatalogType();
	if (!CatalogTypeIsSupported(catalog_type)) {
		return nullptr;
	}
	auto &postgres_transaction = GetPostgresTransaction(transaction);
	return GetCatalogSet(catalog_type).GetEntry(postgres_transaction, lookup_info.GetEntryName());
}

PostgresCatalogSet &PostgresSchemaEntry::GetCatalogSet(CatalogType type) {
	switch (type) {
	case CatalogType::TABLE_ENTRY:
	case CatalogType::VIEW_ENTRY:
		return tables;
	case CatalogType::INDEX_ENTRY:
		return indexes;
	case CatalogType::TYPE_ENTRY:
		return types;
	default:
		throw InternalException("Type not supported for GetCatalogSet");
	}
}

} // namespace duckdb
