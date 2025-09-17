#include "storage/postgres_catalog_set.hpp"
#include "storage/postgres_transaction.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "storage/postgres_schema_entry.hpp"

namespace duckdb {

PostgresCatalogSet::PostgresCatalogSet(Catalog &catalog, bool is_loaded_p) : catalog(catalog), is_loaded(is_loaded_p) {
}

optional_ptr<CatalogEntry> PostgresCatalogSet::GetEntry(PostgresTransaction &transaction, const string &name) {
	TryLoadEntries(transaction);
	{
		lock_guard<mutex> l(entry_lock);
		auto entry = entries.find(name);
		if (entry != entries.end()) {
			// entry found
			return transaction.ReferenceEntry(entry->second);
		}
		// check the case insensitive map if there are any entries
		auto name_entry = entry_map.find(name);
		if (name_entry != entry_map.end()) {
			// try again with the entry we found in the case insensitive map
			auto entry = entries.find(name_entry->second);
			if (entry != entries.end()) {
				// still not found
				return transaction.ReferenceEntry(entry->second);
			}
		}
	}
	// entry not found
	if (SupportReload()) {
		lock_guard<mutex> lock(load_lock);
		// try loading entries again - maybe there has been a change remotely
		auto entry = ReloadEntry(transaction, name);
		if (entry) {
			return entry;
		}
	}
	return nullptr;
}

void PostgresCatalogSet::TryLoadEntries(PostgresTransaction &transaction) {
	if (HasInternalDependencies()) {
		if (is_loaded) {
			return;
		}
	}
	lock_guard<mutex> lock(load_lock);
	if (is_loaded) {
		return;
	}
	is_loaded = true;
	LoadEntries(transaction);
}

optional_ptr<CatalogEntry> PostgresCatalogSet::ReloadEntry(PostgresTransaction &transaction, const string &name) {
	throw InternalException("PostgresCatalogSet does not support ReloadEntry");
}

void PostgresCatalogSet::DropEntry(PostgresTransaction &transaction, DropInfo &info) {
	string drop_query = "DROP ";
	drop_query += CatalogTypeToString(info.type) + " ";
	if (info.if_not_found == OnEntryNotFound::RETURN_NULL) {
		drop_query += " IF EXISTS ";
	}
	if (!info.schema.empty()) {
		drop_query += KeywordHelper::WriteQuoted(info.schema, '"') + ".";
	}
	drop_query += KeywordHelper::WriteQuoted(info.name, '"');
	if (info.cascade) {
		drop_query += "CASCADE";
	}
	transaction.Query(drop_query);

	// erase the entry from the catalog set
	lock_guard<mutex> l(entry_lock);
	entries.erase(info.name);
}

void PostgresCatalogSet::Scan(PostgresTransaction &transaction, const std::function<void(CatalogEntry &)> &callback) {
	TryLoadEntries(transaction);
	lock_guard<mutex> l(entry_lock);
	for (auto &entry : entries) {
		callback(*entry.second);
	}
}

optional_ptr<CatalogEntry> PostgresCatalogSet::CreateEntry(PostgresTransaction &transaction,
                                                           shared_ptr<CatalogEntry> entry) {
	lock_guard<mutex> l(entry_lock);
	auto result = transaction.ReferenceEntry(entry);
	if (result->name.empty()) {
		throw InternalException("PostgresCatalogSet::CreateEntry called with empty name");
	}
	entry_map.insert(make_pair(result->name, result->name));
	entries.insert(make_pair(result->name, std::move(entry)));
	return result;
}

void PostgresCatalogSet::ClearEntries() {
	lock_guard<mutex> entry_guard(entry_lock);
	entry_map.clear();
	entries.clear();
	is_loaded = false;
}

PostgresInSchemaSet::PostgresInSchemaSet(PostgresSchemaEntry &schema, bool is_loaded)
    : PostgresCatalogSet(schema.ParentCatalog(), is_loaded), schema(schema) {
}

optional_ptr<CatalogEntry> PostgresInSchemaSet::CreateEntry(PostgresTransaction &transaction,
                                                            shared_ptr<CatalogEntry> entry) {
	entry->internal = schema.internal;
	return PostgresCatalogSet::CreateEntry(transaction, std::move(entry));
}

} // namespace duckdb
