#include "storage/postgres_catalog_set.hpp"

#include "storage/postgres_catalog.hpp"
#include "storage/postgres_transaction.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "storage/postgres_schema_entry.hpp"

namespace duckdb {

PostgresCatalogSet::PostgresCatalogSet(Catalog &catalog, bool is_loaded_p)
    : catalog(catalog), is_loaded(is_loaded_p), loading_thread(thread_id()) {
}

optional_ptr<CatalogEntry> PostgresCatalogSet::GetEntry(ClientContext &context, PostgresTransaction &transaction,
                                                        const string &name) {
	TryLoadEntries(context, transaction);
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

static string ComputeStalenessSignature(PostgresResult &result) {
	if (result.ColumnCount() < 3) {
		throw InvalidInputException(
		    "pg_staleness_query must return at least 3 columns (identity, name, revision marker), got %llu",
		    result.ColumnCount());
	}
	string signature;
	auto rows = result.Count();
	for (idx_t row = 0; row < rows; row++) {
		signature += result.GetString(row, 0);
		signature += '\0';
		signature += result.GetString(row, 1);
		signature += '\0';
		signature += result.GetString(row, 2);
		signature += '\x01';
	}
	return signature;
}

//! Runs the staleness query on a short-lived pooled connection, not the caller's own.
//! Avoids pinning the caller's connection (and, under PgBouncer transaction-mode pooling,
//! a backend slot) for the round-trip.
static unique_ptr<PostgresResult> RunStalenessQuery(Catalog &catalog, ClientContext &context,
                                                    const string &staleness_query) {
	auto connection = catalog.Cast<PostgresCatalog>().GetConnectionPool().GetConnection();
	return connection.GetConnection().Query(context, staleness_query);
}

void PostgresCatalogSet::RefreshStalenessSignature(PostgresTransaction &transaction, bool use_transaction_connection) {
	auto &context = *transaction.GetContext();
	auto staleness_query = GetStalenessQuery(context);
	if (staleness_query.empty()) {
		return;
	}
	auto result = use_transaction_connection ? transaction.Query(staleness_query)
	                                         : RunStalenessQuery(catalog, context, staleness_query);
	auto signature = ComputeStalenessSignature(*result);
	if (transaction.GetTransactionState() == PostgresTransactionState::TRANSACTION_STARTED) {
		// transaction still open - only valid on commit, since xmin reverts on rollback
		transaction.StageStalenessSignature(*this, std::move(signature));
		return;
	}
	staleness_signature = std::move(signature);
}

void PostgresCatalogSet::PromoteStalenessSignature(string signature) {
	// called from Commit(), possibly a different thread than the one that staged it
	lock_guard<mutex> lock(load_lock);
	staleness_signature = std::move(signature);
}

void PostgresCatalogSet::LoadEntriesLocked(ClientContext &context, PostgresTransaction &transaction) {
	loading_thread = ThreadUtil::GetThreadId();
	try {
		LoadEntries(context, transaction);
	} catch (...) {
		loading_thread = thread_id();
		throw;
	}
	RefreshStalenessSignature(transaction, /*use_transaction_connection=*/false);
	is_loaded = true;
	loading_thread = thread_id();
}

void PostgresCatalogSet::TryLoadEntries(ClientContext &context, PostgresTransaction &transaction) {
	if (HasInternalDependencies()) {
		if (is_loaded || loading_thread == ThreadUtil::GetThreadId()) {
			return;
		}
	}
	if (is_loaded) {
		auto staleness_query = GetStalenessQuery(context);
		if (staleness_query.empty()) {
			return;
		}
		// runs outside load_lock - avoids blocking other threads on this thread's network round-trip
		auto result = RunStalenessQuery(catalog, context, staleness_query);
		auto signature = ComputeStalenessSignature(*result);

		lock_guard<mutex> lock(load_lock);
		if (is_loaded) {
			if (signature == staleness_signature) {
				return;
			}
			ClearEntries();
		}
		// else: someone else cleared/reloaded while we were checking - fall through to the
		// reload below, still holding load_lock.
		LoadEntriesLocked(context, transaction);
		return;
	}

	lock_guard<mutex> lock(load_lock);
	if (is_loaded) {
		// someone else already loaded while we were waiting for the lock.
		return;
	}
	LoadEntriesLocked(context, transaction);
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
	if (!info.GetQualifiedName().Schema().empty() && info.type != CatalogType::SCHEMA_ENTRY) {
		drop_query += PostgresUtils::WriteIdentifier(info.GetQualifiedName().Schema().GetIdentifierName()) + ".";
	}
	drop_query += PostgresUtils::WriteIdentifier(info.GetQualifiedName().Name().GetIdentifierName());
	if (info.cascade) {
		drop_query += " CASCADE";
	}
	transaction.Query(drop_query);

	// erase the entry from the catalog set
	{
		lock_guard<mutex> l(entry_lock);
		entries.erase(info.GetQualifiedName().Name().GetIdentifierName());
	}
	RefreshStalenessSignature(transaction, /*use_transaction_connection=*/true);
}

void PostgresCatalogSet::Scan(ClientContext &context, PostgresTransaction &transaction,
                              const std::function<void(CatalogEntry &)> &callback) {
	TryLoadEntries(context, transaction);
	lock_guard<mutex> l(entry_lock);
	for (auto &entry : entries) {
		transaction.ReferenceEntry(entry.second);
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
