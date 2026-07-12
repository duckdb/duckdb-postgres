//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/postgres_transaction.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "postgres_connection.hpp"
#include "storage/postgres_connection_pool.hpp"

namespace duckdb {
class PostgresCatalog;
class PostgresCatalogSet;
class PostgresSchemaEntry;
class PostgresTableEntry;

enum class PostgresTransactionState { TRANSACTION_NOT_YET_STARTED, TRANSACTION_STARTED, TRANSACTION_FINISHED };

class PostgresTransaction : public Transaction {
public:
	PostgresTransaction(PostgresCatalog &postgres_catalog, TransactionManager &manager, ClientContext &context);
	~PostgresTransaction() override;

	void Start();
	void Commit();
	void Rollback();

	PostgresConnection &GetConnectionWithoutTransaction();
	PostgresConnection &GetConnection();
	optional_ptr<ClientContext> GetContext();

	string GetDSN();
	unique_ptr<PostgresResult> Query(const string &query);
	unique_ptr<PostgresResult> QueryWithoutTransaction(const string &query);
	vector<unique_ptr<PostgresResult>> ExecuteQueries(ClientContext &context, const string &queries);
	static PostgresTransaction &Get(ClientContext &context, Catalog &catalog);
	static string GetBeginTransactionQuery(PostgresIsolationLevel isolation_level, AccessMode access_mode);

	optional_ptr<CatalogEntry> ReferenceEntry(shared_ptr<CatalogEntry> &entry);

	string GetTemporarySchema();

	PostgresTransactionState GetTransactionState() const {
		return transaction_state;
	}
	void StageStalenessSignature(PostgresCatalogSet &catalog_set, string signature);

private:
	PostgresPoolConnection connection;
	PostgresTransactionState transaction_state;
	AccessMode access_mode;
	PostgresIsolationLevel isolation_level;
	string temporary_schema;
	mutex referenced_entries_lock;
	reference_map_t<CatalogEntry, shared_ptr<CatalogEntry>> referenced_entries;
	mutex pending_signatures_lock;
	vector<pair<reference<PostgresCatalogSet>, string>> pending_signatures;

private:
	//! Retrieves the connection **without** starting a transaction if none is active
	PostgresConnection &GetConnectionRaw();

	string GetBeginTransactionQuery();
};

} // namespace duckdb
