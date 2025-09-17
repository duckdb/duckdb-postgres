//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/postgres_transaction.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction.hpp"
#include "postgres_connection.hpp"
#include "storage/postgres_connection_pool.hpp"

namespace duckdb {
class PostgresCatalog;
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
	ClientContext &GetContext();

	string GetDSN();
	unique_ptr<PostgresResult> Query(const string &query);
	unique_ptr<PostgresResult> QueryWithoutTransaction(const string &query);
	vector<unique_ptr<PostgresResult>> ExecuteQueries(const string &queries);
	static PostgresTransaction &Get(ClientContext &context, Catalog &catalog);

	optional_ptr<CatalogEntry> ReferenceEntry(shared_ptr<CatalogEntry> &entry);

	string GetTemporarySchema();

private:
	PostgresPoolConnection connection;
	PostgresTransactionState transaction_state;
	AccessMode access_mode;
	PostgresIsolationLevel isolation_level;
	string temporary_schema;
	reference_map_t<CatalogEntry, shared_ptr<CatalogEntry>> referenced_entries;

private:
	//! Retrieves the connection **without** starting a transaction if none is active
	PostgresConnection &GetConnectionRaw();

	string GetBeginTransactionQuery();
};

} // namespace duckdb
