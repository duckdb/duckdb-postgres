//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/postgres_transaction_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "storage/postgres_catalog.hpp"
#include "storage/postgres_transaction.hpp"
#include "duckdb/common/reference_map.hpp"

namespace duckdb {

class PostgresTransactionManager : public TransactionManager {
public:
	PostgresTransactionManager(AttachedDatabase &db_p, PostgresCatalog &postgres_catalog);

	Transaction &StartTransaction(ClientContext &context) override;
	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
	void RollbackTransaction(Transaction &transaction) override;

	void Checkpoint(ClientContext &context, bool force = false) override;

private:
	PostgresCatalog &postgres_catalog;
	mutex transaction_lock;
	reference_map_t<Transaction, unique_ptr<PostgresTransaction>> transactions;
};

} // namespace duckdb
