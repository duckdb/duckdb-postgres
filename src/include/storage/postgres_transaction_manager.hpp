//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/postgres_transaction_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "dbconnector/storage/transaction_manager.hpp"

#include "storage/postgres_catalog.hpp"
#include "storage/postgres_transaction.hpp"

namespace duckdb {

using PostgresTransactionManager = dbconnector::storage::TransactionManager<PostgresCatalog, PostgresTransaction>;

} // namespace duckdb
