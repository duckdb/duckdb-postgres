#include "storage/postgres_transaction.hpp"
#include "storage/postgres_catalog.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "postgres_result.hpp"

namespace duckdb {

PostgresTransaction::PostgresTransaction(PostgresCatalog &postgres_catalog, TransactionManager &manager,
                                         ClientContext &context)
    : Transaction(manager, context), access_mode(postgres_catalog.access_mode),
      isolation_level(postgres_catalog.isolation_level) {
	connection = postgres_catalog.GetConnectionPool().GetConnection(&context);
}

PostgresTransaction::~PostgresTransaction() = default;

ClientContext &PostgresTransaction::GetContext() {
	return *context.lock();
}

void PostgresTransaction::Start() {
	transaction_state = PostgresTransactionState::TRANSACTION_NOT_YET_STARTED;
}
void PostgresTransaction::Commit() {
	if (transaction_state == PostgresTransactionState::TRANSACTION_STARTED) {
		transaction_state = PostgresTransactionState::TRANSACTION_FINISHED;
		GetConnectionRaw().Execute("COMMIT");
	}
}
void PostgresTransaction::Rollback() {
	if (transaction_state == PostgresTransactionState::TRANSACTION_STARTED) {
		transaction_state = PostgresTransactionState::TRANSACTION_FINISHED;
		GetConnectionRaw().Execute("ROLLBACK");
	}
}

string PostgresTransaction::GetBeginTransactionQuery() {
	string result = "BEGIN TRANSACTION ISOLATION LEVEL ";
	switch (isolation_level) {
	case PostgresIsolationLevel::READ_COMMITTED:
		result += "READ COMMITTED";
		break;
	case PostgresIsolationLevel::REPEATABLE_READ:
		result += "REPEATABLE READ";
		break;
	case PostgresIsolationLevel::SERIALIZABLE:
		result += "SERIALIZABLE";
		break;
	default:
		throw InternalException("Unsupported isolation level");
	}
	if (access_mode == AccessMode::READ_ONLY) {
		result += " READ ONLY";
	}
	return result;
}

PostgresConnection &PostgresTransaction::GetConnectionWithoutTransaction() {
	if (transaction_state == PostgresTransactionState::TRANSACTION_STARTED) {
		throw std::runtime_error("Execution without a Transaction is not possible if a Transaction already started");
	}
	if (access_mode == AccessMode::READ_ONLY) {
		throw std::runtime_error("Execution without a Transaction is not possible in Read Only Mode");
	}
	return connection.GetConnection();
}

PostgresConnection &PostgresTransaction::GetConnection() {
	auto &con = GetConnectionRaw();
	if (transaction_state == PostgresTransactionState::TRANSACTION_NOT_YET_STARTED) {
		transaction_state = PostgresTransactionState::TRANSACTION_STARTED;
		string query = GetBeginTransactionQuery();
		con.Execute(query);
	}
	return con;
}

PostgresConnection &PostgresTransaction::GetConnectionRaw() {
	return connection.GetConnection();
}

string PostgresTransaction::GetDSN() {
	return GetConnectionRaw().GetDSN();
}

unique_ptr<PostgresResult> PostgresTransaction::Query(const string &query) {
	auto &con = GetConnectionRaw();
	if (transaction_state == PostgresTransactionState::TRANSACTION_NOT_YET_STARTED) {
		transaction_state = PostgresTransactionState::TRANSACTION_STARTED;
		string transaction_start = GetBeginTransactionQuery();
		transaction_start += ";\n";
		return con.Query(transaction_start + query);
	}
	return con.Query(query);
}

unique_ptr<PostgresResult> PostgresTransaction::QueryWithoutTransaction(const string &query) {
	auto &con = GetConnectionRaw();
	if (transaction_state == PostgresTransactionState::TRANSACTION_STARTED) {
		throw std::runtime_error("Execution without a Transaction is not possible if a Transaction already started");
	}
	if (access_mode == AccessMode::READ_ONLY) {
		throw std::runtime_error("Execution without a Transaction is not possible in Read Only Mode");
	}
	return con.Query(query);
}

vector<unique_ptr<PostgresResult>> PostgresTransaction::ExecuteQueries(const string &queries) {
	auto &con = GetConnectionRaw();
	if (transaction_state == PostgresTransactionState::TRANSACTION_NOT_YET_STARTED) {
		transaction_state = PostgresTransactionState::TRANSACTION_STARTED;
		string transaction_start = GetBeginTransactionQuery();
		transaction_start += ";\n";
		return con.ExecuteQueries(transaction_start + queries);
	}
	return con.ExecuteQueries(queries);
}

optional_ptr<CatalogEntry> PostgresTransaction::ReferenceEntry(shared_ptr<CatalogEntry> &entry) {
	auto &ref = *entry;
	referenced_entries.emplace(ref, entry);
	return ref;
}

string PostgresTransaction::GetTemporarySchema() {
	if (temporary_schema.empty()) {
		auto result = Query("SELECT nspname FROM pg_namespace WHERE oid = pg_my_temp_schema();");
		if (result->Count() < 1) {
			// no temporary tables exist yet in this connection
			// create a random temporary table and return
			Query("CREATE TEMPORARY TABLE __internal_temporary_table(i INTEGER)");
			result = Query("SELECT nspname FROM pg_namespace WHERE oid = pg_my_temp_schema();");
			if (result->Count() < 1) {
				throw BinderException("Could not find temporary schema pg_temp_NNN for this connection");
			}
		}
		temporary_schema = result->GetString(0, 0);
	}
	return temporary_schema;
}

PostgresTransaction &PostgresTransaction::Get(ClientContext &context, Catalog &catalog) {
	return Transaction::Get(context, catalog).Cast<PostgresTransaction>();
}

} // namespace duckdb
