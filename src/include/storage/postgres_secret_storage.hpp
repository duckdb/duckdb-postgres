//===----------------------------------------------------------------------===//
//                         DuckDB
//
// postgres_secret_storage.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/secret/secret_storage.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "storage/postgres_connection_pool.hpp"

namespace duckdb {

class PostgresCatalog;

class PostgresSecretStorage : public SecretStorage {
public:
	PostgresSecretStorage(const string &storage_name, string attached_database_name_p, string secrets_table_name_p);
	~PostgresSecretStorage() override;

	bool IncludeInLookups() override {
		return true;
	}

	static int64_t GetNextTieBreakOffset();

	// SecretStorage API
	unique_ptr<SecretEntry> StoreSecret(unique_ptr<const BaseSecret> secret, OnCreateConflict on_conflict,
	                                    optional_ptr<CatalogTransaction> transaction = nullptr) override;
	vector<SecretEntry> AllSecrets(optional_ptr<CatalogTransaction> transaction = nullptr) override;
	void DropSecretByName(const string &name, OnEntryNotFound on_entry_not_found,
	                      optional_ptr<CatalogTransaction> transaction = nullptr) override;
	SecretMatch LookupSecret(const string &path, const string &type,
	                         optional_ptr<CatalogTransaction> transaction = nullptr) override;
	unique_ptr<SecretEntry> GetSecretByName(const string &name,
	                                        optional_ptr<CatalogTransaction> transaction = nullptr) override;

	void InitializeSecretsTable(PostgresConnection &connection);

private:
	string SerializeSecret(const BaseSecret &secret);
	unique_ptr<const BaseSecret> DeserializeSecret(PostgresCatalog &postgres_catalog, const string &hex_string,
	                                               const string &secret_name);
	optional_ptr<PostgresCatalog> GetPostgresCatalog(ClientContext &context);

	string attached_database_name;
	string secrets_table_name;

	static std::atomic<int64_t> next_tie_break_offset;
};

} // namespace duckdb
