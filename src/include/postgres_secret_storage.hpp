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
  PostgresSecretStorage(const string &name_p, DatabaseInstance &db,
                        PostgresCatalog &postgres_catalog, SecretManager &secret_manager);
  ~PostgresSecretStorage() override;

  bool IncludeInLookups() override { return true; }

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

private:
  void InitializeSecretsTable();
  string SerializeSecret(const BaseSecret &secret);
  unique_ptr<const BaseSecret> DeserializeSecret(const string &hex_string, const string &secret_name);

  DatabaseInstance &db;
  SecretManager &secret_manager;
  PostgresCatalog &postgres_catalog;
  string secrets_table_name;

  static std::atomic<int64_t> next_tie_break_offset;
};

} // namespace duckdb
