//===----------------------------------------------------------------------===//
//                         DuckDB
//
// postgres_secret_storage.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/secret/secret_storage.hpp"
#include "storage/postgres_connection_pool.hpp"

namespace duckdb {

class PostgresCatalog;

class PostgresSecretStorage : public CatalogSetSecretStorage {
public:
  PostgresSecretStorage(const string &name_p, DatabaseInstance &db,
                        PostgresCatalog &postgres_catalog);
  ~PostgresSecretStorage() override;

  bool IncludeInLookups() override { return true; }

  static int64_t GetNextTieBreakOffset();

  static std::atomic<int64_t> next_unique_id;

protected:
  void WriteSecret(const BaseSecret &secret,
                   OnCreateConflict on_conflict) override;
  void RemoveSecret(const string &secret,
                    OnEntryNotFound on_entry_not_found) override;

private:
  void InitializeSecretsTable();
  string SerializeSecret(const BaseSecret &secret);

  PostgresCatalog &postgres_catalog;
  string secrets_table_name;

  static std::atomic<int64_t> next_tie_break_offset;
};

} // namespace duckdb
