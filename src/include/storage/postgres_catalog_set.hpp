//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/postgres_catalog_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/shared_ptr.hpp"

namespace duckdb {
struct DropInfo;
class PostgresResult;
class PostgresSchemaEntry;
class PostgresTransaction;

class PostgresCatalogSet {
public:
	PostgresCatalogSet(Catalog &catalog, bool is_loaded);

	optional_ptr<CatalogEntry> GetEntry(PostgresTransaction &transaction, const string &name);
	void DropEntry(PostgresTransaction &transaction, DropInfo &info);
	void Scan(PostgresTransaction &transaction, const std::function<void(CatalogEntry &)> &callback);
	virtual optional_ptr<CatalogEntry> CreateEntry(PostgresTransaction &transaction, shared_ptr<CatalogEntry> entry);
	void ClearEntries();
	virtual bool SupportReload() const {
		return false;
	}
	virtual optional_ptr<CatalogEntry> ReloadEntry(PostgresTransaction &transaction, const string &name);

protected:
	virtual void LoadEntries(PostgresTransaction &transaction) = 0;
	//! Whether or not the catalog set contains dependencies to itself that have
	//! to be resolved WHILE loading
	virtual bool HasInternalDependencies() const {
		return false;
	}
	void TryLoadEntries(PostgresTransaction &transaction);

protected:
	Catalog &catalog;

private:
	mutex entry_lock;
	mutex load_lock;
	unordered_map<string, shared_ptr<CatalogEntry>> entries;
	case_insensitive_map_t<string> entry_map;
	atomic<bool> is_loaded;
};

class PostgresInSchemaSet : public PostgresCatalogSet {
public:
	PostgresInSchemaSet(PostgresSchemaEntry &schema, bool is_loaded);

	optional_ptr<CatalogEntry> CreateEntry(PostgresTransaction &transaction, shared_ptr<CatalogEntry> entry) override;

protected:
	PostgresSchemaEntry &schema;
};

struct PostgresResultSlice {
	PostgresResultSlice(shared_ptr<PostgresResult> result_p, idx_t start, idx_t end)
	    : result(std::move(result_p)), start(start), end(end) {
	}

	PostgresResult &GetResult() {
		return *result;
	}

	shared_ptr<PostgresResult> result;
	idx_t start;
	idx_t end;
};

} // namespace duckdb
