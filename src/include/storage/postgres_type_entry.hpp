//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/postgres_type_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "postgres_utils.hpp"

namespace duckdb {

class PostgresTypeEntry : public TypeCatalogEntry {
public:
	PostgresTypeEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTypeInfo &info, PostgresType postgres_type);

	PostgresType postgres_type;
};

} // namespace duckdb
