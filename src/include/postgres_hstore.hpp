//===----------------------------------------------------------------------===//
//                         DuckDB
//
// postgres_hstore.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {

void RegisterHstoreFunctions(ExtensionLoader &loader);

} // namespace duckdb
