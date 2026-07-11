//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/postgres_optimizer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/config.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"

namespace duckdb {

class PostgresOptimizer {
public:
	static void Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);
};

} // namespace duckdb
