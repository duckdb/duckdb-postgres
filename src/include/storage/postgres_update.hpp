//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/postgres_update.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/common/index_vector.hpp"

namespace duckdb {

class PostgresUpdate : public PhysicalOperator {
public:
	PostgresUpdate(PhysicalPlan &physical_plan, LogicalOperator &op, TableCatalogEntry &table,
	               vector<PhysicalIndex> columns, vector<unique_ptr<Expression>> expressions);

	//! The table to delete from
	TableCatalogEntry &table;
	//! The set of columns to update
	vector<PhysicalIndex> columns;
	//! Expressions to execute
	vector<unique_ptr<Expression>> expressions;
	//! Whether or not we can keep the copy alive during Sink calls
	bool keep_copy_alive = true;

public:
	// Source interface
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}

public:
	// Sink interface
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;

	bool IsSink() const override {
		return true;
	}

	bool ParallelSink() const override {
		return false;
	}

	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;
};

} // namespace duckdb
