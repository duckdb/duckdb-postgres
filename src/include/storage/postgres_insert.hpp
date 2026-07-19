//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/postgres_insert.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/common/index_vector.hpp"
#include "storage/postgres_catalog.hpp"

namespace duckdb {

class PostgresInsert : public PhysicalOperator {
public:
	//! INSERT INTO
	PostgresInsert(PhysicalPlan &physical_plan, PostgresCatalog &pg_catalog, ClientContext &context,
	               LogicalOperator &op, TableCatalogEntry &table, physical_index_vector_t<idx_t> column_index_map);
	//! CREATE TABLE AS
	PostgresInsert(PhysicalPlan &physical_plan, PostgresCatalog &pg_catalog, ClientContext &context,
	               LogicalOperator &op, SchemaCatalogEntry &schema, unique_ptr<BoundCreateTableInfo> info);

	//! The table to insert into
	optional_ptr<TableCatalogEntry> table;
	//! Table schema, in case of CREATE TABLE AS
	optional_ptr<SchemaCatalogEntry> schema;
	//! Create table info, in case of CREATE TABLE AS
	unique_ptr<BoundCreateTableInfo> info;
	//! column_index_map
	physical_index_vector_t<idx_t> column_index_map;
	//! Whether or not we can keep the copy alive during Sink calls
	bool keep_copy_alive = true;
	//! Whether to use plain INSERTs instead of COPY
	bool use_plain_inserts = false;

public:
	// Source interface
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}

public:
	// Sink interface
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override {
		if (use_plain_inserts) {
			return GetGlobalSinkStatePlain(context);
		} else {
			return GetGlobalSinkStateCopy(context);
		}
	}

	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override {
		if (use_plain_inserts) {
			return SinkPlain(context, chunk, input);
		} else {
			return SinkCopy(context, chunk, input);
		}
	}

	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override {
		if (use_plain_inserts) {
			return FinalizePlain(pipeline, event, context, input);
		} else {
			return FinalizeCopy(pipeline, event, context, input);
		}
	}

	bool IsSink() const override {
		return true;
	}

	bool ParallelSink() const override {
		return false;
	}

	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;

private:
	static bool UsePlainInserts(PostgresCatalog &pg_catalog, ClientContext &context);
	unique_ptr<GlobalSinkState> GetGlobalSinkStateCopy(ClientContext &context) const;
	unique_ptr<GlobalSinkState> GetGlobalSinkStatePlain(ClientContext &context) const;
	SinkResultType SinkCopy(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const;
	SinkResultType SinkPlain(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const;
	SinkFinalizeType FinalizeCopy(Pipeline &pipeline, Event &event, ClientContext &context,
	                              OperatorSinkFinalizeInput &input) const;
	SinkFinalizeType FinalizePlain(Pipeline &pipeline, Event &event, ClientContext &context,
	                               OperatorSinkFinalizeInput &input) const;
};

} // namespace duckdb
