#include "storage/postgres_catalog.hpp"
#include "storage/postgres_index.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/planner/operator/logical_create_index.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/planner/expression_binder/index_binder.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

PostgresCreateIndex::PostgresCreateIndex(PhysicalPlan &physical_plan, unique_ptr<CreateIndexInfo> info,
                                         TableCatalogEntry &table)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, {LogicalType::BIGINT}, 1), info(std::move(info)),
      table(table) {
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
SourceResultType PostgresCreateIndex::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                              OperatorSourceInput &input) const {
	auto &catalog = table.catalog;
	auto &schema = table.schema;
	auto transaction = catalog.GetCatalogTransaction(context.client);
	auto existing = schema.GetEntry(transaction, CatalogType::INDEX_ENTRY, info->index_name);
	if (existing) {
		switch (info->on_conflict) {
		case OnCreateConflict::IGNORE_ON_CONFLICT:
			return SourceResultType::FINISHED;
		case OnCreateConflict::ERROR_ON_CONFLICT:
			throw BinderException("Index with name \"%s\" already exists in schema \"%s\"", info->index_name,
			                      table.schema.name);
		case OnCreateConflict::REPLACE_ON_CONFLICT: {
			DropInfo drop_info;
			drop_info.type = CatalogType::INDEX_ENTRY;
			drop_info.schema = info->schema;
			drop_info.name = info->index_name;
			schema.DropEntry(context.client, drop_info);
			break;
		}
		default:
			throw InternalException("Unsupported on create conflict");
		}
	}
	schema.CreateIndex(transaction, *info, table);

	return SourceResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Logical Operator
//===--------------------------------------------------------------------===//
class LogicalPostgresCreateIndex : public LogicalExtensionOperator {
public:
	LogicalPostgresCreateIndex(unique_ptr<CreateIndexInfo> info_p, TableCatalogEntry &table)
	    : info(std::move(info_p)), table(table) {
	}

	unique_ptr<CreateIndexInfo> info;
	TableCatalogEntry &table;

	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override {
		return planner.Make<PostgresCreateIndex>(std::move(info), table);
	}

	void Serialize(Serializer &serializer) const override {
		throw NotImplementedException("Cannot serialize Postgres Create index");
	}

	void ResolveTypes() override {
		types = {LogicalType::BIGINT};
	}
};

unique_ptr<LogicalOperator> PostgresCatalog::BindCreateIndex(Binder &binder, CreateStatement &stmt,
                                                             TableCatalogEntry &table,
                                                             unique_ptr<LogicalOperator> plan) {
	// FIXME: this is a work-around for the CreateIndexInfo we are getting here not being fully bound
	// this needs to be fixed upstream (eventually)
	auto create_index_info = unique_ptr_cast<CreateInfo, CreateIndexInfo>(std::move(stmt.info));
	IndexBinder index_binder(binder, binder.context);

	// Bind the index expressions.
	vector<unique_ptr<Expression>> expressions;
	for (auto &expr : create_index_info->expressions) {
		expressions.push_back(index_binder.Bind(expr));
	}

	auto &get = plan->Cast<LogicalGet>();
	index_binder.InitCreateIndexInfo(get, *create_index_info, table.schema.name);

	return make_uniq<LogicalPostgresCreateIndex>(std::move(create_index_info), table);
}

} // namespace duckdb
