#include "storage/postgres_insert.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "storage/postgres_catalog.hpp"
#include "storage/postgres_transaction.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "storage/postgres_table_entry.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "postgres_connection.hpp"
#include "postgres_scanner.hpp"

namespace duckdb {

PostgresInsert::PostgresInsert(PhysicalPlan &physical_plan, PostgresCatalog &pg_catalog, ClientContext &context,
                               LogicalOperator &op, TableCatalogEntry &table,
                               physical_index_vector_t<idx_t> column_index_map_p)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, op.types, 1), table(&table), schema(nullptr),
      column_index_map(std::move(column_index_map_p)), use_plain_inserts(UsePlainInserts(pg_catalog, context)) {
}

PostgresInsert::PostgresInsert(PhysicalPlan &physical_plan, PostgresCatalog &pg_catalog, ClientContext &context,
                               LogicalOperator &op, SchemaCatalogEntry &schema, unique_ptr<BoundCreateTableInfo> info)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, op.types, 1), table(nullptr), schema(&schema),
      info(std::move(info)), use_plain_inserts(UsePlainInserts(pg_catalog, context)) {
}

//===--------------------------------------------------------------------===//
// States
//===--------------------------------------------------------------------===//
class PostgresInsertCopyGlobalState : public GlobalSinkState {
public:
	explicit PostgresInsertCopyGlobalState(ClientContext &context, PostgresTableEntry &table, PostgresCopyFormat format)
	    : table(table), insert_count(0), format(format) {
	}

	PostgresTableEntry &table;
	PostgresCopyState copy_state;
	DataChunk varchar_chunk;
	idx_t insert_count;
	PostgresCopyFormat format;
	vector<string> insert_column_names;
	bool copy_is_active = false;

	void FinishCopyTo(PostgresConnection &connection) {
		if (!copy_is_active) {
			return;
		}
		connection.FinishCopyTo(copy_state);
		copy_is_active = false;
	}
};

class PostgresInsertPlainGlobalState : public GlobalSinkState {
public:
	explicit PostgresInsertPlainGlobalState(ClientContext &context, PostgresTableEntry &table,
	                                        const vector<LogicalType> &varchar_types)
	    : table(table), insert_count(0) {
		varchar_chunk.Initialize(context, varchar_types);
	}

	PostgresTableEntry &table;
	DataChunk varchar_chunk;
	idx_t insert_count;
	string base_insert_query;
	string insert_values;
};

static vector<string> GetInsertColumns(const PostgresInsert &insert, PostgresTableEntry &entry) {
	vector<string> column_names;
	auto &columns = entry.GetColumns();
	idx_t column_count;
	if (!insert.column_index_map.empty()) {
		column_count = 0;
		vector<PhysicalIndex> column_indexes;
		column_indexes.resize(columns.LogicalColumnCount(), PhysicalIndex(DConstants::INVALID_INDEX));
		for (idx_t c = 0; c < insert.column_index_map.size(); c++) {
			auto column_index = PhysicalIndex(c);
			auto mapped_index = insert.column_index_map[column_index];
			if (mapped_index == DConstants::INVALID_INDEX) {
				// column not specified
				continue;
			}
			column_indexes[mapped_index] = column_index;
			column_count++;
		}
		for (idx_t c = 0; c < column_count; c++) {
			auto &col = columns.GetColumn(column_indexes[c]);
			column_names.push_back(col.GetName().GetIdentifierName());
		}
	}
	return column_names;
}

unique_ptr<GlobalSinkState> PostgresInsert::GetGlobalSinkStateCopy(ClientContext &context) const {
	optional_ptr<PostgresTableEntry> insert_table;
	if (!table) {
		auto &schema_ref = *schema.get_mutable();
		insert_table =
		    &schema_ref.CreateTable(schema_ref.GetCatalogTransaction(context), *info)->Cast<PostgresTableEntry>();
	} else {
		insert_table = &table.get_mutable()->Cast<PostgresTableEntry>();
	}
	auto &transaction = PostgresTransaction::Get(context, insert_table->catalog);
	auto &connection = transaction.GetConnection();
	auto insert_columns = GetInsertColumns(*this, *insert_table);
	auto format = insert_table->GetCopyFormat(context);
	auto result = make_uniq<PostgresInsertCopyGlobalState>(context, *insert_table, format);
	auto &insert_column_names = result->insert_column_names;
	if (!insert_columns.empty()) {
		for (auto &str : insert_columns) {
			Identifier col_identifier(str);
			auto index = insert_table->GetColumnIndex(col_identifier, true);
			if (!index.IsValid()) {
				insert_column_names.push_back(str);
			} else {
				insert_column_names.push_back(insert_table->postgres_names[index.index]);
			}
		}
	}
	return std::move(result);
}

static string GetBaseInsertQuery(const PostgresTableEntry &table, const vector<string> &column_names) {
	string query;
	query += "INSERT INTO ";
	query += PostgresUtils::WriteIdentifier(table.schema.name.GetIdentifierName());
	query += ".";
	query += PostgresUtils::WriteIdentifier(table.name.GetIdentifierName());
	query += " ";
	if (!column_names.empty()) {
		query += "(";
		for (idx_t c = 0; c < column_names.size(); c++) {
			if (c > 0) {
				query += ", ";
			}
			query += PostgresUtils::WriteIdentifier(column_names[c]);
		}
		query += ")";
	}
	query += " VALUES ";
	return query;
}

unique_ptr<GlobalSinkState> PostgresInsert::GetGlobalSinkStatePlain(ClientContext &context) const {
	PostgresTableEntry *insert_table;
	if (!table) {
		auto &schema_ref = *schema.get_mutable();
		insert_table =
		    &schema_ref.CreateTable(schema_ref.GetCatalogTransaction(context), *info)->Cast<PostgresTableEntry>();
	} else {
		insert_table = &table.get_mutable()->Cast<PostgresTableEntry>();
	}
	auto insert_columns = GetInsertColumns(*this, *insert_table);
	vector<LogicalType> insert_types;
	idx_t insert_column_count =
	    insert_columns.empty() ? insert_table->GetColumns().LogicalColumnCount() : insert_columns.size();
	for (idx_t c = 0; c < insert_column_count; c++) {
		insert_types.push_back(LogicalType::VARCHAR);
	}
	auto result = make_uniq<PostgresInsertPlainGlobalState>(context, *insert_table, insert_types);
	result->base_insert_query = GetBaseInsertQuery(*insert_table, insert_columns);
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
SinkResultType PostgresInsert::SinkCopy(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<PostgresInsertCopyGlobalState>();
	auto &transaction = PostgresTransaction::Get(context.client, gstate.table.catalog);
	auto &connection = transaction.GetConnection();
	if (!gstate.copy_is_active) {
		// copy hasn't started yet
		connection.BeginCopyTo(context.client, gstate.copy_state, gstate.format,
		                       gstate.table.schema.name.GetIdentifierName(), gstate.table.name.GetIdentifierName(),
		                       gstate.insert_column_names);
		gstate.copy_is_active = true;
	}
	connection.CopyChunk(context.client, gstate.copy_state, chunk, gstate.varchar_chunk);
	gstate.insert_count += chunk.size();
	if (!keep_copy_alive) {
		// if we are can't keep the copy alive we need to restart the copy during every sink
		gstate.FinishCopyTo(connection);
	}
	return SinkResultType::NEED_MORE_INPUT;
}

static void PostgresCastBlob(const Vector &input, Vector &result, idx_t count) {
	static constexpr const char *HEX_TABLE = "0123456789ABCDEF";
	auto input_data = FlatVector::GetData<string_t>(input);
	auto result_data = FlatVector::GetDataMutable<string_t>(result);
	for (idx_t r = 0; r < count; r++) {
		if (FlatVector::IsNull(input, r)) {
			FlatVector::SetNull(result, r, true);
			continue;
		}
		auto blob_data = const_data_ptr_cast(input_data[r].GetData());
		auto blob_size = input_data[r].GetSize();
		if (blob_size == 0) {
			result_data[r] = StringVector::AddString(result, "''");
			continue;
		}
		string result_blob = "'\\x";
		for (idx_t b = 0; b < blob_size; b++) {
			auto blob_entry = blob_data[b];
			auto byte_a = blob_entry >> 4;
			auto byte_b = blob_entry & 0x0F;
			result_blob += string(1, HEX_TABLE[byte_a]);
			result_blob += string(1, HEX_TABLE[byte_b]);
		}
		result_blob += "'::BYTEA";
		result_data[r] = StringVector::AddString(result, result_blob);
	}
}

SinkResultType PostgresInsert::SinkPlain(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	static constexpr const idx_t INSERT_FLUSH_SIZE = 8000;

	auto &gstate = input.global_state.Cast<PostgresInsertPlainGlobalState>();
	auto &transaction = PostgresTransaction::Get(context.client, gstate.table.catalog);
	auto &con = transaction.GetConnection();
	// cast to varchar
	D_ASSERT(chunk.ColumnCount() == gstate.varchar_chunk.ColumnCount());
	chunk.Flatten();
	gstate.varchar_chunk.Reset();
	for (idx_t c = 0; c < chunk.ColumnCount(); c++) {
		switch (chunk.data[c].GetType().id()) {
		case LogicalTypeId::BLOB:
			PostgresCastBlob(chunk.data[c], gstate.varchar_chunk.data[c], chunk.size());
			break;
		case LogicalTypeId::TIMESTAMP_TZ: {
			Vector timestamp_vector(LogicalType::TIMESTAMP);
			timestamp_vector.Reinterpret(chunk.data[c]);
			VectorOperations::Cast(context.client, timestamp_vector, gstate.varchar_chunk.data[c], chunk.size());
			break;
		}
		default:
			VectorOperations::Cast(context.client, chunk.data[c], gstate.varchar_chunk.data[c], chunk.size());
			break;
		}
	}
	gstate.varchar_chunk.SetChildCardinality(chunk.size());
	// for each column type check if we need to add quotes or not
	vector<bool> add_quotes;
	for (idx_t c = 0; c < chunk.ColumnCount(); c++) {
		bool add_quotes_for_type;
		switch (chunk.data[c].GetType().id()) {
		case LogicalTypeId::BOOLEAN:
		case LogicalTypeId::TINYINT:
		case LogicalTypeId::SMALLINT:
		case LogicalTypeId::INTEGER:
		case LogicalTypeId::BIGINT:
		case LogicalTypeId::UTINYINT:
		case LogicalTypeId::USMALLINT:
		case LogicalTypeId::UINTEGER:
		case LogicalTypeId::UBIGINT:
		case LogicalTypeId::FLOAT:
		case LogicalTypeId::DOUBLE:
		case LogicalTypeId::BLOB:
			add_quotes_for_type = false;
			break;
		case LogicalTypeId::ARRAY:
		case LogicalTypeId::LIST:
		case LogicalTypeId::STRUCT:
			throw InvalidInputException("Insertion of '%s' columns is not supported with compatibility mode enabled "
			                            "('pg_use_text_protocol' option)",
			                            chunk.data[c].GetType().ToString());
		default:
			add_quotes_for_type = true;
			break;
		}
		add_quotes.push_back(add_quotes_for_type);
	}

	// generate INSERT INTO statements
	for (idx_t r = 0; r < chunk.size(); r++) {
		if (!gstate.insert_values.empty()) {
			gstate.insert_values += ", ";
		}
		gstate.insert_values += "(";
		for (idx_t c = 0; c < gstate.varchar_chunk.ColumnCount(); c++) {
			if (c > 0) {
				gstate.insert_values += ", ";
			}
			if (FlatVector::IsNull(gstate.varchar_chunk.data[c], r)) {
				gstate.insert_values += "NULL";
			} else {
				auto data = FlatVector::GetDataMutable<string_t>(gstate.varchar_chunk.data[c]);
				if (add_quotes[c]) {
					gstate.insert_values += PostgresUtils::WriteLiteral(data[r].GetString());
				} else {
					gstate.insert_values += data[r].GetString();
				}
			}
		}
		gstate.insert_values += ")";
		if (gstate.insert_values.size() >= INSERT_FLUSH_SIZE) {
			// perform the actual insert
			con.Execute(context.client, gstate.base_insert_query + gstate.insert_values);
			// reset the to-be-inserted values
			gstate.insert_values = string();
		}
	}
	gstate.insert_count += chunk.size();
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType PostgresInsert::FinalizeCopy(Pipeline &pipeline, Event &event, ClientContext &context,
                                              OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<PostgresInsertCopyGlobalState>();
	auto &transaction = PostgresTransaction::Get(context, gstate.table.catalog);
	auto &connection = transaction.GetConnection();
	gstate.FinishCopyTo(connection);
	// update the approx_num_pages - approximately 8 bytes per column per row
	idx_t bytes_per_page = 8192;
	idx_t bytes_per_row = gstate.table.GetColumns().LogicalColumnCount() * 8;
	idx_t rows_per_page = MaxValue<idx_t>(1, bytes_per_page / bytes_per_row);
	gstate.table.approx_num_pages.fetch_add(gstate.insert_count / rows_per_page, std::memory_order_acq_rel);
	return SinkFinalizeType::READY;
}

SinkFinalizeType PostgresInsert::FinalizePlain(Pipeline &pipeline, Event &event, ClientContext &context,
                                               OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<PostgresInsertPlainGlobalState>();
	if (!gstate.insert_values.empty()) {
		// perform the final insert
		auto &transaction = PostgresTransaction::Get(context, gstate.table.catalog);
		auto &con = transaction.GetConnection();
		con.Execute(context, gstate.base_insert_query + gstate.insert_values);
	}
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// GetData
//===--------------------------------------------------------------------===//
SourceResultType PostgresInsert::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                 OperatorSourceInput &input) const {
	idx_t insert_count = 0;
	if (use_plain_inserts) {
		auto &insert_gstate = sink_state->Cast<PostgresInsertPlainGlobalState>();
		insert_count = insert_gstate.insert_count;
	} else {
		auto &insert_gstate = sink_state->Cast<PostgresInsertCopyGlobalState>();
		insert_count = insert_gstate.insert_count;
	}
	chunk.SetChildCardinality(1);
	chunk.data[0].SetValue(0, Value::BIGINT(insert_count));

	return SourceResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//
string PostgresInsert::GetName() const {
	return table ? "PG_INSERT" : "PG_CREATE_TABLE_AS";
}

InsertionOrderPreservingMap<string> PostgresInsert::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Table Name"] = table ? table->name.GetIdentifierName() : info->Base().GetTableName().GetIdentifierName();
	return result;
}

bool PostgresInsert::UsePlainInserts(PostgresCatalog &pg_catalog, ClientContext &context) {
	bool use_text_proto_user_option = false;
	Value value;
	if (context.TryGetCurrentSetting("pg_use_text_protocol", value) && !value.IsNull()) {
		use_text_proto_user_option = BooleanValue::Get(value);
	}
	return pg_catalog.UseTextProtocol(use_text_proto_user_option);
}

//===--------------------------------------------------------------------===//
// Plan
//===--------------------------------------------------------------------===//
PhysicalOperator &AddCastToPostgresTypes(ClientContext &context, PhysicalPlanGenerator &planner,
                                         PhysicalOperator &plan) {
	// check if we need to cast anything
	bool require_cast = false;
	auto &child_types = plan.GetTypes();
	for (auto &type : child_types) {
		auto postgres_type = PostgresUtils::ToPostgresType(type);
		if (postgres_type != type) {
			require_cast = true;
			break;
		}
	}
	if (!require_cast) {
		return plan;
	}

	vector<LogicalType> postgres_types;
	vector<unique_ptr<Expression>> select_list;
	for (idx_t i = 0; i < child_types.size(); i++) {
		auto &type = child_types[i];
		unique_ptr<Expression> expr;
		expr = make_uniq<BoundReferenceExpression>(type, i);

		auto postgres_type = PostgresUtils::ToPostgresType(type);
		if (postgres_type != type) {
			// add a cast
			expr = BoundCastExpression::AddCastToType(context, std::move(expr), postgres_type);
		}
		postgres_types.push_back(std::move(postgres_type));
		select_list.push_back(std::move(expr));
	}

	// we need to cast: add casts
	auto &proj =
	    planner.Make<PhysicalProjection>(std::move(postgres_types), std::move(select_list), plan.estimated_cardinality);
	proj.children.push_back(plan);
	return proj;
}

bool PostgresCatalog::IsPostgresScan(const string &name) {
	return name == "postgres_scan" || name == "postgres_scan_pushdown" || name == "postgres_query";
}

void PostgresCatalog::MaterializePostgresScans(PhysicalOperator &op) {
	if (op.type == PhysicalOperatorType::TABLE_SCAN) {
		auto &table_scan = op.Cast<PhysicalTableScan>();
		if (PostgresCatalog::IsPostgresScan(table_scan.function.name.GetIdentifierName())) {
			auto &bind_data = table_scan.bind_data->Cast<PostgresBindData>();
			bind_data.requires_materialization = true;
			bind_data.max_threads = 1;
			bind_data.can_use_main_thread = true;
			bind_data.emit_ctid = true;
		}
	}
	for (auto &child : op.children) {
		MaterializePostgresScans(child);
	}
}

PhysicalOperator &PostgresCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                              optional_ptr<PhysicalOperator> plan) {
	if (op.return_chunk) {
		throw BinderException("RETURNING clause not yet supported for insertion into Postgres table");
	}
	if (op.on_conflict_info.action_type != OnConflictAction::THROW) {
		throw BinderException("ON CONFLICT clause not yet supported for insertion into Postgres table");
	}

	D_ASSERT(plan);
	MaterializePostgresScans(*plan);
	auto &inner_plan = AddCastToPostgresTypes(context, planner, *plan);

	auto &insert = planner.Make<PostgresInsert>(*this, context, op, op.table, op.column_index_map);
	insert.children.push_back(inner_plan);
	return insert;
}

PhysicalOperator &PostgresCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                     LogicalCreateTable &op, PhysicalOperator &plan) {
	auto &inner_plan = AddCastToPostgresTypes(context, planner, plan);
	MaterializePostgresScans(inner_plan);

	auto &insert = planner.Make<PostgresInsert>(*this, context, op, op.schema, std::move(op.info));
	insert.children.push_back(inner_plan);
	return insert;
}

} // namespace duckdb
