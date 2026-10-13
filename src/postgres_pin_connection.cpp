#include "postgres_scanner.hpp"

#include "storage/postgres_catalog.hpp"

namespace duckdb {

static void PostgresPinConnection(DataChunk &args, ExpressionState &state, Vector &result) {
	UnifiedVectorFormat catalog_name_data;
	args.data[0].ToUnifiedFormat(catalog_name_data);
	if (!catalog_name_data.validity.RowIsValid(0)) {
		throw InvalidInputException("Specified attached database name must be not null");
	}

	auto catalog_name_strt = UnifiedVectorFormat::GetData<string_t>(catalog_name_data)[0];
	string catalog_name(catalog_name_strt.GetData(), catalog_name_strt.GetSize());

	auto &catalog = PostgresUtils::GetCatalogByName(state.GetContext(), catalog_name);
	auto &pool = catalog.GetConnectionPool();

	auto conn = pool.ForceAcquire();
	uint64_t conn_id = pool.PinConnection(std::move(conn));

	auto result_data = FlatVector::GetDataMutable<uint64_t>(result);
	result_data[0] = conn_id;
}

PostgresPinConnectionFunction::PostgresPinConnectionFunction()
    : ScalarFunction("postgres_pin_connection", {LogicalType::VARCHAR}, LogicalType::UBIGINT, PostgresPinConnection) {
	SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	SetFallible();
	SetVolatile();
}

static void PostgresClosePinnedConnection(DataChunk &args, ExpressionState &state, Vector &result) {
	UnifiedVectorFormat catalog_name_data;
	args.data[0].ToUnifiedFormat(catalog_name_data);
	if (!catalog_name_data.validity.RowIsValid(0)) {
		throw InvalidInputException("Specified catalog name must be not null");
	}
	auto catalog_name_strt = UnifiedVectorFormat::GetData<string_t>(catalog_name_data)[0];
	string catalog_name(catalog_name_strt.GetData(), catalog_name_strt.GetSize());

	UnifiedVectorFormat conn_id_data;
	args.data[1].ToUnifiedFormat(conn_id_data);
	if (!conn_id_data.validity.RowIsValid(0)) {
		throw InvalidInputException("Specified connection ID must be not null");
	}
	auto conn_id = UnifiedVectorFormat::GetData<uint64_t>(conn_id_data)[0];

	auto &catalog = PostgresUtils::GetCatalogByName(state.GetContext(), catalog_name);
	{ auto conn = catalog.GetConnectionPool().UnpinConnection(conn_id); }

	auto &result_validity = FlatVector::ValidityMutable(result);
	result_validity.SetInvalid(0);
}

PostgresClosePinnedConnectionFunction::PostgresClosePinnedConnectionFunction()
    : ScalarFunction("postgres_close_pinned_connection", {LogicalType::VARCHAR, LogicalType::UBIGINT},
                     LogicalType::BOOLEAN, PostgresClosePinnedConnection) {
	SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	SetFallible();
	SetVolatile();
}
} // namespace duckdb
