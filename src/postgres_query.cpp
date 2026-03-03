#include "duckdb.hpp"

#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "postgres_parameters.hpp"
#include "postgres_scanner.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "storage/postgres_catalog.hpp"
#include "storage/postgres_transaction.hpp"

namespace duckdb {

static unique_ptr<FunctionData> PGQueryBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<PostgresBindData>(context);

	if (input.inputs[0].IsNull() || input.inputs[1].IsNull()) {
		throw BinderException("Parameters to postgres_query cannot be NULL");
	}

	// look up the database to query
	auto db_name = input.inputs[0].GetValue<string>();
	auto &db_manager = DatabaseManager::Get(context);
	auto db = db_manager.GetDatabase(context, db_name);
	if (!db) {
		throw BinderException("Failed to find attached database \"%s\" referenced in postgres_query", db_name);
	}
	auto &catalog = db->GetCatalog();
	if (catalog.GetCatalogType() != "postgres") {
		throw BinderException("Attached database \"%s\" does not refer to a Postgres database", db_name);
	}
	auto &pg_catalog = catalog.Cast<PostgresCatalog>();
	auto &transaction = Transaction::Get(context, catalog).Cast<PostgresTransaction>();
	auto sql = input.inputs[1].GetValue<string>();
	// strip any trailing semicolons
	StringUtil::RTrim(sql);
	while (!sql.empty() && sql.back() == ';') {
		sql = sql.substr(0, sql.size() - 1);
		StringUtil::RTrim(sql);
	}

	bool use_transaction = true;
	for (auto &kv : input.named_parameters) {
		if (kv.first == "use_transaction") {
			use_transaction = BooleanValue::Get(kv.second);
		}
	}

	vector<Value> param_values;
	auto params_it = input.named_parameters.find("params");
	if (params_it != input.named_parameters.end()) {
		Value &struct_val = params_it->second;
		if (struct_val.IsNull()) {
			throw BinderException("Parameters to postgres_query cannot be NULL");
		}
		if (struct_val.type().id() != LogicalTypeId::STRUCT) {
			throw BinderException("Query parameters must be specified in a STRUCT");
		}
		param_values = StructValue::GetChildren(struct_val);
	}

	auto &con = use_transaction ? transaction.GetConnection() : transaction.GetConnectionWithoutTransaction();

	auto conn = con.GetConn();
	// prepare execution of the query to figure out the result types and names
	auto prepared = PQprepare(conn, "", sql.c_str(), 0, nullptr);
	PostgresResult prepared_wrapper(prepared);
	if (!prepared) {
		throw BinderException("Failed to prepare query \"%s\" (no result returned): %s", sql, PQerrorMessage(conn));
	}
	if (PQresultStatus(prepared) != PGRES_COMMAND_OK) {
		throw BinderException("Failed to prepare query \"%s\": %s", sql, PQresultErrorMessage(prepared));
	}
	// use describe_prepared
	auto describe_prepared = PQdescribePrepared(conn, "");
	PostgresResult describe_wrapper(describe_prepared);
	if (!describe_prepared || PQresultStatus(describe_prepared) != PGRES_COMMAND_OK) {
		auto extended_err = describe_prepared ? PQresultErrorMessage(describe_prepared) : PQerrorMessage(conn);
		throw BinderException("Failed to describe prepared statement: %s", extended_err);
	}
	int nfields = PQnfields(describe_prepared);
	if (nfields <= 0) {
		throw BinderException("No fields returned by query \"%s\" - the query must be a SELECT statement that returns "
		                      "at least one column",
		                      sql);
	}
	for (idx_t c = 0; c < nfields; c++) {
		PostgresType postgres_type;
		postgres_type.oid = PQftype(describe_prepared, c);
		PostgresTypeData type_data;
		type_data.type_name = PostgresUtils::PostgresOidToName(postgres_type.oid);
		type_data.type_modifier = PQfmod(describe_prepared, c);
		auto converted_type = PostgresUtils::TypeToLogicalType(nullptr, nullptr, type_data, postgres_type);
		result->postgres_types.push_back(postgres_type);
		return_types.emplace_back(converted_type);
		names.emplace_back(PQfname(describe_prepared, c));
	}
	int nparams = PQnparams(describe_prepared);
	if (nparams != param_values.size()) {
		throw BinderException("Incorrect number of parameters specified, expected: %d, actual: %zu, query: \"%s\"",
		                      nparams, param_values.size(), sql);
	}
	vector<Oid> param_types;
	for (idx_t p = 0; p < nparams; p++) {
		Oid ptype = PQparamtype(describe_prepared, p);
		param_types.emplace_back(ptype);
	}

	// set up the bind data
	result->SetCatalog(pg_catalog);
	result->dsn = con.GetDSN();
	result->types = return_types;
	result->names = names;
	result->read_only = false;
	result->SetTablePages(0);
	result->sql = std::move(sql);
	result->params = PostgresParameters(std::move(param_types), std::move(param_values));
	result->use_transaction = use_transaction;
	return std::move(result);
}

PostgresQueryFunction::PostgresQueryFunction()
    : TableFunction("postgres_query", {LogicalType::VARCHAR, LogicalType::VARCHAR}, nullptr, PGQueryBind) {
	named_parameters["use_transaction"] = LogicalType::BOOLEAN;
	named_parameters["params"] = LogicalType::ANY;
	PostgresScanFunction scan_function;
	init_global = scan_function.init_global;
	init_local = scan_function.init_local;
	function = scan_function.function;
	projection_pushdown = true;
	global_initialization = TableFunctionInitialization::INITIALIZE_ON_SCHEDULE;
}
} // namespace duckdb
