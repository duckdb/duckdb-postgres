#include "duckdb.hpp"

#include "dbconnector/defer.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "postgres_parameters.hpp"
#include "postgres_prepared_statement.hpp"
#include "postgres_scanner.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "storage/postgres_catalog.hpp"
#include "storage/postgres_transaction.hpp"

namespace duckdb {

static bool ExtractFlag(TableFunctionBindInput &input, const string &name, bool default_val) {
	auto it = input.named_parameters.find(Identifier(name));
	if (it != input.named_parameters.end()) {
		Value &bool_val = it->second;
		if (!bool_val.IsNull()) {
			return BooleanValue::Get(bool_val);
		}
	}
	return default_val;
}

static unique_ptr<FunctionData> BindDML(ClientContext &context, TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types, vector<Identifier> &names,
                                        PostgresCatalog &catalog, PostgresConnection &con, std::string sql,
                                        bool use_transaction, uint64_t pinned_connection_id,
                                        unique_ptr<PostgresPreparedStatement> prepared_statement) {
	// The statement returns no result columns: it's a command (DDL, or DML without RETURNING).
	// Instead of failing, run it as a command and return a single-row Success result. We reuse
	// the prepare/describe just done — no extra round-trip — and defer execution to
	// InitGlobalState (execution time, not bind, so EXPLAIN does not run it).
	auto result = make_uniq<PostgresBindData>(context, catalog.GetName());
	result->command_only = true;
	result->pinned_connection_id = pinned_connection_id;
	if (ExtractFlag(input, "suppress_dml_output", false)) {
		// This invocation wraps a command with no result set (DDL, or DML without RETURNING). Tell the
		// binder via the return-type modifier so that when this is routed through CONNECT the outer
		// statement is reported as NOTHING and displays like a native command (no spurious result table).
		input.table_function.call_return_type = StatementReturnType::NOTHING;
	}
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back(Identifier("rowcount"));
	result->dsn = con.GetDSN();
	result->types = return_types;
	result->names.emplace_back(names[0].GetIdentifierName());
	result->read_only = false;
	result->sql = std::move(sql);
	result->use_transaction = use_transaction;
	result->prepared_statement = std::move(prepared_statement);
	PostgresScanFunction::PrepareBind(catalog.GetPostgresVersion(), context, *result, 0, catalog);
	return std::move(result);
}

static PostgresQueryMode ResolveQueryMode(ClientContext &context, TableFunctionBindInput &input,
                                          const vector<Value> &params) {
	auto it = input.named_parameters.find(Identifier("query_mode"));
	if (it != input.named_parameters.end()) {
		Value &str_val = it->second;
		if (!str_val.IsNull()) {
			if (str_val.type().id() != LogicalTypeId::VARCHAR) {
				throw BinderException("Invalid non-VARCHAR 'query_mode' parameter specified");
			}
			PostgresQueryMode mode = PostgresUtils::ExtractQueryMode(str_val);
			if (mode != PostgresQueryMode::AUTO) {
				return mode;
			}
		}
	}
	PostgresQueryMode mode = PostgresUtils::ExtractQueryMode(context);
	if (mode != PostgresQueryMode::AUTO) {
		return mode;
	}
	Value text_proto_val;
	if (context.TryGetCurrentSetting("pg_use_text_protocol", text_proto_val) && !text_proto_val.IsNull() &&
	    BooleanValue::Get(text_proto_val)) {
		return PostgresQueryMode::PLAIN_QUERY_TEXT;
	}
	if (params.size() > 0) {
		return PostgresQueryMode::PLAIN_QUERY_BINARY;
	}
	return PostgresQueryMode::AUTO;
}

static string StripTrailingSemicolon(string sql) {
	StringUtil::RTrim(sql);
	while (!sql.empty() && sql.back() == ';') {
		sql = sql.substr(0, sql.size() - 1);
		StringUtil::RTrim(sql);
	}
	return sql;
}

static vector<Value> ExtractParams(TableFunctionBindInput &input) {
	auto params_it = input.named_parameters.find("params");
	if (params_it != input.named_parameters.end()) {
		Value &struct_val = params_it->second;
		if (struct_val.IsNull()) {
			throw BinderException("Parameters to postgres_query cannot be NULL");
		}
		if (struct_val.type().id() != LogicalTypeId::STRUCT && struct_val.type().id() != LogicalTypeId::TUPLE) {
			throw BinderException("Query parameters must be specified in a STRUCT");
		}
		return StructValue::GetChildren(struct_val);
	}
	return vector<Value>();
}

static uint64_t ExtractPinnedConnId(TableFunctionBindInput &input) {
	uint64_t pinned_connection_id = 0;
	auto conn_it = input.named_parameters.find("connection");
	if (conn_it != input.named_parameters.end()) {
		Value &conn_val = conn_it->second;
		if (conn_val.IsNull()) {
			throw BinderException("Specified connection must be not null");
		}
		pinned_connection_id = UBigIntValue::Get(conn_val);
	}
	return pinned_connection_id;
}

static unique_ptr<FunctionData> PGQueryBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<Identifier> &names) {
	if (input.inputs[0].IsNull() || input.inputs[1].IsNull()) {
		throw BinderException("Parameters to postgres_query cannot be NULL");
	}

	auto db_name = input.inputs[0].GetValue<string>();
	auto &catalog = PostgresUtils::GetCatalogByName(context, db_name);
	auto input_sql = input.inputs[1].GetValue<string>();
	auto sql = StripTrailingSemicolon(std::move(input_sql));
	bool use_transaction = ExtractFlag(input, "use_transaction", true);
	vector<Value> param_values = ExtractParams(input);
	uint64_t pinned_connection_id = ExtractPinnedConnId(input);

	optional_ptr<PostgresConnection> con_ptr = nullptr;
	PostgresPoolConnection pinned_connection;
	uint64_t pool_connection_id = 0;

	if (pinned_connection_id > 0) {
		pinned_connection = catalog.GetConnectionPool().UnpinConnection(pinned_connection_id);
		PostgresConnection &conn = pinned_connection.GetConnection();
		con_ptr = &conn;
		pool_connection_id = pinned_connection.Id();
	} else {
		auto &transaction = Transaction::Get(context, catalog).Cast<PostgresTransaction>();
		if (use_transaction) {
			con_ptr = &transaction.GetConnection();
		} else {
			con_ptr = &transaction.GetConnectionWithoutTransaction();
		}
		pool_connection_id = transaction.GetPoolConnectionId();
	}

	auto deferred_pin = dbconnector::Defer([&pinned_connection, &catalog] {
		if (!pinned_connection) {
			return;
		}
		try {
			catalog.GetConnectionPool().PinConnection(std::move(pinned_connection));
		} catch (...) {
			// suppress
		}
	});

	PostgresConnection &con = *con_ptr;

	if (!ExtractFlag(input, "prepare", true)) {
		if (param_values.size() > 0) {
			throw BinderException("query parameters cannot be used with 'prepare=FALSE'");
		}
		return BindDML(context, input, return_types, names, catalog, con, std::move(sql), use_transaction,
		               pinned_connection_id, nullptr);
	}

	auto conn_lock = con.Lock();
	auto conn = con.GetConn();
	// prepare execution of the query to figure out the result types and names
	string stmt_name = PostgresPreparedStatement::GenerateName();
	PGresult *prepared_res_ptr = PQprepare(conn, stmt_name.c_str(), sql.c_str(), 0, nullptr);
	auto prepared = make_uniq<PostgresPreparedStatement>(conn, pool_connection_id, stmt_name, sql, prepared_res_ptr);
	// use describe_prepared
	auto describe_prepared = PQdescribePrepared(conn, prepared->GetName().c_str());
	PostgresResult describe_wrapper(describe_prepared);
	if (!describe_prepared || PQresultStatus(describe_prepared) != PGRES_COMMAND_OK) {
		auto extended_err = describe_prepared ? PQresultErrorMessage(describe_prepared) : PQerrorMessage(conn);
		throw BinderException("Failed to describe prepared statement: %s", extended_err);
	}
	conn_lock.unlock();
	int nfields = PQnfields(describe_prepared);
	if (nfields <= 0) {
		return BindDML(context, input, return_types, names, catalog, con, std::move(sql), use_transaction,
		               pinned_connection_id, std::move(prepared));
	}
	auto result = make_uniq<PostgresBindData>(context, catalog.GetName());
	result->pinned_connection_id = pinned_connection_id;
	auto type_config = PostgresTypeConfig::FromContext(context);
	for (idx_t c = 0; c < nfields; c++) {
		PostgresType postgres_type;
		postgres_type.oid = PQftype(describe_prepared, c);
		PostgresTypeData type_data;
		type_data.type_name = PostgresUtils::PostgresOidToName(postgres_type.oid);
		type_data.type_modifier = PQfmod(describe_prepared, c);
		auto converted_type = PostgresUtils::TypeToLogicalType(nullptr, nullptr, type_config, type_data, postgres_type);
		result->postgres_types.push_back(postgres_type);
		return_types.emplace_back(converted_type);
		char *name_ptr = PQfname(describe_prepared, c);
		string name = name_ptr ? name_ptr : "unnamed";
		names.emplace_back(name);
	}
	PostgresUtils::DeduplicateColumns(names);
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
	result->type_config = type_config;
	result->dsn = con.GetDSN();
	result->types = return_types;
	for (auto &nm : names) {
		result->names.emplace_back(nm.GetIdentifierName());
	}
	result->read_only = false;
	result->sql = std::move(sql);
	result->query_mode = ResolveQueryMode(context, input, param_values);
	result->params = PostgresParameters(std::move(param_types), std::move(param_values));
	result->use_transaction = use_transaction;
	result->prepared_statement = std::move(prepared);
	PostgresScanFunction::PrepareBind(catalog.GetPostgresVersion(), context, *result, 0, catalog);
	return std::move(result);
}

static FunctionSignature PostgresQuerySignature(bool suppress_dml_output) {
	FunctionSignature signature;
	signature.AddParameter("database", LogicalType::VARCHAR)
	    .AddParameter("sql", LogicalType::VARCHAR)
	    .WithTypedKwargs("options", [&](TypedKwargs &options) {
		    options.Add("use_transaction", LogicalType::BOOLEAN).Add("params", LogicalType::ANY);
		    if (suppress_dml_output) {
			    options.Add("suppress_dml_output", LogicalType::BOOLEAN);
		    }
		    options.Add("prepare", LogicalType::BOOLEAN);
		    options.Add("query_mode", LogicalType::VARCHAR);
		    options.Add("connection", LogicalType::UBIGINT);
	    });
	return signature;
}

PostgresQueryFunction::PostgresQueryFunction()
    : TableFunction("postgres_query", PostgresQuerySignature(true), nullptr, PGQueryBind) {
	PostgresScanFunction scan_function;
	init_global = scan_function.init_global;
	init_local = scan_function.init_local;
	function = scan_function.function;
	projection_pushdown = true;
	global_initialization = TableFunctionInitialization::INITIALIZE_ON_SCHEDULE;
}

PostgresExecuteFunction::PostgresExecuteFunction()
    : TableFunction("postgres_execute", PostgresQuerySignature(false), nullptr, PGQueryBind) {
	PostgresScanFunction scan_function;
	init_global = scan_function.init_global;
	init_local = scan_function.init_local;
	function = scan_function.function;
	projection_pushdown = true;
	global_initialization = TableFunctionInitialization::INITIALIZE_ON_SCHEDULE;
}
} // namespace duckdb
