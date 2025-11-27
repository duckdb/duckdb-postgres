#define DUCKDB_BUILD_LOADABLE_EXTENSION
#include "duckdb.hpp"

#include "postgres_scanner.hpp"
#include "postgres_storage.hpp"
#include "postgres_scanner_extension.hpp"
#include "postgres_binary_copy.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "storage/postgres_catalog.hpp"
#include "storage/postgres_optimizer.hpp"
#include "duckdb/planner/extension_callback.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/main/connection_manager.hpp"
#include "duckdb/common/error_data.hpp"

using namespace duckdb;

class PostgresExtensionState : public ClientContextState {
public:
	bool CanRequestRebind() override {
		return true;
	}
	RebindQueryInfo OnPlanningError(ClientContext &context, SQLStatement &statement, ErrorData &error) override {
		if (error.Type() != ExceptionType::BINDER) {
			return RebindQueryInfo::DO_NOT_REBIND;
		}
		auto &extra_info = error.ExtraInfo();
		auto entry = extra_info.find("error_subtype");
		if (entry == extra_info.end()) {
			return RebindQueryInfo::DO_NOT_REBIND;
		}
		if (entry->second != "COLUMN_NOT_FOUND") {
			return RebindQueryInfo::DO_NOT_REBIND;
		}
		// clear caches and rebind
		PostgresClearCacheFunction::ClearPostgresCaches(context);
		return RebindQueryInfo::ATTEMPT_TO_REBIND;
	}
};

class PostgresExtensionCallback : public ExtensionCallback {
public:
	void OnConnectionOpened(ClientContext &context) override {
		context.registered_state->Insert("postgres_extension", make_shared_ptr<PostgresExtensionState>());
	}
};

static void SetPostgresConnectionLimit(ClientContext &context, SetScope scope, Value &parameter) {
	if (scope == SetScope::LOCAL) {
		throw InvalidInputException("pg_connection_limit can only be set globally");
	}
	auto databases = DatabaseManager::Get(context).GetDatabases(context);
	for (auto &db_ref : databases) {
		auto &db = *db_ref;
		auto &catalog = db.GetCatalog();
		if (catalog.GetCatalogType() != "postgres") {
			continue;
		}
		catalog.Cast<PostgresCatalog>().GetConnectionPool().SetMaximumConnections(UBigIntValue::Get(parameter));
	}
	auto &config = DBConfig::GetConfig(context);
	config.SetOption("pg_connection_limit", parameter);
}

static void SetPostgresDebugQueryPrint(ClientContext &context, SetScope scope, Value &parameter) {
	PostgresConnection::DebugSetPrintQueries(BooleanValue::Get(parameter));
}

unique_ptr<BaseSecret> CreatePostgresSecretFunction(ClientContext &context, CreateSecretInput &input) {
	// apply any overridden settings
	vector<string> prefix_paths;
	auto result = make_uniq<KeyValueSecret>(prefix_paths, "postgres", "config", input.name);
	for (const auto &named_param : input.options) {
		auto lower_name = StringUtil::Lower(named_param.first);

		if (lower_name == "host") {
			result->secret_map["host"] = named_param.second.ToString();
		} else if (lower_name == "user") {
			result->secret_map["user"] = named_param.second.ToString();
		} else if (lower_name == "database") {
			result->secret_map["dbname"] = named_param.second.ToString();
		} else if (lower_name == "dbname") {
			result->secret_map["dbname"] = named_param.second.ToString();
		} else if (lower_name == "password") {
			result->secret_map["password"] = named_param.second.ToString();
		} else if (lower_name == "port") {
			result->secret_map["port"] = named_param.second.ToString();
		} else if (lower_name == "passfile") {
			result->secret_map["passfile"] = named_param.second.ToString();
		} else if (lower_name == "use_rds_iam_auth") {
			result->secret_map["use_rds_iam_auth"] = named_param.second.ToString();
		} else if (lower_name == "aws_region") {
			result->secret_map["aws_region"] = named_param.second.ToString();
		} else {
			throw InternalException("Unknown named parameter passed to CreatePostgresSecretFunction: " + lower_name);
		}
	}

	//! Set redact keys
	result->redact_keys = {"password"};
	return std::move(result);
}

void SetPostgresSecretParameters(CreateSecretFunction &function) {
	function.named_parameters["host"] = LogicalType::VARCHAR;
	function.named_parameters["port"] = LogicalType::VARCHAR;
	function.named_parameters["password"] = LogicalType::VARCHAR;
	function.named_parameters["user"] = LogicalType::VARCHAR;
	function.named_parameters["database"] = LogicalType::VARCHAR; // alias for dbname
	function.named_parameters["dbname"] = LogicalType::VARCHAR;
	function.named_parameters["passfile"] = LogicalType::VARCHAR;
	function.named_parameters["use_rds_iam_auth"] = LogicalType::BOOLEAN;
	function.named_parameters["aws_region"] = LogicalType::VARCHAR;
}

void SetPostgresNullByteReplacement(ClientContext &context, SetScope scope, Value &parameter) {
	if (parameter.IsNull()) {
		return;
	}
	for (const auto c : StringValue::Get(parameter)) {
		if (c == '\0') {
			throw BinderException("NULL byte replacement string cannot contain NULL values");
		}
	}
}

static void LoadInternal(ExtensionLoader &loader) {
	PostgresScanFunction postgres_fun;
	loader.RegisterFunction(postgres_fun);

	PostgresScanFunctionFilterPushdown postgres_fun_filter_pushdown;
	loader.RegisterFunction(postgres_fun_filter_pushdown);

	PostgresAttachFunction attach_func;
	loader.RegisterFunction(attach_func);

	PostgresClearCacheFunction clear_cache_func;
	loader.RegisterFunction(clear_cache_func);

	PostgresQueryFunction query_func;
	loader.RegisterFunction(query_func);

	PostgresExecuteFunction execute_func;
	loader.RegisterFunction(execute_func);

	PostgresBinaryCopyFunction binary_copy;
	loader.RegisterFunction(binary_copy);

	// Register the new type
	SecretType secret_type;
	secret_type.name = "postgres";
	secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	secret_type.default_provider = "config";

	loader.RegisterSecretType(secret_type);

	CreateSecretFunction postgres_secret_function = {"postgres", "config", CreatePostgresSecretFunction};
	SetPostgresSecretParameters(postgres_secret_function);
	loader.RegisterFunction(postgres_secret_function);

	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	config.storage_extensions["postgres_scanner"] = make_uniq<PostgresStorageExtension>();

	config.AddExtensionOption("pg_use_binary_copy", "Whether or not to use BINARY copy to read data",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(true));
	config.AddExtensionOption("pg_use_ctid_scan", "Whether or not to parallelize scanning using table ctids",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(true));
	config.AddExtensionOption("pg_pages_per_task", "The amount of pages per task", LogicalType::UBIGINT,
	                          Value::UBIGINT(PostgresBindData::DEFAULT_PAGES_PER_TASK));
	config.AddExtensionOption("pg_connection_limit", "The maximum amount of concurrent Postgres connections",
	                          LogicalType::UBIGINT, Value::UBIGINT(PostgresConnectionPool::DEFAULT_MAX_CONNECTIONS),
	                          SetPostgresConnectionLimit);
	config.AddExtensionOption(
	    "pg_array_as_varchar", "Read Postgres arrays as varchar - enables reading mixed dimensional arrays",
	    LogicalType::BOOLEAN, Value::BOOLEAN(false), PostgresClearCacheFunction::ClearCacheOnSetting);
	config.AddExtensionOption("pg_connection_cache", "Whether or not to use the connection cache", LogicalType::BOOLEAN,
	                          Value::BOOLEAN(true), PostgresConnectionPool::PostgresSetConnectionCache);
	config.AddExtensionOption("pg_experimental_filter_pushdown", "Whether or not to use filter pushdown",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(true));
	config.AddExtensionOption("pg_null_byte_replacement",
	                          "When writing NULL bytes to Postgres, replace them with the given character",
	                          LogicalType::VARCHAR, Value(), SetPostgresNullByteReplacement);
	config.AddExtensionOption("pg_debug_show_queries", "DEBUG SETTING: print all queries sent to Postgres to stdout",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(false), SetPostgresDebugQueryPrint);
	config.AddExtensionOption("pg_use_text_protocol",
	                          "Whether or not to use TEXT protocol to read data. This is slower, but provides better "
	                          "compatibility with non-Postgres systems",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(false));

	OptimizerExtension postgres_optimizer;
	postgres_optimizer.optimize_function = PostgresOptimizer::Optimize;
	config.optimizer_extensions.push_back(std::move(postgres_optimizer));

	config.extension_callbacks.push_back(make_uniq<PostgresExtensionCallback>());
	for (auto &connection : ConnectionManager::Get(loader.GetDatabaseInstance()).GetConnectionList()) {
		connection->registered_state->Insert("postgres_extension", make_shared_ptr<PostgresExtensionState>());
	}
}

void PostgresScannerExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(postgres_scanner, loader) {
	LoadInternal(loader);
}
}
