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
#include "storage/postgres_connection_pool.hpp"
#include "storage/postgres_optimizer.hpp"
#include "duckdb/planner/extension_callback.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/main/connection_manager.hpp"
#include "duckdb/common/error_data.hpp"
#include "postgres_logging.hpp"
#include "postgres_hstore.hpp"

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
		catalog.Cast<PostgresCatalog>().GetConnectionPool().SetMaxConnections(UBigIntValue::Get(parameter));
	}
	auto &config = DBConfig::GetConfig(context);
	config.SetOption("pg_connection_limit", parameter);

	// propagate the value also to 'pg_pool_max_connections'
	optional_ptr<const ConfigurationOption> option;
	auto setting_index = config.TryGetSettingIndex("pg_pool_max_connections", option);
	if (setting_index.IsValid()) {
		context.config.user_settings.SetUserSetting(setting_index.GetIndex(), parameter);
	}
}

static void DisablePool(ClientContext &context, SetScope scope, Value &parameter) {
	if (scope == SetScope::LOCAL) {
		throw InvalidInputException("pg_connection_cache can only be set globally");
	}
	if (parameter.IsNull() || BooleanValue::Get(parameter)) {
		Value def_size = Value::UBIGINT(dbconnector::pool::ConnectionPoolConfig().max_connections);
		SetPostgresConnectionLimit(context, scope, def_size);
		return;
	}
	Value zero = Value::UBIGINT(0);
	SetPostgresConnectionLimit(context, scope, zero);
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

static std::string CreatePoolNote(const std::string &option) {
	return std::string() + "This option only applies to newly attached Postgres databases, " +
	       "to configure a database that is already attached use " +
	       "\"FROM postgres_configure_pool(catalog_name='my_attached_postgres_db', " + option + ")\"";
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

	PostgresReadBinaryFunction read_binary_func;
	loader.RegisterFunction(read_binary_func);

	PostgresConfigurePoolFunction configure_pool_function;
	loader.RegisterFunction(configure_pool_function);

	RegisterHstoreFunctions(loader);

	// Register the new type
	SecretType secret_type;
	secret_type.name = "postgres";
	secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	secret_type.default_provider = "config";

	loader.RegisterSecretType(secret_type);

	CreateSecretFunction postgres_secret_function = {"postgres", "config", CreatePostgresSecretFunction};
	SetPostgresSecretParameters(postgres_secret_function);
	loader.RegisterFunction(postgres_secret_function);

	dbconnector::pool::ConnectionPoolConfig default_pool_config;
	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	StorageExtension::Register(config, "postgres_scanner", make_shared_ptr<PostgresStorageExtension>());

	config.AddExtensionOption("pg_use_binary_copy", "Whether or not to use BINARY copy to read data",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(true));
	config.AddExtensionOption("pg_use_ctid_scan", "Whether or not to parallelize scanning using table ctids",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(true));
	config.AddExtensionOption("pg_pages_per_task", "The amount of pages per task", LogicalType::UBIGINT,
	                          Value::UBIGINT(PostgresBindData::DEFAULT_PAGES_PER_TASK));
	config.AddExtensionOption(
	    "pg_connection_limit",
	    "The maximum amount of concurrent Postgres connections."
	    " This option is deprecated, instead use \"SET pg_pool_max_connections = 42\" for newly attached Postgres "
	    "databases and \"FROM postgres_configure_pool(catalog_name='my_attached_postgres_db', max_connections=42)\" "
	    "for "
	    "already attached Postgres databases.",
	    LogicalType::UBIGINT, Value::UBIGINT(default_pool_config.max_connections), SetPostgresConnectionLimit);
	config.AddExtensionOption(
	    "pg_array_as_varchar", "Read Postgres arrays as varchar - enables reading mixed dimensional arrays",
	    LogicalType::BOOLEAN, Value::BOOLEAN(false), PostgresClearCacheFunction::ClearCacheOnSetting);
	config.AddExtensionOption(
	    "pg_connection_cache",
	    "Whether or not to use the connection pooling."
	    " This option is deprecated, instead to disable the connection pooling use \"SET pg_pool_max_connections=0\" "
	    "for newly attached Postgres databases and \"FROM "
	    "postgres_configure_pool(catalog_name='my_attached_postgres_db', "
	    "max_connections=0)\" for already attached Postgres databases.",
	    LogicalType::BOOLEAN, Value::BOOLEAN(true), DisablePool);
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
	config.AddExtensionOption("pg_statement_timeout_millis",
	                          "Postgres statement timeout in milliseconds to set on scan connections",
	                          LogicalType::UINTEGER, Value());
	config.AddExtensionOption("pg_idle_in_transaction_timeout_millis",
	                          "Postgres idle in transaction timeout in milliseconds to set on scan connections",
	                          LogicalType::UINTEGER, Value());
	// connection pool options
	config.AddExtensionOption(
	    "pg_pool_acquire_mode",
	    "How to acquire connections from the pool: 'force' (always connect, ignore pool limit), "
	    "'wait' (block until available), 'try' (fail immediately if unavailable) (default: force)",
	    LogicalType::VARCHAR, Value(dbconnector::pool::AcquireModeHelpers::ToString(default_pool_config.acquire_mode)),
	    PostgresConnectionPool::ValidatePoolAcquireMode, SetScope::GLOBAL);
	config.AddExtensionOption("pg_pool_max_connections",
	                          "Maximum number of connections that are allowed to be cached in a connection pool for "
	                          "each attached Postgres database. "
	                          "This number can be temporary exceeded when parallel scans are used. " +
	                              CreatePoolNote("max_connections=42"),
	                          LogicalType::UBIGINT, Value::UBIGINT(default_pool_config.max_connections), nullptr,
	                          SetScope::GLOBAL);
	config.AddExtensionOption("pg_pool_wait_timeout_millis",
	                          "Maximum number of milliseconds to wait when acquiring a connection from a pool where "
	                          "all available connections are already taken. " +
	                              CreatePoolNote("wait_timeout_millis=60000"),
	                          LogicalType::UBIGINT, Value::UBIGINT(default_pool_config.wait_timeout_millis), nullptr,
	                          SetScope::GLOBAL);
	config.AddExtensionOption(
	    "pg_pool_enable_thread_local_cache",
	    "Whether to enable the connection caching in thread-local cache. Such connections are getting pinned to the "
	    "threads and are not made available to other threads, while still taking the place in the pool. " +
	        CreatePoolNote("enable_thread_local_cache=FALSE"),
	    LogicalType::BOOLEAN, Value::BOOLEAN(default_pool_config.tl_cache_enabled), nullptr, SetScope::GLOBAL);
	config.AddExtensionOption("pg_pool_max_lifetime_millis",
	                          "Maximum number of milliseconds the connection can be kept open. This number is checked "
	                          "when the connection is taken from the pool and returned to the pool. "
	                          "When the connection pool reaper thread is enabled ('pg_pool_enable_reaper_thread' "
	                          "option), then this number is checked in background periodically. " +
	                              CreatePoolNote("max_lifetime_millis=600000"),
	                          LogicalType::UBIGINT, Value::UBIGINT(default_pool_config.max_lifetime_millis), nullptr,
	                          SetScope::GLOBAL);
	config.AddExtensionOption("pg_pool_idle_timeout_millis",
	                          "Maximum number of milliseconds the connection can be kept idle in the pool. This number "
	                          "is checked when the connection is taken from the pool. "
	                          "When the connection pool reaper thread is enabled ('pg_pool_enable_reaper_thread' "
	                          "option), then this number is checked in background periodically. " +
	                              CreatePoolNote("idle_timeout_millis=300000"),
	                          LogicalType::UBIGINT, Value::UBIGINT(default_pool_config.idle_timeout_millis), nullptr,
	                          SetScope::GLOBAL);
	config.AddExtensionOption(
	    "pg_pool_enable_reaper_thread",
	    "Whether to enable the connection pool reaper thread, that periodically scans the pool to check the "
	    "'max_lifetime_millis' and 'idle_timeout_millis' and closes the connection which exceed the specified values. "
	    "Either 'max_lifetime_millis' or 'idle_timeout_millis' must be set to a non-zero value for this option to be "
	    "effective. " +
	        CreatePoolNote("enable_reaper_thread=TRUE"),
	    LogicalType::BOOLEAN, Value::BOOLEAN(default_pool_config.start_reaper_thread), nullptr, SetScope::GLOBAL);
	config.AddExtensionOption("pg_pool_health_check_query",
	                          "The query that is used to check that the connection is healthy. Setting this option to "
	                          "an empty string disables the health check. " +
	                              CreatePoolNote("health_check_query=SELECT 42"),
	                          LogicalType::VARCHAR, PostgresConnectionPool::DefaultHealthCheckQuery(), nullptr,
	                          SetScope::GLOBAL);

	OptimizerExtension postgres_optimizer;
	postgres_optimizer.optimize_function = PostgresOptimizer::Optimize;
	OptimizerExtension::Register(config, std::move(postgres_optimizer));

	ExtensionCallback::Register(config, make_shared_ptr<PostgresExtensionCallback>());
	for (auto &connection : ConnectionManager::Get(loader.GetDatabaseInstance()).GetConnectionList()) {
		connection->registered_state->Insert("postgres_extension", make_shared_ptr<PostgresExtensionState>());
	}

	auto &instance = loader.GetDatabaseInstance();
	auto &log_manager = instance.GetLogManager();
	log_manager.RegisterLogType(make_uniq<PostgresQueryLogType>());
}

void PostgresScannerExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(postgres_scanner, loader) {
	LoadInternal(loader);
}
}
