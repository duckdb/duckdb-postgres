#include "storage/postgres_catalog.hpp"
#include "storage/postgres_schema_entry.hpp"
#include "storage/postgres_transaction.hpp"
#include "postgres_connection.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/common/printer.hpp"
#include <cstdio>

namespace duckdb {

PostgresCatalog::PostgresCatalog(AttachedDatabase &db_p, string connection_string_p, string attach_path_p,
                                 AccessMode access_mode, string schema_to_load, PostgresIsolationLevel isolation_level,
                                 string secret_name_p)
    : Catalog(db_p), connection_string(std::move(connection_string_p)), attach_path(std::move(attach_path_p)),
      secret_name(std::move(secret_name_p)), access_mode(access_mode), isolation_level(isolation_level),
      schemas(*this, schema_to_load), connection_pool(*this), default_schema(schema_to_load) {
	if (default_schema.empty()) {
		default_schema = "public";
	}
	Value connection_limit;
	auto &db_instance = db_p.GetDatabase();
	if (db_instance.TryGetCurrentSetting("pg_connection_limit", connection_limit)) {
		connection_pool.SetMaximumConnections(UBigIntValue::Get(connection_limit));
	}

	auto connection = connection_pool.GetConnection();
	this->version = connection.GetConnection().GetPostgresVersion();
}

string EscapeConnectionString(const string &input) {
	string result = "'";
	for (auto c : input) {
		if (c == '\\') {
			result += "\\\\";
		} else if (c == '\'') {
			result += "\\'";
		} else {
			result += c;
		}
	}
	result += "'";
	return result;
}

string AddConnectionOption(const KeyValueSecret &kv_secret, const string &name) {
	Value input_val = kv_secret.TryGetValue(name);
	if (input_val.IsNull()) {
		// not provided
		return string();
	}
	string result;
	result += name;
	result += "=";
	result += EscapeConnectionString(input_val.ToString());
	result += " ";
	return result;
}

unique_ptr<SecretEntry> GetSecret(ClientContext &context, const string &secret_name) {
	auto &secret_manager = SecretManager::Get(context);
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	// FIXME: this should be adjusted once the `GetSecretByName` API supports this use case
	auto secret_entry = secret_manager.GetSecretByName(transaction, secret_name, "memory");
	if (secret_entry) {
		return secret_entry;
	}
	secret_entry = secret_manager.GetSecretByName(transaction, secret_name, "local_file");
	if (secret_entry) {
		return secret_entry;
	}
	return nullptr;
}

string GenerateRdsAuthToken(const string &hostname, const string &port, const string &username,
                             const string &aws_region) {

	auto escape_shell_arg = [](const string &arg) -> string {
		string escaped = "'";
		for (char c : arg) {
			if (c == '\'') {
				escaped += "'\\''";
			} else {
				escaped += c;
			}
		}
		escaped += "'";
		return escaped;
	};

	string command = "aws rds generate-db-auth-token --hostname " + escape_shell_arg(hostname) +
	                 " --port " + escape_shell_arg(port) + " --username " + escape_shell_arg(username);
	
	if (!aws_region.empty()) {
		command += " --region " + escape_shell_arg(aws_region);
	}
	
	command += " 2>&1";

	FILE *pipe = popen(command.c_str(), "r");
	if (!pipe) {
		throw IOException("Failed to execute AWS CLI command to generate RDS auth token. "
		                  "Make sure AWS CLI is installed and configured.");
	}

	string token;
	char buffer[128];
	while (fgets(buffer, sizeof(buffer), pipe) != nullptr) {
		token += buffer;
	}

	int status = pclose(pipe);

	if (!token.empty() && token.back() == '\n') {
		token.pop_back();
	}
	if (status != 0) {

		throw IOException("Failed to generate RDS auth token: %s. "
		                  "Make sure AWS CLI is installed, configured, and you have the necessary permissions.",
		                  token.empty() ? "Unknown error" : token.c_str());
	}

	if (!token.empty() && token.back() == '\n') {
		token.pop_back();
	}


	if (PostgresConnection::DebugPrintQueries()) {
		string debug_msg = StringUtil::Format(
		    "[RDS IAM Auth] Generated auth token for hostname=%s, port=%s, username=%s, region=%s\n, token=%s",
		    hostname.c_str(), port.c_str(), username.c_str(),
		    aws_region.empty() ? "(default)" : aws_region.c_str(), token.c_str());
		Printer::Print(debug_msg);
	}

	return token;
}

string PostgresCatalog::GetConnectionString(ClientContext &context, const string &attach_path, string secret_name) {
	// if no secret is specified we default to the unnamed postgres secret, if it exists
	string connection_string = attach_path;
	bool explicit_secret = !secret_name.empty();
	if (!explicit_secret) {
		// look up settings from the default unnamed postgres secret if none is provided
		secret_name = "__default_postgres";
	}

	auto secret_entry = GetSecret(context, secret_name);
	if (secret_entry) {
		// secret found - read data
		const auto &kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_entry->secret);
		string new_connection_info;

		Value use_rds_iam_auth_val = kv_secret.TryGetValue("use_rds_iam_auth");
		bool use_rds_iam_auth = false;
		if (!use_rds_iam_auth_val.IsNull()) {
			use_rds_iam_auth = BooleanValue::Get(use_rds_iam_auth_val);
		}

		new_connection_info += AddConnectionOption(kv_secret, "user");
		
		if (use_rds_iam_auth) {
			Value host_val = kv_secret.TryGetValue("host");
			Value port_val = kv_secret.TryGetValue("port");
			Value user_val = kv_secret.TryGetValue("user");
			Value aws_region_val = kv_secret.TryGetValue("aws_region");

			if (host_val.IsNull() || port_val.IsNull() || user_val.IsNull()) {
				throw BinderException(
				    "RDS IAM authentication requires 'host', 'port', and 'user' to be set in the secret");
			}

			string hostname = host_val.ToString();
			string port = port_val.ToString();
			string username = user_val.ToString();
			string aws_region;


			if (!aws_region_val.IsNull()) {
				aws_region = aws_region_val.ToString();
			}

			try {
				string rds_token = GenerateRdsAuthToken(hostname, port, username, aws_region);
				new_connection_info += "password=";
				new_connection_info += EscapeConnectionString(rds_token);
				new_connection_info += " ";
			} catch (const std::exception &e) {
				throw BinderException("Failed to generate RDS auth token: %s", e.what());
			}
		} else {
			new_connection_info += AddConnectionOption(kv_secret, "password");
		}

		new_connection_info += AddConnectionOption(kv_secret, "host");
		new_connection_info += AddConnectionOption(kv_secret, "port");
		new_connection_info += AddConnectionOption(kv_secret, "dbname");
		new_connection_info += AddConnectionOption(kv_secret, "passfile");

		connection_string = new_connection_info + connection_string;
	} else if (explicit_secret) {
		// secret not found and one was explicitly provided - throw an error
		throw BinderException("Secret with name \"%s\" not found", secret_name);
	}
	return connection_string;
}

string PostgresCatalog::GetFreshConnectionString(ClientContext &context) {
	return GetConnectionString(context, attach_path, secret_name);
}

PostgresCatalog::~PostgresCatalog() = default;

void PostgresCatalog::Initialize(bool load_builtin) {
}

optional_ptr<CatalogEntry> PostgresCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	auto &postgres_transaction = PostgresTransaction::Get(transaction.GetContext(), *this);
	auto entry = schemas.GetEntry(postgres_transaction, info.schema);
	if (entry) {
		switch (info.on_conflict) {
		case OnCreateConflict::REPLACE_ON_CONFLICT: {
			DropInfo try_drop;
			try_drop.type = CatalogType::SCHEMA_ENTRY;
			try_drop.name = info.schema;
			try_drop.if_not_found = OnEntryNotFound::RETURN_NULL;
			try_drop.cascade = false;
			schemas.DropEntry(postgres_transaction, try_drop);
			break;
		}
		case OnCreateConflict::IGNORE_ON_CONFLICT:
			return entry;
		case OnCreateConflict::ERROR_ON_CONFLICT:
		default:
			throw BinderException("Failed to create schema \"%s\": schema already exists", info.schema);
		}
	}
	return schemas.CreateSchema(postgres_transaction, info);
}

void PostgresCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	auto &postgres_transaction = PostgresTransaction::Get(context, *this);
	return schemas.DropEntry(postgres_transaction, info);
}

void PostgresCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	auto &postgres_transaction = PostgresTransaction::Get(context, *this);
	schemas.Scan(postgres_transaction, [&](CatalogEntry &schema) { callback(schema.Cast<PostgresSchemaEntry>()); });
}

optional_ptr<SchemaCatalogEntry> PostgresCatalog::LookupSchema(CatalogTransaction transaction,
                                                               const EntryLookupInfo &schema_lookup,
                                                               OnEntryNotFound if_not_found) {
	auto schema_name = schema_lookup.GetEntryName();
	auto &postgres_transaction = PostgresTransaction::Get(transaction.GetContext(), *this);
	if (schema_name == "pg_temp") {
		schema_name = postgres_transaction.GetTemporarySchema();
	}
	auto entry = schemas.GetEntry(postgres_transaction, schema_name);
	if (!entry && if_not_found != OnEntryNotFound::RETURN_NULL) {
		throw BinderException("Schema with name \"%s\" not found", schema_name);
	}
	return reinterpret_cast<SchemaCatalogEntry *>(entry.get());
}

bool PostgresCatalog::InMemory() {
	return false;
}

string PostgresCatalog::GetDBPath() {
	return attach_path;
}

DatabaseSize PostgresCatalog::GetDatabaseSize(ClientContext &context) {
	auto &postgres_transaction = PostgresTransaction::Get(context, *this);
	auto result = postgres_transaction.Query("SELECT pg_database_size(current_database());");
	DatabaseSize size;
	size.free_blocks = 0;
	size.total_blocks = 0;
	size.used_blocks = 0;
	size.wal_size = 0;
	size.block_size = 0;
	size.bytes = result->GetInt64(0, 0);
	return size;
}

void PostgresCatalog::ClearCache() {
	schemas.ClearEntries();
}

} // namespace duckdb
