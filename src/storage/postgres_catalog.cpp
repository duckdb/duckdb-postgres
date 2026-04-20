#include "storage/postgres_catalog.hpp"
#include "storage/postgres_schema_entry.hpp"
#include "storage/postgres_transaction.hpp"
#include "postgres_connection.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/common/chrono.hpp"
#include "duckdb/common/printer.hpp"
#include <cstdio>
#include <unordered_map>

namespace duckdb {

namespace {

struct RdsTokenCacheEntry {
	string token;
	steady_clock::time_point expiry;
};

mutex s_rds_cache_mutex;
std::unordered_map<std::string, RdsTokenCacheEntry> s_rds_token_cache;
constexpr int RDS_TOKEN_TTL_SECS = 13 * 60; // 13 minutes — tokens expire at 15

} // namespace

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

// Generate an RDS IAM auth token via the AWS CLI.
// Results are cached for 13 minutes (tokens expire at 15).
// stderr from the aws command is not captured here — it propagates to the process stderr
// so the user can see credential errors directly.
static string GenerateRdsAuthToken(const string &hostname, const string &port, const string &username,
                                   const string &aws_region) {
	string cache_key = hostname + "|" + port + "|" + username + "|" + aws_region;
	{
		lock_guard<mutex> lock(s_rds_cache_mutex);
		auto it = s_rds_token_cache.find(cache_key);
		if (it != s_rds_token_cache.end() &&
		    steady_clock::now() < it->second.expiry) {
			return it->second.token;
		}
	}

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
	// No 2>&1 — aws CLI stderr goes to the process stderr so users see credential errors.

	FILE *pipe = popen(command.c_str(), "r");
	if (!pipe) {
		throw IOException("Failed to execute AWS CLI command to generate RDS auth token. "
		                  "Make sure AWS CLI is installed and configured.");
	}

	string token;
	char buffer[256];
	while (fgets(buffer, sizeof(buffer), pipe) != nullptr) {
		token += buffer;
	}

	int status = pclose(pipe);

	// Strip the single trailing newline the aws CLI appends
	if (!token.empty() && token.back() == '\n') {
		token.pop_back();
	}

	if (status != 0) {
		throw IOException("Failed to generate RDS auth token (aws CLI exited with code %d). "
		                  "Check AWS credentials and IAM permissions.",
		                  status);
	}

	if (PostgresConnection::DebugPrintQueries()) {
		string prefix = token.size() > 6 ? token.substr(0, 6) + "..." : token;
		Printer::Print(StringUtil::Format(
		    "[RDS IAM Auth] Generated token for host=%s port=%s user=%s region=%s (prefix=%s len=%llu)\n",
		    hostname, port, username, aws_region.empty() ? "(default)" : aws_region, prefix,
		    (unsigned long long)token.size()));
	}

	{
		lock_guard<mutex> lock(s_rds_cache_mutex);
		s_rds_token_cache[cache_key] = {
		    token, steady_clock::now() + std::chrono::seconds(RDS_TOKEN_TTL_SECS)};
	}

	return token;
}

PostgresCatalog::PostgresCatalog(AttachedDatabase &db_p, string connection_string_p, string attach_path_p,
                                 AccessMode access_mode, string schema_to_load, PostgresIsolationLevel isolation_level,
                                 ClientContext &context, string secret_name_p)
    : Catalog(db_p), connection_string(std::move(connection_string_p)), attach_path(std::move(attach_path_p)),
      secret_name(std::move(secret_name_p)), access_mode(access_mode), isolation_level(isolation_level),
      schemas(*this, schema_to_load),
      connection_pool(make_shared_ptr<PostgresConnectionPool>(*this, context)), default_schema(schema_to_load) {
	if (default_schema.empty()) {
		default_schema = "public";
	}

	// If the secret uses RDS IAM auth, cache the params for token regeneration in new pool connections.
	// We read them here while we have the ClientContext; CreateNewConnection() has no context.
	if (!secret_name.empty()) {
		auto secret_entry = GetSecret(context, secret_name);
		if (secret_entry) {
			const auto &kv = dynamic_cast<const KeyValueSecret &>(*secret_entry->secret);
			Value rds_val = kv.TryGetValue("use_rds_iam_auth");
			if (!rds_val.IsNull() && BooleanValue::Get(rds_val)) {
				use_rds_iam_auth = true;
				rds_hostname = kv.TryGetValue("host").ToString();
				rds_port = kv.TryGetValue("port").ToString();
				rds_username = kv.TryGetValue("user").ToString();
				Value region_val = kv.TryGetValue("aws_region");
				if (!region_val.IsNull()) {
					rds_region = region_val.ToString();
				}
				// Build the base connection string (without password) for fresh-token assembly
				string base;
				base += AddConnectionOption(kv, "user");
				base += AddConnectionOption(kv, "host");
				base += AddConnectionOption(kv, "port");
				base += AddConnectionOption(kv, "dbname");
				base += AddConnectionOption(kv, "passfile");
				rds_base_connection_string = base + attach_path;
			}
		}
	}

	auto connection = connection_pool->GetConnection();
	this->version = connection.GetConnection().GetPostgresVersion(context);
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

			string rds_token = GenerateRdsAuthToken(hostname, port, username, aws_region);
			new_connection_info += "password=";
			new_connection_info += EscapeConnectionString(rds_token);
			new_connection_info += " ";
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

string PostgresCatalog::GetFreshConnectionString() {
	if (!use_rds_iam_auth) {
		return connection_string;
	}
	string fresh_token = GenerateRdsAuthToken(rds_hostname, rds_port, rds_username, rds_region);
	return rds_base_connection_string + " password=" + EscapeConnectionString(fresh_token) + " ";
}

PostgresCatalog::~PostgresCatalog() = default;

void PostgresCatalog::Initialize(bool load_builtin) {
}

optional_ptr<CatalogEntry> PostgresCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	auto &postgres_transaction = PostgresTransaction::Get(transaction.GetContext(), *this);
	auto entry = schemas.GetEntry(transaction.GetContext(), postgres_transaction, info.schema);
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
	schemas.Scan(context, postgres_transaction,
	             [&](CatalogEntry &schema) { callback(schema.Cast<PostgresSchemaEntry>()); });
}

optional_ptr<SchemaCatalogEntry> PostgresCatalog::LookupSchema(CatalogTransaction transaction,
                                                               const EntryLookupInfo &schema_lookup,
                                                               OnEntryNotFound if_not_found) {
	auto schema_name = schema_lookup.GetEntryName();
	auto &postgres_transaction = PostgresTransaction::Get(transaction.GetContext(), *this);
	if (schema_name == "pg_temp") {
		schema_name = postgres_transaction.GetTemporarySchema();
	}
	auto entry = schemas.GetEntry(transaction.GetContext(), postgres_transaction, schema_name);
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
