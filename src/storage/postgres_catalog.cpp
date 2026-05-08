#include "storage/postgres_catalog.hpp"
#include "storage/postgres_schema_entry.hpp"
#include "storage/postgres_transaction.hpp"
#include "postgres_oauth.hpp"
#include "postgres_connection.hpp"
#include "postgres_secrets.hpp"
#include "storage/postgres_secret_storage.hpp"
#include "postgres_utils.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

namespace duckdb {

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

PostgresCatalog::PostgresCatalog(ClientContext &ctx, AttachedDatabase &db_p, string attach_path_p,
                                 AccessMode access_mode, string schema_to_load, PostgresIsolationLevel isolation_level,
                                 const string &secret_name, SecretStorageTable secret_storage_table_p)
    : Catalog(db_p), attach_path(std::move(attach_path_p)), access_mode(access_mode), isolation_level(isolation_level),
      secret_storage_table(std::move(secret_storage_table_p)), schemas(*this, schema_to_load),
      connection_pool(make_shared_ptr<PostgresConnectionPool>(*this, ctx)), default_schema(schema_to_load) {
	auto secret_entry = GetSecretEntry(ctx, secret_name);
	this->rds_token_config = PostgresAws::ExtractTokenConfigFromSecret(secret_entry);
	if (!rds_token_config.enabled) {
		this->connection_string = CreateConnectionString(secret_entry, attach_path);
	}

	if (default_schema.empty()) {
		default_schema = "public";
	}

	PostgresPoolConnection connection;
	{
		auto oauth_token_holder = SetThreadLocalOAuthTokenFromSessionOption(ctx);
		connection = connection_pool->GetConnection();
	}
	this->version = connection.GetConnection().GetPostgresVersion(ctx);
}

unique_ptr<SecretEntry> PostgresCatalog::GetSecretEntry(ClientContext &ctx, const std::string &secret_name) {
	std::string name = secret_name;
	// if no secret is specified we default to the unnamed postgres secret, if it exists
	bool explicit_secret = !name.empty();
	if (!explicit_secret) {
		// look up settings from the default unnamed postgres secret if none is provided
		name = "__default_postgres";
	}
	auto secret_entry = GetSecret(ctx, name);
	if (!secret_entry && explicit_secret) {
		// secret not found and one was explicitly provided - throw an error
		throw BinderException("Secret with name \"%s\" not found", secret_name);
	}
	return secret_entry;
}

string PostgresCatalog::CreateConnectionString(optional_ptr<SecretEntry> secret_entry, const string &attach_path) {
	string connection_string = attach_path;
	if (secret_entry) {
		// secret found - read data
		const auto &kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_entry->secret);

		// if URI is specified, we use it as a connection string
		Value uri_val = kv_secret.TryGetValue("uri");
		if (!uri_val.IsNull()) {
			// no other options can be specified along with the URI
			for (const string opt_name : PostgresSecrets::ConnectionOptionNames()) {
				if (!kv_secret.TryGetValue(opt_name).IsNull()) {
					throw BinderException("Options with name \"%s\" cannot be specified when 'URI' option is specified",
					                      opt_name);
				}
			}
			// attach path must be empty
			if (!attach_path.empty()) {
				throw BinderException("ATTACH path must be empty when 'URI' option is specified, was: \"%s\"",
				                      attach_path);
			}
			return uri_val.ToString();
		}

		string new_connection_info;
		for (const string opt_name : PostgresSecrets::ConnectionOptionNames()) {
			new_connection_info += PostgresUtils::ExtractConnectionOption(kv_secret, opt_name);
		}
		connection_string = new_connection_info + connection_string;
	}
	return connection_string;
}

string PostgresCatalog::GetConnectionString() {
	if (!rds_token_config.enabled) {
		return connection_string;
	}

	// AWS RDS IAM token hadling below
	std::lock_guard<std::mutex> rds_token_lock(rds_token_mutex);

	auto now = std::chrono::steady_clock::now();
	bool expired = false;
	if (rds_token.empty()) {
		expired = true;
	} else {
		int64_t age_seconds = std::chrono::duration_cast<std::chrono::seconds>(now - rds_token_last_refreshed).count();
		int64_t max_age = static_cast<int64_t>(rds_token_config.expiration_seconds) - 60;
		expired = age_seconds > max_age;
	}

	if (expired) {
		this->rds_token = PostgresAws::GenerateRdsAuthToken(rds_token_config);
		this->rds_token_last_refreshed = now;
		this->connection_string = rds_token_config.base_connection_string +
		                          "password=" + PostgresUtils::EscapeConnectionString(rds_token) + " " + attach_path;
	}
	return std::string(connection_string.data(), connection_string.length());
}

PostgresCatalog::~PostgresCatalog() = default;

void PostgresCatalog::Initialize(bool load_builtin) {
	(void)load_builtin;
	RegisterSecretStorage();
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

void PostgresCatalog::RegisterSecretStorage() {
	// SECRET_STORAGE_TABLE = '' is specified, in this case
	// we don't even proble the DB
	if (secret_storage_table.DisabledByUser()) {
		return;
	}

	// Look up whether the table exists in DB
	auto connection = connection_pool->GetConnection();
	bool secret_storage_table_exists = secret_storage_table.Exists(connection.GetConnection());

	string attached_database_name = GetAttached().GetName();

	if (!secret_storage_table_exists) {
		if (!secret_storage_table.specified_explicitly) {
			// Table does not exist and SECRET_STORAGE_TABLE was not specified by user -
			// no secret storage is registered in this case
			return;
		}

		// Table does not exist and was requested by user - creating it in this PG catalog
		string create_table_error = secret_storage_table.Create(connection.GetConnection());
		if (!create_table_error.empty()) {
			throw IOException("Error initializing secrets storage table, name: \"%s\" in attached database: \"%s\": %s",
			                  secret_storage_table.name, attached_database_name, create_table_error);
		}
	}

	// At this point we are sure that table exists, lets register a secret storage
	// that will read/write this table
	auto &secret_manager = SecretManager::Get(GetAttached().GetDatabase());
	auto secret_storage_name = "postgres_" + attached_database_name;

	// Register the secret storage, name and tie-break clashes handling needs
	// to be improved in secret manager.
	for (;;) {
		try {
			auto secret_storage = make_uniq<PostgresSecretStorage>(secret_storage_name, attached_database_name,
			                                                       secret_storage_table.name);
			secret_manager.LoadSecretStorage(std::move(secret_storage));
		} catch (const InvalidConfigurationException &e) {
			string error = e.what();
			if (error.find("already registered") != std::string::npos) {
				// Storage already exists - this is fine, reuse the existing one
				return;
			}
			if (error.find("tie break score collides") != std::string::npos) {
				// Got a tie break offset clash, lets try again, static offset source is incremented
				// in the constructor of the PostgresSecretStorage.
				continue;
			}

			// Some other error has happened
			throw;
		}
	}
}

} // namespace duckdb
