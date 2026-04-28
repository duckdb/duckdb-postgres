#include "storage/postgres_catalog.hpp"
#include "yugabyte_topology.hpp"
#include "duckdb/common/string_util.hpp"
#include "storage/postgres_schema_entry.hpp"
#include "storage/postgres_transaction.hpp"
#include "postgres_connection.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

namespace duckdb {

static void DiscoverYugabyteTopology(ClientContext &context, PostgresConnection &conn,
                                     const string &connection_string, YugabyteTopology &topology) {
	auto result = conn.TryQuery(context,
	    "SELECT host, port, node_type, cloud, region, zone, public_ip FROM yb_servers()");
	if (!result) {
		return;
	}
	auto rows = result->Count();
	for (idx_t r = 0; r < rows; r++) {
		YugabyteTserver ts;
		ts.host = result->GetString(r, 0);
		ts.port = result->IsNull(r, 1) ? 5433 : static_cast<int32_t>(result->GetInt64(r, 1));
		ts.cloud = result->IsNull(r, 3) ? "" : result->GetString(r, 3);
		ts.region = result->IsNull(r, 4) ? "" : result->GetString(r, 4);
		ts.zone = result->IsNull(r, 5) ? "" : result->GetString(r, 5);
		ts.ip_address = result->IsNull(r, 6) ? ts.host : result->GetString(r, 6);
		topology.tservers.push_back(std::move(ts));
	}

	for (auto &ts : topology.tservers) {
		string probe_dsn = StringUtil::Format(
		    "host='%s' port=%d connect_timeout=2", ts.ip_address, ts.port);
		PGconn *probe = PQconnectdb(probe_dsn.c_str());
		if (probe && PQstatus(probe) == CONNECTION_OK) {
			ts.reachable = true;
			topology.direct_connect_available = true;
		}
		if (probe) {
			PQfinish(probe);
		}
	}
}

PostgresCatalog::PostgresCatalog(AttachedDatabase &db_p, string connection_string_p, string attach_path_p,
                                 AccessMode access_mode, string schema_to_load, PostgresIsolationLevel isolation_level,
                                 ClientContext &context)
    : Catalog(db_p), connection_string(std::move(connection_string_p)), attach_path(std::move(attach_path_p)),
      access_mode(access_mode), isolation_level(isolation_level), schemas(*this, schema_to_load),
      connection_pool(make_shared_ptr<PostgresConnectionPool>(*this, context)), default_schema(schema_to_load) {
	if (default_schema.empty()) {
		default_schema = "public";
	}

	auto connection = connection_pool->GetConnection();
	this->version = connection.GetConnection().GetPostgresVersion(context);

	if (version.type_v == PostgresInstanceType::YUGABYTE) {
		DiscoverYugabyteTopology(context, connection.GetConnection(), connection_string, yb_topology);
	}
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

		new_connection_info += AddConnectionOption(kv_secret, "user");
		new_connection_info += AddConnectionOption(kv_secret, "password");
		new_connection_info += AddConnectionOption(kv_secret, "host");
		new_connection_info += AddConnectionOption(kv_secret, "port");
		new_connection_info += AddConnectionOption(kv_secret, "dbname");
		new_connection_info += AddConnectionOption(kv_secret, "passfile");
		new_connection_info += AddConnectionOption(kv_secret, "options");

		connection_string = new_connection_info + connection_string;
	} else if (explicit_secret) {
		// secret not found and one was explicitly provided - throw an error
		throw BinderException("Secret with name \"%s\" not found", secret_name);
	}
	return connection_string;
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
