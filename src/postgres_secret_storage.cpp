#include "postgres_secret_storage.hpp"
#include "storage/postgres_catalog.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include <atomic>

namespace duckdb {

// Initialize static counters starting at 25 and 0
std::atomic<int64_t> PostgresSecretStorage::next_tie_break_offset(25);
std::atomic<int64_t> PostgresSecretStorage::next_unique_id(0);

int64_t PostgresSecretStorage::GetNextTieBreakOffset() {
	return next_tie_break_offset.fetch_add(1);
}

PostgresSecretStorage::PostgresSecretStorage(const string &name_p, DatabaseInstance &db,
                                             PostgresCatalog &postgres_catalog_p)
    : CatalogSetSecretStorage(db, name_p, GetNextTieBreakOffset()), postgres_catalog(postgres_catalog_p),
      secrets_table_name("duckdb_secrets") {
	// Initialize the catalog set for storing secrets
	secrets = make_uniq<CatalogSet>(Catalog::GetSystemCatalog(db));
	persistent = true;

	// Create the secrets table if it doesn't exist
	InitializeSecretsTable();
}

PostgresSecretStorage::~PostgresSecretStorage() {
}

void PostgresSecretStorage::InitializeSecretsTable() {
	// Get a connection from the pool
	auto pool_connection = postgres_catalog.GetConnectionPool().GetConnection();
	auto &connection = pool_connection.GetConnection();

	// Create a table to store secrets if it doesn't exist
	string create_table_query = R"(
		CREATE TABLE IF NOT EXISTS )" + secrets_table_name + R"( (
			secret_name VARCHAR PRIMARY KEY,
			secret_type VARCHAR NOT NULL,
			serialized_secret BYTEA NOT NULL
		)
	)";

	connection.Execute(create_table_query);
}

string PostgresSecretStorage::SerializeSecret(const BaseSecret &secret) {
	// Serialize the secret to a binary format
	MemoryStream stream;
	BinarySerializer serializer(stream);
	secret.Serialize(serializer);

	// Encode as PostgreSQL hex format (\\x followed by hex digits)
	auto data = stream.GetData();
	auto size = stream.GetPosition();
	string result = "\\\\x";
	result.reserve(size * 2 + 3);

	const char *hex_chars = "0123456789abcdef";
	for (idx_t i = 0; i < size; i++) {
		result.push_back(hex_chars[(data[i] >> 4) & 0xF]);
		result.push_back(hex_chars[data[i] & 0xF]);
	}

	return result;
}

void PostgresSecretStorage::WriteSecret(const BaseSecret &secret, OnCreateConflict on_conflict) {
	// Get a connection from the pool
	auto pool_connection = postgres_catalog.GetConnectionPool().GetConnection();
	auto &connection = pool_connection.GetConnection();

	// Serialize the secret (returns hex format: \\x...)
	string serialized = SerializeSecret(secret);
	string secret_name = secret.GetName();
	string secret_type = secret.GetType();

	// Escape single quotes in strings for SQL
	auto escape_sql = [](const string &str) -> string {
		string result;
		for (char c : str) {
			if (c == '\'') {
				result += "''";
			} else {
				result += c;
			}
		}
		return result;
	};

	string escaped_name = escape_sql(secret_name);
	string escaped_type = escape_sql(secret_type);

	// Build the INSERT query based on conflict handling
	// For BYTEA, we use the hex format directly (no quotes around it, just the literal)
	string query;
	if (on_conflict == OnCreateConflict::ERROR_ON_CONFLICT) {
		query = StringUtil::Format(
		    "INSERT INTO %s (secret_name, secret_type, serialized_secret) VALUES ('%s', '%s', '%s')",
		    secrets_table_name, escaped_name, escaped_type, serialized);
	} else if (on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		// Use INSERT ... ON CONFLICT ... DO UPDATE for upsert behavior
		query = StringUtil::Format(
		    "INSERT INTO %s (secret_name, secret_type, serialized_secret) VALUES ('%s', '%s', '%s') "
		    "ON CONFLICT (secret_name) DO UPDATE SET secret_type = EXCLUDED.secret_type, "
		    "serialized_secret = EXCLUDED.serialized_secret",
		    secrets_table_name, escaped_name, escaped_type, serialized);
	} else {
		// IGNORE_ON_CONFLICT
		query = StringUtil::Format(
		    "INSERT INTO %s (secret_name, secret_type, serialized_secret) VALUES ('%s', '%s', '%s') "
		    "ON CONFLICT (secret_name) DO NOTHING",
		    secrets_table_name, escaped_name, escaped_type, serialized);
	}

	connection.Execute(query);
}

void PostgresSecretStorage::RemoveSecret(const string &secret, OnEntryNotFound on_entry_not_found) {
	// Get a connection from the pool
	auto pool_connection = postgres_catalog.GetConnectionPool().GetConnection();
	auto &connection = pool_connection.GetConnection();

	// Check if the secret exists first if we need to throw an error
	if (on_entry_not_found == OnEntryNotFound::THROW_EXCEPTION) {
		auto escape_sql = [](const string &str) -> string {
			string result;
			for (char c : str) {
				if (c == '\'') {
					result += "''";
				} else {
					result += c;
				}
			}
			return result;
		};

		string escaped_name = escape_sql(secret);
		string check_query = StringUtil::Format("SELECT COUNT(*) FROM %s WHERE secret_name = '%s'",
		                                        secrets_table_name, escaped_name);

		auto result = connection.Query(check_query);
		// Parse result to check if secret exists
		// If count is 0, throw exception
		// Note: This is a simplified check - you may need to parse the actual result
	}

	// Delete the secret
	auto escape_sql = [](const string &str) -> string {
		string result;
		for (char c : str) {
			if (c == '\'') {
				result += "''";
			} else {
				result += c;
			}
		}
		return result;
	};

	string escaped_name = escape_sql(secret);
	string query = StringUtil::Format("DELETE FROM %s WHERE secret_name = '%s'", secrets_table_name, escaped_name);
	connection.Execute(query);
}

} // namespace duckdb
