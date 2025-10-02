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

// Helper function to escape SQL strings
static string EscapeSQLString(const string &str) {
	string result;
	for (char c : str) {
		if (c == '\'') {
			result += "''";
		} else {
			result += c;
		}
	}
	return result;
}

// Initialize static counter starting at 25
std::atomic<int64_t> PostgresSecretStorage::next_tie_break_offset(25);

int64_t PostgresSecretStorage::GetNextTieBreakOffset() {
	return next_tie_break_offset.fetch_add(1);
}

PostgresSecretStorage::PostgresSecretStorage(const string &name_p, DatabaseInstance &db_p,
                                             PostgresCatalog &postgres_catalog_p, SecretManager &secret_manager_p)
    : SecretStorage(name_p, GetNextTieBreakOffset()), db(db_p), secret_manager(secret_manager_p),
      postgres_catalog(postgres_catalog_p), secrets_table_name("duckdb_secrets") {
	persistent = true;

	// Create the secrets table if it doesn't exist
	InitializeSecretsTable();
}

PostgresSecretStorage::~PostgresSecretStorage() {
}

unique_ptr<const BaseSecret> PostgresSecretStorage::DeserializeSecret(const string &hex_string_ref,
                                                                       const string &secret_name) {
	// Parse the hex string (format: \x followed by hex digits)
	if (hex_string_ref.size() < 2 || hex_string_ref[0] != '\\' || hex_string_ref[1] != 'x') {
		return nullptr;
	}

	auto hex_data = hex_string_ref.c_str() + 2;
	auto hex_size = hex_string_ref.size() - 2;

	// Convert hex to binary
	vector<uint8_t> binary_data;
	binary_data.reserve(hex_size / 2);

	for (idx_t i = 0; i < hex_size; i += 2) {
		uint8_t byte = 0;
		for (idx_t j = 0; j < 2; j++) {
			byte <<= 4;
			char c = hex_data[i + j];
			if (c >= '0' && c <= '9') {
				byte |= (c - '0');
			} else if (c >= 'a' && c <= 'f') {
				byte |= (c - 'a' + 10);
			} else if (c >= 'A' && c <= 'F') {
				byte |= (c - 'A' + 10);
			}
		}
		binary_data.push_back(byte);
	}

	// Deserialize the secret
	MemoryStream stream(data_ptr_cast(binary_data.data()), binary_data.size());
	BinaryDeserializer deserializer(stream);

	deserializer.Begin();
	auto secret = secret_manager.DeserializeSecret(deserializer, secret_name);
	deserializer.End();

	return secret;
}

unique_ptr<SecretEntry> PostgresSecretStorage::StoreSecret(unique_ptr<const BaseSecret> secret,
                                                            OnCreateConflict on_conflict,
                                                            optional_ptr<CatalogTransaction> transaction) {
	auto secret_name = secret->GetName();
	auto secret_type = secret->GetType();

	// Check if secret already exists
	auto existing = GetSecretByName(secret_name, transaction);
	if (existing) {
		if (on_conflict == OnCreateConflict::ERROR_ON_CONFLICT) {
			throw InvalidInputException("Persistent secret with name '%s' already exists in secret storage '%s'!",
			                            secret_name, storage_name);
		} else if (on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
			return nullptr;
		}
		// REPLACE_ON_CONFLICT - continue to overwrite
	}

	// Serialize the secret BEFORE moving it
	string serialized = SerializeSecret(*secret);

	// Get a connection from the pool
	auto pool_connection = postgres_catalog.GetConnectionPool().GetConnection();
	auto &connection = pool_connection.GetConnection();

	string escaped_name = EscapeSQLString(secret_name);
	string escaped_type = EscapeSQLString(secret_type);

	// Build the INSERT query based on conflict handling
	string query;
	if (on_conflict == OnCreateConflict::ERROR_ON_CONFLICT) {
		query = StringUtil::Format(
		    "INSERT INTO %s (secret_name, secret_type, serialized_secret) VALUES ('%s', '%s', E'%s')",
		    secrets_table_name, escaped_name, escaped_type, serialized);
	} else {
		// REPLACE_ON_CONFLICT or IGNORE (though IGNORE is handled above)
		query = StringUtil::Format(
		    "INSERT INTO %s (secret_name, secret_type, serialized_secret) VALUES ('%s', '%s', E'%s') "
		    "ON CONFLICT (secret_name) DO UPDATE SET secret_type = EXCLUDED.secret_type, "
		    "serialized_secret = EXCLUDED.serialized_secret",
		    secrets_table_name, escaped_name, escaped_type, serialized);
	}

	connection.Execute(query);

	// Return the secret entry
	auto secret_entry = make_uniq<SecretEntry>(std::move(secret));
	secret_entry->storage_mode = storage_name;
	secret_entry->persist_type = SecretPersistType::PERSISTENT;
	return secret_entry;
}

vector<SecretEntry> PostgresSecretStorage::AllSecrets(optional_ptr<CatalogTransaction> transaction) {
	vector<SecretEntry> result;

	auto pool_connection = postgres_catalog.GetConnectionPool().GetConnection();
	auto &connection = pool_connection.GetConnection();

	string query = StringUtil::Format("SELECT secret_name, serialized_secret FROM %s", secrets_table_name);
	auto query_result = connection.Query(query);

	for (idx_t i = 0; i < query_result->Count(); i++) {
		auto secret_name = query_result->GetString(i, 0);
		auto hex_string = query_result->GetString(i, 1);

		auto secret = DeserializeSecret(hex_string, secret_name);
		if (secret) {
			SecretEntry entry(std::move(secret));
			entry.storage_mode = storage_name;
			entry.persist_type = SecretPersistType::PERSISTENT;
			result.push_back(std::move(entry));
		}
	}

	return result;
}

void PostgresSecretStorage::DropSecretByName(const string &name, OnEntryNotFound on_entry_not_found,
                                             optional_ptr<CatalogTransaction> transaction) {
	// Check if secret exists
	auto existing = GetSecretByName(name, transaction);
	if (!existing && on_entry_not_found == OnEntryNotFound::THROW_EXCEPTION) {
		throw InvalidInputException("Failed to remove non-existent persistent secret '%s' in secret storage '%s'", name,
		                            storage_name);
	}

	// Get a connection from the pool
	auto pool_connection = postgres_catalog.GetConnectionPool().GetConnection();
	auto &connection = pool_connection.GetConnection();

	// Delete the secret
	string escaped_name = EscapeSQLString(name);
	string query = StringUtil::Format("DELETE FROM %s WHERE secret_name = '%s'", secrets_table_name, escaped_name);
	connection.Execute(query);
}

SecretMatch PostgresSecretStorage::LookupSecret(const string &path, const string &type,
                                                optional_ptr<CatalogTransaction> transaction) {
	auto best_match = SecretMatch();

	auto pool_connection = postgres_catalog.GetConnectionPool().GetConnection();
	auto &connection = pool_connection.GetConnection();

	string escaped_type = EscapeSQLString(StringUtil::Lower(type));
	string query = StringUtil::Format("SELECT secret_name, serialized_secret FROM %s WHERE LOWER(secret_type) = '%s'",
										secrets_table_name, escaped_type);
	auto query_result = connection.Query(query);

	for (idx_t i = 0; i < query_result->Count(); i++) {
		auto secret_name = query_result->GetString(i, 0);
		auto hex_string = query_result->GetString(i, 1);

		auto secret = DeserializeSecret(hex_string, secret_name);
		if (secret) {
			SecretEntry entry(std::move(secret));
			entry.storage_mode = storage_name;
			entry.persist_type = SecretPersistType::PERSISTENT;
			best_match = SelectBestMatch(entry, path, tie_break_offset, best_match);
		}
	}
	
	return best_match;
}

unique_ptr<SecretEntry> PostgresSecretStorage::GetSecretByName(const string &name,
                                                                optional_ptr<CatalogTransaction> transaction) {
	auto pool_connection = postgres_catalog.GetConnectionPool().GetConnection();
	auto &connection = pool_connection.GetConnection();

	string escaped_name = EscapeSQLString(name);
	string query = StringUtil::Format("SELECT serialized_secret FROM %s WHERE secret_name = '%s'", secrets_table_name,
										escaped_name);
	auto result = connection.Query(query);

	if (result->Count() == 0) {
		return nullptr;
	}

	auto hex_string = result->GetString(0, 0);
	auto secret = DeserializeSecret(hex_string, name);
	if (!secret) {
		return nullptr;
	}

	auto secret_entry = make_uniq<SecretEntry>(std::move(secret));
	secret_entry->storage_mode = storage_name;
	secret_entry->persist_type = SecretPersistType::PERSISTENT;
	return secret_entry;
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
	serializer.Begin();
	secret.Serialize(serializer);
	serializer.End();

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

} // namespace duckdb
