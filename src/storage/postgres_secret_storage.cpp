#include "storage/postgres_secret_storage.hpp"
#include "storage/postgres_catalog.hpp"
#include "storage/postgres_transaction.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
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
	return next_tie_break_offset.fetch_add(1, std::memory_order_acq_rel);
}

PostgresSecretStorage::PostgresSecretStorage(const string &storage_name, string attached_database_name_p,
                                             string secrets_table_name_p)
    : SecretStorage(storage_name, GetNextTieBreakOffset()), attached_database_name(std::move(attached_database_name_p)),
      secrets_table_name(std::move(secrets_table_name_p)) {
	persistent = true;
}

PostgresSecretStorage::~PostgresSecretStorage() {
}

optional_ptr<PostgresCatalog> PostgresSecretStorage::GetPostgresCatalog(ClientContext &context) {
	auto &db_manager = DatabaseManager::Get(context);
	auto attached_db = db_manager.GetDatabase(context, attached_database_name);
	if (!attached_db) {
		return nullptr;
	}
	return &attached_db->GetCatalog().Cast<PostgresCatalog>();
}

unique_ptr<const BaseSecret> PostgresSecretStorage::DeserializeSecret(PostgresCatalog &postgres_catalog,
                                                                      const string &hex_string_ref,
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
	auto &secret_manager = SecretManager::Get(postgres_catalog.GetAttached().GetDatabase());
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

	// Get the Postgres catalog and transaction
	auto &context = transaction->GetContext();
	auto postgres_catalog_ptr = GetPostgresCatalog(context);
	if (!postgres_catalog_ptr) {
		throw InvalidInputException("Cannot store secret: database '%s' is not attached", attached_database_name);
	}
	auto &postgres_catalog = *postgres_catalog_ptr;
	auto &postgres_transaction = PostgresTransaction::Get(context, postgres_catalog);

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

	PostgresConnection &conn = postgres_transaction.GetConnection();
	string err_msg;
	conn.TryQuery(postgres_transaction.GetContext(), query, &err_msg);

	// Report the error filtering out the secret
	if (!err_msg.empty()) {
		size_t pos = err_msg.find(serialized);
		if (pos != std::string::npos) {
			err_msg.replace(pos, serialized.length(), "<serialized_secret>");
		}
		throw IOException(err_msg);
	}

	// Return the secret entry
	auto secret_entry = make_uniq<SecretEntry>(std::move(secret));
	secret_entry->storage_mode = storage_name;
	secret_entry->persist_type = SecretPersistType::PERSISTENT;
	return secret_entry;
}

vector<SecretEntry> PostgresSecretStorage::AllSecrets(optional_ptr<CatalogTransaction> transaction) {
	vector<SecretEntry> result;

	auto &context = transaction->GetContext();
	auto postgres_catalog_ptr = GetPostgresCatalog(context);
	if (!postgres_catalog_ptr) {
		// Database is not attached, return empty list
		return result;
	}
	auto &postgres_catalog = *postgres_catalog_ptr;
	auto &postgres_transaction = PostgresTransaction::Get(context, postgres_catalog);

	string query = StringUtil::Format("SELECT secret_name, serialized_secret FROM %s", secrets_table_name);
	auto query_result = postgres_transaction.Query(query);

	for (idx_t i = 0; i < query_result->Count(); i++) {
		auto secret_name = query_result->GetString(i, 0);
		auto hex_string = query_result->GetString(i, 1);

		auto secret = DeserializeSecret(postgres_catalog, hex_string, secret_name);
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
	auto &context = transaction->GetContext();
	auto postgres_catalog_ptr = GetPostgresCatalog(context);
	if (!postgres_catalog_ptr) {
		if (on_entry_not_found == OnEntryNotFound::THROW_EXCEPTION) {
			throw InvalidInputException("Cannot drop secret: database '%s' is not attached", attached_database_name);
		}
		return;
	}
	auto &postgres_catalog = *postgres_catalog_ptr;

	// Check if secret exists
	auto existing = GetSecretByName(name, transaction);
	if (!existing && on_entry_not_found == OnEntryNotFound::THROW_EXCEPTION) {
		throw InvalidInputException("Failed to remove non-existent persistent secret '%s' in secret storage '%s'", name,
		                            storage_name);
	}

	auto &postgres_transaction = PostgresTransaction::Get(context, postgres_catalog);

	// Delete the secret
	string escaped_name = EscapeSQLString(name);
	string query = StringUtil::Format("DELETE FROM %s WHERE secret_name = '%s'", secrets_table_name, escaped_name);
	postgres_transaction.Query(query);
}

SecretMatch PostgresSecretStorage::LookupSecret(const string &path, const string &type,
                                                optional_ptr<CatalogTransaction> transaction) {
	auto best_match = SecretMatch();

	auto &context = transaction->GetContext();
	auto postgres_catalog_ptr = GetPostgresCatalog(context);
	if (!postgres_catalog_ptr) {
		// Database is not attached, return no match
		return best_match;
	}
	auto &postgres_catalog = *postgres_catalog_ptr;

	auto &postgres_transaction = PostgresTransaction::Get(context, postgres_catalog);

	string escaped_type = EscapeSQLString(StringUtil::Lower(type));
	string query = StringUtil::Format("SELECT secret_name, serialized_secret FROM %s WHERE LOWER(secret_type) = '%s'",
	                                  secrets_table_name, escaped_type);
	auto query_result = postgres_transaction.Query(query);

	for (idx_t i = 0; i < query_result->Count(); i++) {
		auto secret_name = query_result->GetString(i, 0);
		auto hex_string = query_result->GetString(i, 1);

		auto secret = DeserializeSecret(postgres_catalog, hex_string, secret_name);
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
	auto &context = transaction->GetContext();
	auto postgres_catalog_ptr = GetPostgresCatalog(context);
	if (!postgres_catalog_ptr) {
		// Database is not attached, return null
		return nullptr;
	}
	auto &postgres_catalog = *postgres_catalog_ptr;
	auto &postgres_transaction = PostgresTransaction::Get(context, postgres_catalog);

	string escaped_name = EscapeSQLString(name);
	string query = StringUtil::Format("SELECT serialized_secret FROM %s WHERE secret_name = '%s'", secrets_table_name,
	                                  escaped_name);
	auto result = postgres_transaction.Query(query);

	if (result->Count() == 0) {
		return nullptr;
	}

	auto hex_string = result->GetString(0, 0);
	auto secret = DeserializeSecret(postgres_catalog, hex_string, name);
	if (!secret) {
		return nullptr;
	}

	auto secret_entry = make_uniq<SecretEntry>(std::move(secret));
	secret_entry->storage_mode = storage_name;
	secret_entry->persist_type = SecretPersistType::PERSISTENT;
	return secret_entry;
}

string PostgresSecretStorage::InitializeSecretsTable(PostgresConnection &connection,
                                                     const std::string &secrets_table_name) {
	// Check whether the table exists, 'to_regclass' should require the least priviliges
	auto res = connection.Query(nullptr, "SELECT to_regclass('" + secrets_table_name + "')");
	if (!res->IsNull(0, 0)) {
		return std::string();
	}

	// Create the table to store secrets
	string create_table_query = R"(
		CREATE TABLE )" + secrets_table_name +
	                            R"( (
			secret_name VARCHAR PRIMARY KEY,
			secret_type VARCHAR NOT NULL,
			serialized_secret BYTEA NOT NULL
		)
	)";

	string err_msg;
	connection.TryQuery(nullptr, create_table_query, &err_msg);
	return err_msg;
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
