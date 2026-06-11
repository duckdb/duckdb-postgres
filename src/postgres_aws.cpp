#include "postgres_aws.hpp"

#include <mutex>
#include <stdexcept>

#include "duckdb/parser/keyword_helper.hpp"

#include "dbconnector/defer.hpp"

#include "postgres_secrets.hpp"
#include "postgres_utils.hpp"

namespace duckdb {

bool PostgresAwsRdsTokenConfig::Enabled() {
	return !rds_secret_name.empty();
}

int64_t PostgresAwsRdsTokenConfig::MaxAgeSeconds() {
	return expiration_seconds - 60;
}

static std::string MakeCreateSecretQuery(const std::string &template_secret_name, const std::string &secret_name,
                                         const KeyValueSecret &kv_secret) {
	std::string query("CREATE OR REPLACE TEMPORARY SECRET \"");
	query += secret_name;
	query += "\" (\n";
	query += "  TYPE rds,\n";
	query += "  PROVIDER credential_chain,\n";
	for (auto &en : kv_secret.secret_map) {
		query += "  " + en.first + " " + en.second.ToSQLString() + ",\n";
	}
	query += "  RDS_TEMPLATE_SECRET_NAME " + PostgresUtils::WriteLiteral(template_secret_name) + "\n";
	query += ")";
	return query;
}

static void RunQuery(Connection &conn, const std::string &query, const std::string &err_msg = std::string()) {
	auto res = conn.Query(query);
	if (res->HasError()) {
		if (err_msg.empty()) {
			throw InvalidConfigurationException("Error generating RDS IAM token: %s", res->GetError());
		} else {
			std::string error_type = EnumUtil::ToString(res->GetErrorType());
			throw InvalidConfigurationException("Error generating RDS IAM token, type: %s, %s", error_type, err_msg);
		}
	}
}

static unique_ptr<SecretEntry> GetRdsSecret(SecretManager &secret_manager, Connection &conn, const std::string &name) {
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(*conn.context);
	auto secret_entry = secret_manager.GetSecretByName(transaction, name);
	if (!secret_entry) {
		throw InvalidConfigurationException("Specified RDS secret with name: \"%s\" not found", name);
	}
	return secret_entry;
}

std::string PostgresAws::GenerateRdsAuthToken(AttachedDatabase &attached_db,
                                              const PostgresAwsRdsTokenConfig &token_config) {
	DatabaseInstance &db = attached_db.GetDatabase();
	Connection conn(db);
	RunQuery(conn, "LOAD aws");
	RunQuery(conn, "BEGIN TRANSACTION");
	auto deferred_rollback = dbconnector::Defer([&conn] { conn.Query("ROLLBACK"); });

	SecretManager &secret_manager = SecretManager::Get(db);
	auto template_secret = GetRdsSecret(secret_manager, conn, token_config.rds_secret_name);
	const auto &kv_template_secret = dynamic_cast<const KeyValueSecret &>(*template_secret->secret);

	std::string secret_name = token_config.rds_secret_name + "_" + attached_db.GetName() + "_postgres_temp";
	std::string create_secret_query =
	    MakeCreateSecretQuery(token_config.rds_secret_name, secret_name, kv_template_secret);

	std::string quoted_secret_name = PostgresUtils::WriteIdentifier(secret_name);
	RunQuery(conn, create_secret_query, "error creating RDS secret from template: " + token_config.rds_secret_name);
	auto deferred_drop_secret =
	    dbconnector::Defer([&conn, quoted_secret_name] { conn.Query("DROP SECRET " + quoted_secret_name); });

	auto secret = GetRdsSecret(secret_manager, conn, secret_name);
	const auto &kv_secret = dynamic_cast<const KeyValueSecret &>(*secret->secret);
	std::string token = kv_secret.TryGetValue("session_token").ToString();

	if (token.find("X-Amz-Algorithm") == std::string::npos) {
		throw InvalidConfigurationException("Unable to generate RDS IAM token for secret with name: \"%s\"",
		                                    token_config.rds_secret_name);
	}

	return token;
}

static std::string ExtractString(const KeyValueSecret &kv, const std::string name) {
	Value val = kv.TryGetValue(Identifier(name));
	if (val.IsNull()) {
		return std::string();
	}
	std::string str = StringValue::Get(val);
	StringUtil::Trim(str);
	return str;
}

PostgresAwsRdsTokenConfig PostgresAws::ExtractTokenConfigFromSecret(optional_ptr<SecretEntry> secret_entry) {
	PostgresAwsRdsTokenConfig config;
	if (!secret_entry) {
		return config;
	}
	const auto &kv = dynamic_cast<const KeyValueSecret &>(*secret_entry->secret);

	config.rds_secret_name = ExtractString(kv, "aws_rds_secret");
	if (config.rds_secret_name.empty()) {
		return config;
	}

	std::string password = ExtractString(kv, "password");
	if (!password.empty()) {
		throw BinderException("Invalid AWS RDS IAM auth secret configuration: 'PASSWORD' parameters must not be "
		                      "specified - generated IAM token will be used instead of the password");
	}

	// Build the base connection string (without password)
	for (const string &opt_name : PostgresSecrets::ConnectionOptionNames()) {
		if (opt_name == "password" || opt_name == "passfile") {
			continue;
		}
		config.base_connection_string += PostgresUtils::ExtractConnectionOption(kv, opt_name);
	}

	return config;
}

} // namespace duckdb
