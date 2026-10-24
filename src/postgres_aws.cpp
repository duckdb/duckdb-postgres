#include "postgres_aws.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database.hpp"

#include <mutex>
#include <stdexcept>

#include "duckdb/parser/keyword_helper.hpp"

#include "dbconnector/defer.hpp"

#include "postgres_connection_string.hpp"
#include "postgres_secrets.hpp"
#include "postgres_utils.hpp"

namespace duckdb {

bool PostgresAwsRdsTokenConfig::Enabled() {
	return !rds_secret_name.empty();
}

int64_t PostgresAwsRdsTokenConfig::MaxAgeSeconds() {
	if (max_age_seconds_override >= 0) {
		return max_age_seconds_override;
	}
	return expiration_seconds - 60;
}

static std::string MakeCreateSecretQuery(const std::string &template_secret_name, const std::string &secret_name,
                                         const identifier_tree_t<Value> &template_options) {
	std::string query("CREATE OR REPLACE TEMPORARY SECRET \"");
	query += secret_name;
	query += "\" (\n";
	query += "  TYPE rds,\n";
	query += "  PROVIDER credential_chain,\n";
	for (auto &en : template_options) {
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

static unique_ptr<SecretEntry> GetRdsSecret(SecretManager &secret_manager, Connection &conn, const std::string &name,
                                            const std::string &storage) {
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(*conn.context);
	auto secret_entry = secret_manager.GetSecretByName(transaction, name, storage);
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
	// The temporary secret is only looked up in (and dropped from) the temporary storage: this can be
	// called when creating a new connection to the attached DB, so the lookup must not reach the
	// Postgres secret storage of this DB - it would need a new connection itself and would deadlock.
	std::string secret_name = token_config.rds_secret_name + "_" + attached_db.GetName() + "_postgres_temp";
	std::string create_secret_query =
	    MakeCreateSecretQuery(token_config.rds_secret_name, secret_name, token_config.template_options);

	std::string quoted_secret_name = PostgresUtils::WriteIdentifier(secret_name);
	RunQuery(conn, create_secret_query, "error creating RDS secret from template: " + token_config.rds_secret_name);
	auto deferred_drop_secret = dbconnector::Defer(
	    [&conn, quoted_secret_name] { conn.Query("DROP TEMPORARY SECRET " + quoted_secret_name + " FROM memory"); });

	auto secret = GetRdsSecret(secret_manager, conn, secret_name, SecretManager::TEMPORARY_STORAGE_NAME);
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

PostgresAwsRdsTokenConfig PostgresAws::ExtractTokenConfigFromSecret(ClientContext &context,
                                                                    optional_ptr<SecretEntry> secret_entry) {
	PostgresAwsRdsTokenConfig config;
	if (!secret_entry) {
		return config;
	}
	const auto &kv = dynamic_cast<const KeyValueSecret &>(*secret_entry->secret);

	config.rds_secret_name = ExtractString(kv, "aws_rds_secret");
	std::string max_age_str = ExtractString(kv, "aws_rds_token_max_age_seconds");
	if (config.rds_secret_name.empty()) {
		if (!max_age_str.empty()) {
			throw BinderException("Invalid AWS RDS IAM auth secret configuration: 'AWS_RDS_TOKEN_MAX_AGE_SECONDS' "
			                      "parameter can only be specified along with 'AWS_RDS_SECRET' parameter");
		}
		return config;
	}

	if (!max_age_str.empty()) {
		int64_t max_age = Value(max_age_str).DefaultCastAs(LogicalType::BIGINT).GetValue<int64_t>();
		int64_t max_allowed = config.MaxAgeSeconds();
		if (max_age < 0 || max_age > max_allowed) {
			throw BinderException("Invalid AWS RDS IAM auth secret configuration: 'AWS_RDS_TOKEN_MAX_AGE_SECONDS' "
			                      "must be between 0 and %d, specified: %d",
			                      max_allowed, max_age);
		}
		config.max_age_seconds_override = max_age;
	}

	std::string password = ExtractString(kv, "password");
	if (!password.empty()) {
		throw BinderException("Invalid AWS RDS IAM auth secret configuration: 'PASSWORD' parameters must not be "
		                      "specified - generated IAM token will be used instead of the password");
	}

	// Resolve the template secret while the Postgres secret storage of the attached DB is not yet registered
	auto &secret_manager = SecretManager::Get(context);
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	auto template_secret = secret_manager.GetSecretByName(transaction, config.rds_secret_name);
	if (!template_secret) {
		throw InvalidConfigurationException("Specified RDS secret with name: \"%s\" not found", config.rds_secret_name);
	}
	const auto &kv_template_secret = dynamic_cast<const KeyValueSecret &>(*template_secret->secret);
	config.template_options = kv_template_secret.secret_map;

	bool has_app_name =
	    !kv.TryGetValue("application_name").IsNull() || !kv.TryGetValue("fallback_application_name").IsNull();

	// Build the base connection string (without password)
	for (const string &opt_name : PostgresSecrets::ConnectionOptionNames()) {
		if (opt_name == "password" || opt_name == "passfile") {
			continue;
		}
		config.base_connection_string += PostgresConnectionString::ExtractOption(kv, opt_name);
	}

	if (!has_app_name) {
		PostgresConnectionString::AppendDefaultApplicationName(config.base_connection_string,
		                                                       PostgresConnectionStringFormat::KEY_VALUE);
		config.base_connection_string.push_back(' ');
	}

	return config;
}

} // namespace duckdb
