#include "postgres_aws.hpp"

#include <mutex>
#include <stdexcept>

#include <aws/core/Aws.h>
#include <aws/rds/RDSClient.h>

#include "postgres_secrets.hpp"
#include "postgres_utils.hpp"

namespace duckdb {

static std::mutex sdk_init_mutex;
static bool sdk_initialized = false;
static Aws::SDKOptions sdk_options;

static void EnsureAwsSdkInitialized() {
	std::lock_guard<std::mutex> lock(sdk_init_mutex);
	if (!sdk_initialized) {
		Aws::InitAPI(sdk_options);
		sdk_initialized = true;
	}
}

std::string PostgresAws::GenerateRdsAuthToken(const PostgresAwsRdsTokenConfig &token_config) {
	EnsureAwsSdkInitialized();

	Aws::Client::ClientConfiguration config;
	config.region = token_config.region;
	Aws::RDS::RDSClient rds_client(config);

	// https://github.com/aws/aws-sdk-cpp/issues/861#issuecomment-386643571
	// Aws::String token = rdsClient.GenerateConnectAuthToken(hostname.c_str(), aws_region.c_str(),
	// static_cast<unsigned>(port_int), username.c_str());

	std::string host_and_port = token_config.hostname + ":" + token_config.port;
	std::string host_and_port_with_prefix = "http://" + host_and_port;
	std::string host_and_port_with_suffix = host_and_port + "/";
	Aws::Http::URI uri(host_and_port_with_prefix.c_str());
	uri.AddQueryStringParameter("Action", "connect");
	uri.AddQueryStringParameter("DBUser", token_config.username.c_str());
	auto token = rds_client.GeneratePresignedUrl(uri, Aws::Http::HttpMethod::HTTP_GET, token_config.region.c_str(),
	                                             "rds-db", static_cast<long long>(token_config.expiration_seconds));
	Aws::Utils::StringUtils::Replace(token, host_and_port_with_prefix.c_str(), host_and_port_with_suffix.c_str());

	std::string token_str = token.c_str();
	return token_str;
}

static std::string ExtractString(const KeyValueSecret &kv, const std::string name) {
	Value val = kv.TryGetValue(name);
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
	Value enabled_val = kv.TryGetValue("aws_rds_iam_auth_enabled");
	if (enabled_val.IsNull() || !BooleanValue::Get(enabled_val)) {
		return config;
	}

	config.enabled = true;
	config.hostname = ExtractString(kv, "host");
	config.port = ExtractString(kv, "port");
	config.username = ExtractString(kv, "user");
	config.region = ExtractString(kv, "aws_region");
	if (config.hostname.empty() || config.port.empty() || config.username.empty()) {
		throw BinderException("Invalid AWS RDS IAM auth secret configuration: 'HOST', 'PORT', 'USER' and 'AWS_REGION' "
		                      "parameters must be specified");
	}

	std::string password = ExtractString(kv, "password");
	if (!password.empty()) {
		throw BinderException("Invalid AWS RDS IAM auth secret configuration: 'PASSWORD' parameters must not be "
		                      "specified - IAM token will be used instead of the password");
	}

	Value expiration_seconds_val = kv.TryGetValue("aws_rds_iam_token_expiration_seconds");
	if (!expiration_seconds_val.IsNull()) {
		config.expiration_seconds = UBigIntValue::Get(expiration_seconds_val);
	}

	// Build the base connection string (without password)
	for (const string opt_name : PostgresSecrets::ConnectionOptionNames()) {
		if (opt_name == "password" || opt_name == "passfile") {
			continue;
		}
		config.base_connection_string += PostgresUtils::ExtractConnectionOption(kv, opt_name);
	}

	return config;
}

} // namespace duckdb
