#include "postgres_secrets.hpp"

#include <algorithm>
#include <vector>
#include <unordered_map>

namespace duckdb {

// clang-format off
static const std::vector<std::string> key_names = {
  "host",
  "hostaddr",
  "port",
  "dbname",
  "user",
  "password",
  "passfile",
  "require_auth",
  "channel_binding",
  "connect_timeout",
  "client_encoding",
  "options",
  "application_name",
  "fallback_application_name",
  "keepalives",
  "keepalives_idle",
  "keepalives_interval",
  "keepalives_count",
  "tcp_user_timeout",
  "replication",
  "gssencmode",
  "sslmode",
  "requiressl",
  "sslnegotiation",
  "sslcompression",
  "sslcert",
  "sslkey",
  "sslkeylogfile",
  "sslpassword",
  "sslcertmode",
  "sslrootcert",
  "sslcrl",
  "sslcrldir",
  "sslsni",
  "requirepeer",
  "ssl_min_protocol_version",
  "ssl_max_protocol_version",
  "min_protocol_version",
  "max_protocol_version",
  "krbsrvname",
  "gsslib",
  "gssdelegation",
  "scram_client_key",
  "scram_server_key",
  "service",
  "target_session_attrs",
  "load_balance_hosts",
  "oauth_issuer",
  "oauth_client_id",
  "oauth_client_secret",
  "oauth_scope"
};

static const std::unordered_map<std::string, std::string> key_aliases = {
  {"database", "dbname"},
  {"hostname", "host"},
  {"username", "user"}
};
// clang-format on

static const std::string &ResolveAlias(const std::string &input_name) {
	auto it = key_aliases.find(input_name);
	if (it == key_aliases.end()) {
		return input_name;
	}
	return it->second;
}

const std::vector<std::string> &PostgresSecrets::KeyNames() {
	return key_names;
}

unique_ptr<BaseSecret> PostgresSecrets::CreateFunction(ClientContext &context, CreateSecretInput &input) {
	vector<string> prefix_paths;
	auto result = make_uniq<KeyValueSecret>(prefix_paths, "postgres", "config", input.name);
	for (const auto &named_param : input.options) {
		auto input_name = StringUtil::Lower(named_param.first);
		auto name = ResolveAlias(input_name);
		if (std::find(key_names.begin(), key_names.end(), name) == key_names.end()) {
			throw InternalException("Unknown named parameter for a Postgres secret: '" + named_param.first + "'");
		}
		result->secret_map[name] = named_param.second.ToString();
	}
	//! Set redact keys
	result->redact_keys = {"password", "sslpassword", "oauth_client_secret"};
	return std::move(result);
}

void PostgresSecrets::SetSecretParameters(CreateSecretFunction &function) {
	for (const std::string &name : key_names) {
		function.named_parameters[name] = LogicalType::VARCHAR;
	}
	for (auto &en : key_aliases) {
		function.named_parameters[en.first] = LogicalType::VARCHAR;
	}
}

} // namespace duckdb
