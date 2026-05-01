//===----------------------------------------------------------------------===//
//                         DuckDB
//
// postgres_aws.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <string>

#include "duckdb.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

namespace duckdb {

struct PostgresAwsRdsTokenConfig {
	bool enabled = false;
	std::string hostname;
	std::string port;
	std::string username;
	std::string region;
	std::uint64_t expiration_seconds = 900; // 15 min;
	std::string base_connection_string;
};

struct PostgresAws {
	static std::string GenerateRdsAuthToken(const PostgresAwsRdsTokenConfig &token_config);

	static PostgresAwsRdsTokenConfig ExtractTokenConfigFromSecret(optional_ptr<SecretEntry> secret_entry);
};

} // namespace duckdb
