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
	static const int64_t expiration_seconds = 900; // 15 min, this value is fixed, also used in duckdb-aws

	std::string rds_secret_name;
	std::string base_connection_string;

	bool Enabled();
	int64_t MaxAgeSeconds();
};

struct PostgresAws {
	static std::string GenerateRdsAuthToken(AttachedDatabase &attached_db,
	                                        const PostgresAwsRdsTokenConfig &token_config);

	static PostgresAwsRdsTokenConfig ExtractTokenConfigFromSecret(optional_ptr<SecretEntry> secret_entry);
};

} // namespace duckdb
