//===----------------------------------------------------------------------===//
//                         DuckDB
//
// postgres_aws.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include "duckdb/main/attached_database.hpp"
#include <string>

#include "duckdb.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

namespace duckdb {

struct PostgresAwsRdsTokenConfig {
	static const int64_t expiration_seconds = 900; // 15 min, this value is fixed, also used in duckdb-aws

	std::string rds_secret_name;
	std::string base_connection_string;
	//! Options of the RDS template secret, resolved once at ATTACH time. The template is not looked up
	//! by name when the token is re-generated, because such lookup searches all secret storages,
	//! including the Postgres secret storage of the attached DB, that requires a new connection.
	identifier_tree_t<Value> template_options;
	//! Overrides the max token age, set from 'aws_rds_token_max_age_seconds' secret option
	int64_t max_age_seconds_override = -1;

	bool Enabled();
	int64_t MaxAgeSeconds();
};

struct PostgresAws {
	static std::string GenerateRdsAuthToken(AttachedDatabase &attached_db,
	                                        const PostgresAwsRdsTokenConfig &token_config);

	static PostgresAwsRdsTokenConfig ExtractTokenConfigFromSecret(ClientContext &context,
	                                                              optional_ptr<SecretEntry> secret_entry);
};

} // namespace duckdb
