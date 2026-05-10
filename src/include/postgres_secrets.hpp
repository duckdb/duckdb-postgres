//===----------------------------------------------------------------------===//
//                         DuckDB
//
// postgres_scanner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

namespace duckdb {

struct PostgresSecrets {
	static const std::vector<std::string> &ConnectionOptionNames();

	static SecretType CreateType();

	static SecretType CreateRdsType();

	static unique_ptr<BaseSecret> CreateFunction(ClientContext &context, CreateSecretInput &input);

	static void SetSecretParameters(CreateSecretFunction &function);
};

} // namespace duckdb
