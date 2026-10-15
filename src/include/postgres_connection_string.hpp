//===----------------------------------------------------------------------===//
//                         DuckDB
//
// postgres_binary_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include "duckdb.hpp"

namespace duckdb {

enum class PostgresConnectionStringFormat { KEY_VALUE, URI };

struct PostgresConnectionString {
	static string Escape(const string &input);

	static string ExtractOption(const KeyValueSecret &kv_secret, const string &name);

	static bool ContainsApplicationName(const std::string &conn_str);

	//! Whether libpq reads the connection string as a URI rather than key/value pairs
	static bool IsURI(const string &conn_string);

	static void AppendDefaultApplicationName(string &conn_string, PostgresConnectionStringFormat format);
};

} // namespace duckdb
