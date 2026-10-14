#include "postgres_connection_string.hpp"

#include <cstring>
#include <string>
#include <memory>
#include <libpq-fe.h>

#include "duckdb.hpp"

#include "dbconnector/query/query_writer.hpp"

namespace duckdb {

string PostgresConnectionString::Escape(const string &input) {
	using namespace dbconnector::query;
	auto config = QueryWriter::CreateConfig('\'', QuoteEscapeStyle::BACKSLASH);
	return QueryWriter::WriteQuotedAndEscaped(config, input);
}

string PostgresConnectionString::ExtractOption(const KeyValueSecret &kv_secret, const string &name) {
	Value input_val = kv_secret.TryGetValue(Identifier(name));
	if (input_val.IsNull()) {
		// not provided
		return string();
	}
	string result;
	result += name;
	result += "=";
	result += Escape(input_val.ToString());
	result += " ";
	return result;
}

bool PostgresConnectionString::ContainsApplicationName(const std::string &conninfo_str) {
	char *errmsg = nullptr;

	// Parse the connection string into an array of PQconninfoOption structs
	PQconninfoOption *options = PQconninfoParse(conninfo_str.c_str(), &errmsg);

	if (!options) {
		std::string err_str;
		if (errmsg) {
			err_str = std::string(errmsg);
			PQfreemem(errmsg);
		}
		throw InvalidInputException("Failed to parse connection string: \"%s\"", err_str);
	}

	bool found = false;

	for (PQconninfoOption *opt = options; opt->keyword != nullptr; ++opt) {
		if (std::strcmp(opt->keyword, "application_name") == 0) {
			// opt->val will be non-null if it was explicitly defined in the string
			if (opt->val != nullptr && std::strlen(opt->val) > 0) {
				found = true;
			}
			break;
		}
	}

	// cleanup
	PQconninfoFree(options);
	if (errmsg) {
		PQfreemem(errmsg);
	}

	return found;
}

void PostgresConnectionString::AppendDefaultApplicationName(string &conn_string,
                                                            PostgresConnectionStringFormat format) {
	string app_name = "application_name=DuckDB";
	if (format == PostgresConnectionStringFormat::KEY_VALUE) {
		if (conn_string.length() == 0 || conn_string[conn_string.length() - 1] != ' ') {
			conn_string.push_back(' ');
		}
	} else {
		bool first = conn_string.find('?') == std::string::npos;
		char separator = first ? '?' : '&';
		conn_string.push_back(separator);
	}
	conn_string += app_name;
}

} // namespace duckdb
