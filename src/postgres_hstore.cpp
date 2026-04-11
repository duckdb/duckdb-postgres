#include "postgres_hstore.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"

#include <optional>

namespace duckdb {

struct HstorePair {
	std::string key;
	std::optional<std::string> value;
};

namespace {

bool IsSpace(const char c) {
	const auto u = static_cast<unsigned char>(c);
	return std::isspace(u);
}

void SkipWhitespace(std::string_view input, size_t &pos) {
	while (pos < input.size() && IsSpace(input[pos])) {
		++pos;
	}
}

bool IsNullLiteral(const std::string_view &s) {
	return s.size() == 4 && std::tolower(static_cast<unsigned char>(s[0])) == 'n' &&
	       std::tolower(static_cast<unsigned char>(s[1])) == 'u' &&
	       std::tolower(static_cast<unsigned char>(s[2])) == 'l' &&
	       std::tolower(static_cast<unsigned char>(s[3])) == 'l';
}

std::optional<std::string> ReadToken(std::string_view input, size_t &pos, bool is_key) {
	SkipWhitespace(input, pos);
	if (pos >= input.size()) {
		return std::nullopt;
	}

	std::string result;

	// quoted
	if (input[pos] == '"') {
		++pos; // skip opening quote
		while (pos < input.size()) {
			char c = input[pos];
			++pos;
			if (c == '"') {
				return std::move(result);
			}
			if (c == '\\') {
				if (pos >= input.size()) {
					break;
				}
				result += input[pos];
				++pos;
			} else {
				result += c;
			}
		}
		throw InvalidInputException("syntax error in hstore: unexpected end of string");
	}

	// unquoted
	auto terminator = is_key ? '=' : ',';
	while (pos < input.size()) {
		char c = input[pos];
		if (std::isspace(static_cast<unsigned char>(c)) || c == terminator) {
			break;
		}
		if (c == '\\') {
			++pos;
			if (pos >= input.size()) {
				throw InvalidInputException("syntax error in hstore: unexpected end of string");
			}
			result += input[pos];
		} else {
			result += c;
		}
		++pos;
	}

	if (is_key && result.empty()) {
		throw InvalidInputException("syntax error in hstore, near \"%c\" at position %d", input[pos],
		                            static_cast<int>(pos));
	}
	if (!is_key && IsNullLiteral(result)) {
		return std::nullopt;
	}
	return std::move(result);
}

void ExpectArrow(std::string_view input, size_t &pos) {
	SkipWhitespace(input, pos);
	if (pos + 1 >= input.size() || input[pos] != '=' || input[pos + 1] != '>') {
		throw InvalidInputException("syntax error in hstore, near \"%c\" at position %d",
		                            pos < input.size() ? input[pos] : '?', static_cast<int>(pos));
	}
	pos += 2;
}

std::vector<HstorePair> ParseHstore(std::string_view input) {
	std::vector<HstorePair> pairs;
	size_t pos = 0;

	SkipWhitespace(input, pos);
	while (pos < input.size()) {
		auto key = ReadToken(input, pos, /* is_key = */ true);
		D_ASSERT(key.has_value());
		ExpectArrow(input, pos);
		auto value = ReadToken(input, pos, /* is_key = */ false);
		pairs.push_back({std::move(*key), std::move(value)});

		// Expect comma or end
		SkipWhitespace(input, pos);
		if (pos >= input.size()) {
			break;
		}
		if (input[pos] != ',') {
			throw InvalidInputException("syntax error in hstore, near \"%c\" at position %d", input[pos],
			                            static_cast<int>(pos));
		}
		++pos;
		SkipWhitespace(input, pos);
	}

	return pairs;
}

void JsonEscapeString(std::string &out, const std::string &s) {
	out += '"';
	for (char c : s) {
		switch (c) {
		case '"':
			out += "\\\"";
			break;
		case '\\':
			out += "\\\\";
			break;
		case '\b':
			out += "\\b";
			break;
		case '\f':
			out += "\\f";
			break;
		case '\n':
			out += "\\n";
			break;
		case '\r':
			out += "\\r";
			break;
		case '\t':
			out += "\\t";
			break;
		default:
			if (static_cast<unsigned char>(c) < 0x20) {
				char buf[8];
				snprintf(buf, sizeof(buf), "\\u%04x", static_cast<unsigned char>(c));
				out += buf;
			} else {
				out += c;
			}
			break;
		}
	}
	out += '"';
}

void PostgresHstoreGetFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &hstore_vector = args.data[0];
	auto &key_vector = args.data[1];

	BinaryExecutor::ExecuteWithNulls<string_t, string_t, string_t>(
	    hstore_vector, key_vector, result, args.size(),
	    [&](string_t hstore, string_t key, ValidityMask &mask, idx_t idx) -> string_t {
		    auto pairs = ParseHstore(hstore.GetString());

		    for (auto it = pairs.rbegin(); it != pairs.rend(); ++it) {
			    if (it->key == key.GetString()) {
				    if (!it->value.has_value()) {
					    mask.SetInvalid(idx);
					    return string_t {};
				    }
				    return StringVector::AddString(result, *it->value);
			    }
		    }
		    mask.SetInvalid(idx);
		    return string_t {};
	    });
}

void PostgresHstoreToJsonFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &hstore_vector = args.data[0];

	UnaryExecutor::Execute<string_t, string_t>(hstore_vector, result, args.size(),
	                                           [&](string_t hstore_str) -> string_t {
		                                           auto pairs = ParseHstore(hstore_str.GetString());
		                                           std::string json;
		                                           json += '{';
		                                           bool first = true;
		                                           for (auto &pair : pairs) {
			                                           if (!first) {
				                                           json += ", ";
			                                           }
			                                           first = false;
			                                           JsonEscapeString(json, pair.key);
			                                           json += ": ";
			                                           if (pair.value.has_value()) {
				                                           JsonEscapeString(json, *pair.value);
			                                           } else {
				                                           json += "null";
			                                           }
		                                           }
		                                           json += '}';
		                                           return StringVector::AddString(result, json);
	                                           });
}

} // anonymous namespace

void RegisterHstoreFunctions(ExtensionLoader &loader) {
	auto hstore_get = ScalarFunction("postgres_hstore_get", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                 LogicalType::VARCHAR, PostgresHstoreGetFun);
	loader.RegisterFunction(hstore_get);

	auto hstore_to_json =
	    ScalarFunction("postgres_hstore_to_json", {LogicalType::VARCHAR}, LogicalType::JSON(), PostgresHstoreToJsonFun);
	loader.RegisterFunction(hstore_to_json);
}

} // namespace duckdb
