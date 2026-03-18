#include "postgres_connection.hpp"
#include "postgres_binary_writer.hpp"
#include "postgres_text_writer.hpp"
#include "storage/postgres_table_entry.hpp"

namespace duckdb {

void PostgresCopyState::Initialize(ClientContext &context) {
	Value replacement_value;
	if (!context.TryGetCurrentSetting("pg_null_byte_replacement", replacement_value)) {
		return;
	}
	if (replacement_value.IsNull()) {
		return;
	}
	auto replacement_str = StringValue::Get(replacement_value);
	for (const auto c : replacement_str) {
		if (c == '\0') {
			throw InternalException("NULL byte replacement string cannot contain NULL values");
		}
	}
	has_null_byte_replacement = true;
	null_byte_replacement = std::move(replacement_str);
}

void PostgresConnection::BeginCopyTo(ClientContext &context, PostgresCopyState &state, PostgresCopyFormat format,
                                     const string &schema_name, const string &table_name,
                                     const vector<string> &column_names) {
	string query = "COPY ";
	if (!schema_name.empty()) {
		query += KeywordHelper::WriteQuoted(schema_name, '"') + ".";
	}
	query += KeywordHelper::WriteQuoted(table_name, '"') + " ";
	if (!column_names.empty()) {
		query += "(";
		for (idx_t c = 0; c < column_names.size(); c++) {
			if (c > 0) {
				query += ", ";
			}
			query += KeywordHelper::WriteQuoted(column_names[c], '"');
		}
		query += ") ";
	}
	query += "FROM STDIN (FORMAT ";
	state.Initialize(context);
	state.format = format;
	switch (state.format) {
	case PostgresCopyFormat::BINARY:
		query += "BINARY";
		break;
	case PostgresCopyFormat::TEXT:
		query += "TEXT, NULL '\b'";
		break;
	default:
		throw InternalException("Unsupported type for postgres copy format");
	}
	query += ")";

	PostgresResult pg_res(PQExecute(context, query.c_str()));
	auto result = pg_res.res;
	if (!result || PQresultStatus(result) != PGRES_COPY_IN) {
		throw std::runtime_error("Failed to prepare COPY \"" + query + "\": " + string(PQresultErrorMessage(result)));
	}
	if (state.format == PostgresCopyFormat::BINARY) {
		// binary copy requires a header
		PostgresBinaryWriter writer(state);
		writer.WriteHeader();
		CopyData(writer);
	}
}

void PostgresConnection::CopyData(data_ptr_t buffer, idx_t size) {
	int result;
	do {
		result = PQputCopyData(GetConn(), (const char *)buffer, int(size));
	} while (result == 0);
	if (result == -1) {
		throw InternalException("Error during PQputCopyData: %s", PQerrorMessage(GetConn()));
	}
}

void PostgresConnection::CopyData(PostgresBinaryWriter &writer) {
	CopyData(writer.stream.GetData(), writer.stream.GetPosition());
}

void PostgresConnection::CopyData(PostgresTextWriter &writer) {
	CopyData(writer.stream.GetData(), writer.stream.GetPosition());
}

void PostgresConnection::FinishCopyTo(PostgresCopyState &state) {
	if (state.format == PostgresCopyFormat::BINARY) {
		// binary copy requires a footer
		PostgresBinaryWriter writer(state);
		writer.WriteFooter();
		CopyData(writer);
	} else if (state.format == PostgresCopyFormat::TEXT) {
		// text copy requires a footer
		PostgresTextWriter writer(state);
		writer.WriteFooter();
		CopyData(writer);
	}

	auto result_code = PQputCopyEnd(GetConn(), nullptr);
	if (result_code != 1) {
		throw InternalException("Error during PQputCopyEnd: %s", PQerrorMessage(GetConn()));
	}
	// fetch the query result to check for errors
	PostgresResult pg_res(PQgetResult(GetConn()));
	auto result = pg_res.res;
	if (!result || PQresultStatus(result) != PGRES_COMMAND_OK) {
		string error_msg(PQresultErrorMessage(result));
		throw std::runtime_error("Failed to copy data: " + error_msg);
	}
}

bool NeedsQuotes(const string &to_quote, idx_t size) {
	// Check if the string contains list or struct specific characters, or if it's empty or starts/ends with whitespaces
	if (size <= 0) {
		// Always quote the empty string
		return true;
	}
	if (isspace(to_quote[0])) {
		// The string starts with whitespace, we need to preserve it
		return true;
	}
	if (isspace(to_quote[size - 1])) {
		// The string ends with whitespace, we need to preserve it
		return true;
	}
	for (idx_t c = 0; c < size; c++) {
		switch (to_quote[c]) {
		case '"':
		case '\\':
		case '{':
		case '}':
		case '(':
		case ')':
		case ',':
			return true;
		}
	}
	return false;
}

void EscapeQuotes(const string &to_escape, string &result, idx_t size) {
	// Escape quotes and backslashes so that the string can be quoted
	for (idx_t c = 0; c < size; c++) {
		switch (to_escape[c]) {
		case '"':
		case '\\':
			result += "\\";
		}
		result += to_escape[c];
	}
}

void QuoteAndEscapeIfNeeded(const string &to_quote, string &result, idx_t size) {
	// Quote the string iff it contains list or struct specific characters
	if (!NeedsQuotes(to_quote, size)) {
		result += to_quote;
		return;
	}
	result += '"';
	EscapeQuotes(to_quote, result, size);
	result += '"';
}

void CastToPostgresVarchar(ClientContext &context, Vector &input, Vector &result, idx_t size);

void CastListToPostgresArray(ClientContext &context, Vector &input, Vector &varchar_vector, idx_t size) {
	// cast child list
	auto &child_data = ListVector::GetEntry(input);
	auto child_count = ListVector::GetListSize(input);
	bool skip_quoting = child_data.GetType().id() == LogicalTypeId::LIST; // Do not quote dimensions in multi-D arrays
	Vector child_varchar(LogicalType::VARCHAR, child_count);
	CastToPostgresVarchar(context, child_data, child_varchar, child_count);

	// construct the list entries
	auto child_entries = FlatVector::GetData<string_t>(child_varchar);
	auto list_entries = FlatVector::GetData<list_entry_t>(input);
	auto result_entries = FlatVector::GetData<string_t>(varchar_vector);
	for (idx_t r = 0; r < size; r++) {
		if (FlatVector::IsNull(input, r)) {
			FlatVector::SetNull(varchar_vector, r, true);
			continue;
		}
		auto list_entry = list_entries[r];
		string result;
		result = "{";
		for (idx_t list_idx = 0; list_idx < list_entry.length; list_idx++) {
			if (list_idx > 0) {
				result += ",";
			}
			auto child_idx = list_entry.offset + list_idx;
			if (FlatVector::IsNull(child_varchar, child_idx)) {
				result += "NULL";
			} else {
				auto child = child_entries[child_idx];
				if (skip_quoting) {
					result += child.GetString();
				} else {
					QuoteAndEscapeIfNeeded(child.GetString(), result, child.GetSize());
				}
			}
		}
		result += "}";
		result_entries[r] = StringVector::AddString(varchar_vector, result);
	}
}

void CastStructToPostgres(ClientContext &context, Vector &input, Vector &varchar_vector, idx_t size) {
	auto &child_vectors = StructVector::GetEntries(input);
	// cast child data of structs
	vector<Vector> child_varchar_vectors;
	for (idx_t c = 0; c < child_vectors.size(); c++) {
		Vector child_varchar(LogicalType::VARCHAR, size);
		CastToPostgresVarchar(context, *child_vectors[c], child_varchar, size);
		child_varchar_vectors.push_back(std::move(child_varchar));
	}

	// construct the struct entries
	auto result_entries = FlatVector::GetData<string_t>(varchar_vector);
	for (idx_t r = 0; r < size; r++) {
		if (FlatVector::IsNull(input, r)) {
			FlatVector::SetNull(varchar_vector, r, true);
			continue;
		}
		string result;
		result = "(";
		for (idx_t c = 0; c < child_varchar_vectors.size(); c++) {
			if (c > 0) {
				result += ",";
			}
			if (FlatVector::IsNull(child_varchar_vectors[c], r)) {
				result += ""; // Struct literals encode null by omitting the value
			} else {
				auto child = FlatVector::GetData<string_t>(child_varchar_vectors[c])[r];
				QuoteAndEscapeIfNeeded(child.GetString(), result, child.GetSize());
			}
		}
		result += ")";
		result_entries[r] = StringVector::AddString(varchar_vector, result);
	}
}

void CastBlobToPostgres(ClientContext &context, Vector &input, Vector &result, idx_t size) {
	auto input_data = FlatVector::GetData<string_t>(input);
	auto result_data = FlatVector::GetData<string_t>(result);
	for (idx_t r = 0; r < size; r++) {
		if (FlatVector::IsNull(input, r)) {
			FlatVector::SetNull(result, r, true);
			continue;
		}
		const char *HEX_STRING = "0123456789ABCDEF";
		string blob_str = "\\x";
		auto blob_data = const_data_ptr_cast(input_data[r].GetData());
		auto blob_size = input_data[r].GetSize();
		for (idx_t c = 0; c < blob_size; c++) {
			blob_str += HEX_STRING[blob_data[c] / 16];
			blob_str += HEX_STRING[blob_data[c] % 16];
		}
		result_data[r] = StringVector::AddString(result, blob_str);
	}
}

void CastGeometryToPostgres(ClientContext &context, Vector &input, Vector &result, idx_t size) {
	// Cast to WKT format
	VectorOperations::Cast(context, input, result, size);
}

void CastToPostgresVarchar(ClientContext &context, Vector &input, Vector &result, idx_t size) {
	auto &type = input.GetType();
	switch (type.id()) {
	case LogicalTypeId::LIST:
		CastListToPostgresArray(context, input, result, size);
		break;
	case LogicalTypeId::STRUCT:
		CastStructToPostgres(context, input, result, size);
		break;
	case LogicalTypeId::GEOMETRY:
		CastGeometryToPostgres(context, input, result, size);
		break;
	case LogicalTypeId::BLOB:
		CastBlobToPostgres(context, input, result, size);
		break;
	default:
		VectorOperations::Cast(context, input, result, size);
		break;
	}
}

void PostgresConnection::CopyChunk(ClientContext &context, PostgresCopyState &state, DataChunk &chunk,
                                   DataChunk &varchar_chunk) {
	chunk.Flatten();

	if (state.format == PostgresCopyFormat::BINARY) {
		PostgresBinaryWriter writer(state);
		for (idx_t r = 0; r < chunk.size(); r++) {
			writer.BeginRow(chunk.ColumnCount());
			for (idx_t c = 0; c < chunk.ColumnCount(); c++) {
				auto &col = chunk.data[c];
				writer.WriteValue(col, r);
			}
			writer.FinishRow();
		}
		CopyData(writer);
	} else if (state.format == PostgresCopyFormat::TEXT) {
		// cast columns to varchar
		if (varchar_chunk.ColumnCount() == 0) {
			// not initialized yet
			vector<LogicalType> varchar_types;
			for (idx_t c = 0; c < chunk.ColumnCount(); c++) {
				varchar_types.push_back(LogicalType::VARCHAR);
			}
			varchar_chunk.Initialize(Allocator::DefaultAllocator(), varchar_types);
		} else {
			varchar_chunk.Reset();
		}
		D_ASSERT(chunk.ColumnCount() == varchar_chunk.ColumnCount());
		// for text format cast to varchar first
		for (idx_t c = 0; c < chunk.ColumnCount(); c++) {
			CastToPostgresVarchar(context, chunk.data[c], varchar_chunk.data[c], chunk.size());
		}
		varchar_chunk.SetCardinality(chunk.size());

		PostgresTextWriter writer(state);
		for (idx_t r = 0; r < chunk.size(); r++) {
			for (idx_t c = 0; c < chunk.ColumnCount(); c++) {
				if (c > 0) {
					writer.WriteSeparator();
				}
				D_ASSERT(varchar_chunk.data[c].GetType().id() == LogicalTypeId::VARCHAR);
				auto &col = varchar_chunk.data[c];
				writer.WriteValue(col, r);
			}
			writer.FinishRow();
		}
		CopyData(writer);
	}
}

} // namespace duckdb
