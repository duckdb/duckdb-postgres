#include "postgres_text_reader.hpp"
#include "postgres_scanner.hpp"
#include "duckdb/common/types/blob.hpp"

namespace duckdb {

PostgresTextReader::PostgresTextReader(ClientContext &context, PostgresConnection &con_p,
                                       const vector<column_t> &column_ids, const PostgresBindData &bind_data)
    : PostgresResultReader(con_p, column_ids, bind_data), context(context) {
}

PostgresTextReader::~PostgresTextReader() {
	Reset();
}

void PostgresTextReader::BeginCopy(const string &sql) {
	result = con.Query(sql);
	row_offset = 0;
}

struct PostgresListParser {
	PostgresListParser() : capacity(STANDARD_VECTOR_SIZE), size(0), vector(LogicalType::VARCHAR, capacity) {
	}

	void Initialize() {
	}

	void AddString(const string &str, bool &quoted) {
		if (size >= capacity) {
			vector.Resize(capacity, capacity * 2);
			capacity *= 2;
		}
		if (!quoted && str == "NULL") {
			FlatVector::SetNull(vector, size, true);
		} else {
			FlatVector::GetData<string_t>(vector)[size] = StringVector::AddStringOrBlob(vector, str);
		}
		size++;
		quoted = false;
	}

	void Finish() {
	}

	idx_t capacity;
	idx_t size;
	Vector vector;
};

struct PostgresStructParser {
	PostgresStructParser(ClientContext &context, idx_t child_count, idx_t row_count) {
		vector<LogicalType> child_varchar_types;
		for (idx_t c = 0; c < child_count; c++) {
			child_varchar_types.push_back(LogicalType::VARCHAR);
		}

		data.Initialize(context, child_varchar_types, row_count);
	}

	void Initialize() {
		column_offset = 0;
	}

	void AddString(const string &str, bool &quoted) {
		if (column_offset >= data.ColumnCount()) {
			throw InvalidInputException("Too many columns in data for parsing struct - string %s - expected %d", str,
			                            data.ColumnCount());
		}
		auto &col = data.data[column_offset];
		if (!quoted && str == "NULL") {
			FlatVector::SetNull(col, row_offset, true);
		} else {
			FlatVector::GetData<string_t>(col)[row_offset] = StringVector::AddStringOrBlob(col, str);
		}
		column_offset++;
	}

	void Finish() {
		if (column_offset != data.ColumnCount()) {
			throw InvalidInputException("Missing columns in data for parsing struct - expected %d but got %d",
			                            data.ColumnCount(), column_offset);
		}
		row_offset++;
	}

	DataChunk data;
	idx_t column_offset = 0;
	idx_t row_offset = 0;
};

struct PostgresCTIDParser {
	PostgresCTIDParser() {
	}

	void Initialize() {
	}

	void AddString(const string &str, bool &quoted) {
		values.push_back(StringUtil::ToUnsigned(str));
	}

	void Finish() {
		if (values.size() != 2) {
			throw InvalidInputException("CTID mismatch - expected (page_index, row_in_page)");
		}
	}

	vector<idx_t> values;
};

template <class T>
void ParsePostgresNested(T &parser, string_t list, char start, char end) {
	auto str = list.GetData();
	auto size = list.GetSize();
	if (size == 0 || str[0] != start || str[size - 1] != end) {
		throw InvalidInputException("Invalid Postgres list - expected %s...%s - got %s", string(1, start),
		                            string(1, end), list.GetString());
	}
	parser.Initialize();
	bool quoted = false;
	bool was_quoted = false;
	vector<char> delims;
	string current_string;
	for (idx_t i = 1; i < size - 1; i++) {
		auto c = str[i];
		if (quoted) {
			switch (c) {
			case '"':
				quoted = false;
				break;
			case '\\':
				// escape - directly add the next character to the string
				if (i + 1 < size) {
					current_string += str[i + 1];
				}
				// skip the next character
				i++;
				break;
			default:
				current_string += c;
				break;
			}
			continue;
		}
		switch (c) {
		case '{':
			delims.push_back('}');
			current_string += c;
			break;
		case '(':
			delims.push_back(')');
			current_string += c;
			break;
		case '}':
		case ')':
			if (delims.empty() || delims.back() != c) {
				throw InvalidInputException("Failed to convert list %s - mismatch in brackets", list.GetString());
			}
			delims.pop_back();
			current_string += c;
			break;
		case '"':
			quoted = true;
			was_quoted = true;
			break;
		case ',':
			if (!delims.empty()) {
				// in a nested struct
				current_string += c;
				break;
			}
			// next element
			if (!current_string.empty() || was_quoted) {
				parser.AddString(current_string, was_quoted);
			}
			current_string = string();
			break;
		default:
			current_string += c;
		}
	}
	if (!current_string.empty() || was_quoted) {
		parser.AddString(current_string, was_quoted);
	}
	parser.Finish();
}

void ParsePostgresList(PostgresListParser &list_parser, string_t list) {
	ParsePostgresNested(list_parser, list, '{', '}');
}

void ParsePostgresStruct(PostgresStructParser &struct_parser, string_t list) {
	ParsePostgresNested(struct_parser, list, '(', ')');
}

void ParsePostgresCTID(PostgresCTIDParser &ctid_parser, string_t list) {
	ParsePostgresNested(ctid_parser, list, '(', ')');
}

void PostgresTextReader::ConvertList(Vector &source, Vector &target, const PostgresType &postgres_type, idx_t count) {
	// lists have the format {1, 2, 3}
	UnifiedVectorFormat vdata;
	source.ToUnifiedFormat(count, vdata);

	auto strings = UnifiedVectorFormat::GetData<string_t>(vdata);
	auto list_data = FlatVector::GetData<list_entry_t>(target);

	PostgresListParser list_parser;
	for (idx_t i = 0; i < count; i++) {
		if (!vdata.validity.RowIsValid(i)) {
			// NULL value - skip
			FlatVector::SetNull(target, i, true);
			continue;
		}
		list_data[i].offset = list_parser.size;
		ParsePostgresList(list_parser, strings[i]);
		list_data[i].length = list_parser.size - list_data[i].offset;
	}
	if (list_parser.size > 0) {
		auto &target_child = ListVector::GetEntry(target);
		ListVector::Reserve(target, list_parser.size);
		ConvertVector(list_parser.vector, target_child,
		              postgres_type.children.empty() ? PostgresType() : postgres_type.children[0], list_parser.size);
	}
	ListVector::SetListSize(target, list_parser.size);
}

void PostgresTextReader::ConvertStruct(Vector &source, Vector &target, const PostgresType &postgres_type, idx_t count) {
	// structs have the format (1, 2, 3)
	UnifiedVectorFormat vdata;
	source.ToUnifiedFormat(count, vdata);
	auto strings = UnifiedVectorFormat::GetData<string_t>(vdata);
	auto &children = StructVector::GetEntries(target);

	PostgresStructParser struct_parser(context, children.size(), count);
	for (idx_t i = 0; i < count; i++) {
		if (!vdata.validity.RowIsValid(i)) {
			// NULL value - skip
			FlatVector::SetNull(target, i, true);
			for (idx_t c = 0; c < children.size(); c++) {
				FlatVector::SetNull(struct_parser.data.data[c], i, true);
			}
			continue;
		}
		ParsePostgresStruct(struct_parser, strings[i]);
	}
	for (idx_t c = 0; c < children.size(); c++) {
		ConvertVector(struct_parser.data.data[c], *children[c],
		              c >= postgres_type.children.size() ? PostgresType() : postgres_type.children[c], count);
	}
}

void PostgresTextReader::ConvertCTID(Vector &source, Vector &target, idx_t count) {
	// ctids have the format (page_index, row_in_page)
	UnifiedVectorFormat vdata;
	source.ToUnifiedFormat(count, vdata);
	auto strings = UnifiedVectorFormat::GetData<string_t>(vdata);
	auto result = FlatVector::GetData<int64_t>(target);

	for (idx_t i = 0; i < count; i++) {
		if (!vdata.validity.RowIsValid(i)) {
			// NULL value - skip
			FlatVector::SetNull(target, i, true);
			continue;
		}
		PostgresCTIDParser ctid_parser;
		ParsePostgresCTID(ctid_parser, strings[i]);
		auto page_index = ctid_parser.values[0];
		auto row_in_page = ctid_parser.values[1];
		result[i] = NumericCast<int64_t>((page_index << 16LL) + row_in_page);
	}
}

void PostgresTextReader::ConvertBlob(Vector &source, Vector &target, idx_t count) {
	// ctids have the format (page_index, row_in_page)
	UnifiedVectorFormat vdata;
	source.ToUnifiedFormat(count, vdata);
	auto strings = UnifiedVectorFormat::GetData<string_t>(vdata);
	auto result = FlatVector::GetData<string_t>(target);

	for (idx_t i = 0; i < count; i++) {
		if (!vdata.validity.RowIsValid(i)) {
			// NULL value - skip
			FlatVector::SetNull(target, i, true);
			continue;
		}
		auto blob_str = strings[i];
		auto str = blob_str.GetData();
		auto size = blob_str.GetSize();
		if (size < 2 || str[0] != '\\' || str[1] != 'x') {
			throw InvalidInputException("Incorrect blob format - expected \\x... for blob");
		}
		if (size % 2 != 0) {
			throw InvalidInputException("Blob size must be modulo 2 (\\xAA)");
		}
		string result_blob;
		for (idx_t i = 2; i < size; i += 2) {
			int byte_a = Blob::HEX_MAP[static_cast<uint8_t>(str[i])];
			int byte_b = Blob::HEX_MAP[static_cast<uint8_t>(str[i + 1])];
			result_blob += UnsafeNumericCast<data_t>((byte_a << 4) + byte_b);
		}
		result[i] = StringVector::AddStringOrBlob(target, result_blob);
	}
}

void PostgresTextReader::ConvertVector(Vector &source, Vector &target, const PostgresType &postgres_type, idx_t count) {
	if (source.GetType().id() != LogicalTypeId::VARCHAR) {
		throw InternalException("Source needs to be VARCHAR");
	}
	if (postgres_type.info == PostgresTypeAnnotation::CTID) {
		ConvertCTID(source, target, count);
		return;
	}
	switch (target.GetType().id()) {
	case LogicalTypeId::LIST:
		ConvertList(source, target, postgres_type, count);
		break;
	case LogicalTypeId::STRUCT:
		ConvertStruct(source, target, postgres_type, count);
		break;
	case LogicalTypeId::BLOB:
		ConvertBlob(source, target, count);
		break;
	default:
		VectorOperations::Cast(context, source, target, count);
	}
}

PostgresReadResult PostgresTextReader::Read(DataChunk &output) {
	if (!result) {
		return PostgresReadResult::FINISHED;
	}
	if (scan_chunk.data.empty()) {
		// initialize the scan chunk
		vector<LogicalType> types;
		for (idx_t i = 0; i < output.ColumnCount(); i++) {
			types.push_back(LogicalType::VARCHAR);
		}
		scan_chunk.Initialize(context, types);
	}
	scan_chunk.Reset();
	for (; scan_chunk.size() < STANDARD_VECTOR_SIZE && row_offset < result->Count(); row_offset++) {
		idx_t output_offset = scan_chunk.size();
		for (idx_t output_idx = 0; output_idx < output.ColumnCount(); output_idx++) {
			auto col_idx = column_ids[output_idx];
			auto &out_vec = scan_chunk.data[output_idx];
			if (result->IsNull(row_offset, output_idx)) {
				FlatVector::SetNull(out_vec, output_offset, true);
				continue;
			}
			auto col_data = FlatVector::GetData<string_t>(out_vec);
			col_data[output_offset] =
			    StringVector::AddStringOrBlob(out_vec, result->GetStringRef(row_offset, output_idx));
		}
		scan_chunk.SetCardinality(scan_chunk.size() + 1);
	}
	for (idx_t c = 0; c < output.ColumnCount(); c++) {
		auto col_idx = column_ids[c];
		if (col_idx == COLUMN_IDENTIFIER_ROW_ID) {
			PostgresType ctid_type;
			ctid_type.info = PostgresTypeAnnotation::CTID;
			ConvertVector(scan_chunk.data[c], output.data[c], ctid_type, scan_chunk.size());
		} else {
			ConvertVector(scan_chunk.data[c], output.data[c], bind_data.postgres_types[c], scan_chunk.size());
		}
	}
	output.SetCardinality(scan_chunk.size());

	bool finished = row_offset >= result->Count();
	if (finished) {
		// The result set is fully consumed. Reset immediately to free the PGresult.
		Reset();
		return PostgresReadResult::FINISHED;
	}
	return PostgresReadResult::HAVE_MORE_TUPLES;
}

void PostgresTextReader::Reset() {
	result.reset();
	row_offset = 0;
}

} // namespace duckdb
