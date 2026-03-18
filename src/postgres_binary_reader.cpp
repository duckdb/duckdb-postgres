#include "postgres_binary_reader.hpp"
#include "postgres_scanner.hpp"

namespace duckdb {

PostgresBinaryReader::PostgresBinaryReader(PostgresConnection &con_p, const vector<column_t> &column_ids,
                                           const PostgresBindData &bind_data)
    : PostgresResultReader(con_p, column_ids, bind_data) {
}

PostgresBinaryReader::~PostgresBinaryReader() {
	Reset();
}

void PostgresBinaryReader::BeginCopy(ClientContext &context, const string &sql) {
	con.BeginCopyFrom(context, sql, PGRES_COPY_OUT);
	if (!Next()) {
		throw IOException("Failed to fetch header for COPY \"%s\"", sql);
	}
	CheckHeader();
}

PostgresReadResult PostgresBinaryReader::Read(DataChunk &output) {
	while (output.size() < STANDARD_VECTOR_SIZE) {
		while (!Ready()) {
			if (!Next()) {
				// finished this batch
				return PostgresReadResult::FINISHED;
			}
		}

		// read a row
		auto tuple_count = ReadInteger<int16_t>();
		if (tuple_count <= 0) {
			// tuple_count of -1 signifies the file trailer (i.e. footer) - reset and skip
			Reset();
			continue;
		}

		D_ASSERT(tuple_count == column_ids.size());

		idx_t output_offset = output.size();
		for (idx_t output_idx = 0; output_idx < output.ColumnCount(); output_idx++) {
			auto col_idx = column_ids[output_idx];
			auto &out_vec = output.data[output_idx];
			if (col_idx == COLUMN_IDENTIFIER_ROW_ID) {
				// row id
				// ctid in postgres are a composite type of (page_index, tuple_in_page)
				// the page index is a 4-byte integer, the tuple_in_page a 2-byte integer
				PostgresType ctid_type;
				ctid_type.info = PostgresTypeAnnotation::CTID;
				ReadValue(LogicalType::BIGINT, ctid_type, out_vec, output_offset);
			} else {
				ReadValue(bind_data.types[col_idx], bind_data.postgres_types[col_idx], out_vec, output_offset);
			}
		}
		Reset();
		output.SetCardinality(output_offset + 1);
	}
	// we filled a chunk
	return PostgresReadResult::HAVE_MORE_TUPLES;
}

bool PostgresBinaryReader::Next() {
	Reset();
	char *out_buffer;
	int len = PQgetCopyData(con.GetConn(), &out_buffer, 0);
	auto new_buffer = data_ptr_cast(out_buffer);

	// len -1 signals end
	if (len == -1) {
		// consume all available results
		while (true) {
			PostgresResult pg_res(PQgetResult(con.GetConn()));
			auto final_result = pg_res.res;
			if (!final_result) {
				break;
			}
			if (PQresultStatus(final_result) != PGRES_COMMAND_OK) {
				throw IOException("Failed to fetch header for COPY: %s", string(PQresultErrorMessage(final_result)));
			}
		}
		return false;
	}

	// len -2 is error
	// we expect at least 2 bytes in each message for the tuple count
	if (!new_buffer || len < sizeof(int16_t)) {
		throw IOException("Unable to read binary COPY data from Postgres: %s", string(PQerrorMessage(con.GetConn())));
	}
	buffer = new_buffer;
	buffer_ptr = buffer;
	end = buffer + len;
	return true;
}

void PostgresBinaryReader::Reset() {
	if (buffer) {
		PQfreemem(buffer);
	}
	buffer = nullptr;
	buffer_ptr = nullptr;
	end = nullptr;
}

bool PostgresBinaryReader::Ready() {
	return buffer_ptr != nullptr;
}

void PostgresBinaryReader::CheckHeader() {
	auto magic_len = PostgresConversion::COPY_HEADER_LENGTH;
	auto flags_len = 8;
	auto header_len = magic_len + flags_len;

	if (!buffer_ptr) {
		throw IOException("buffer_ptr not set in CheckHeader");
	}
	if (buffer_ptr + header_len >= end) {
		throw IOException("Unable to read binary COPY data from Postgres, invalid header");
	}
	if (memcmp(buffer_ptr, PostgresConversion::COPY_HEADER, magic_len) != 0) {
		throw IOException("Expected Postgres binary COPY header, got something else");
	}
	buffer_ptr += header_len;
	// as far as i can tell the "Flags field" and the "Header
	// extension area length" do not contain anything interesting
}

PostgresDecimalConfig PostgresBinaryReader::ReadDecimalConfig() {
	PostgresDecimalConfig config;
	config.ndigits = ReadInteger<uint16_t>();
	config.weight = ReadInteger<int16_t>();
	auto sign = ReadInteger<uint16_t>();

	if (!(sign == NUMERIC_POS || sign == NUMERIC_NAN || sign == NUMERIC_PINF || sign == NUMERIC_NINF ||
	      sign == NUMERIC_NEG)) {
		throw NotImplementedException("Postgres numeric NA/Inf");
	}
	config.is_negative = sign == NUMERIC_NEG;
	config.scale = ReadInteger<uint16_t>();

	return config;
}

void PostgresBinaryReader::ReadGeometry(const LogicalType &type, const PostgresType &postgres_type, Vector &out_vec,
                                        idx_t output_offset) {
	idx_t element_count = 0;
	switch (postgres_type.info) {
	case PostgresTypeAnnotation::GEOM_LINE:
	case PostgresTypeAnnotation::GEOM_CIRCLE:
		element_count = 3;
		break;
	case PostgresTypeAnnotation::GEOM_LINE_SEGMENT:
	case PostgresTypeAnnotation::GEOM_BOX:
		element_count = 4;
		break;
	case PostgresTypeAnnotation::GEOM_PATH: {
		// variable number of elements
		auto path_is_closed = ReadBoolean(); // ignored for now
		element_count = 2 * ReadInteger<uint32_t>();
		break;
	}
	case PostgresTypeAnnotation::GEOM_POLYGON:
		// variable number of elements
		element_count = 2 * ReadInteger<uint32_t>();
		break;
	default:
		throw InternalException("Unsupported type for ReadGeometry");
	}
	auto list_entries = FlatVector::GetData<list_entry_t>(out_vec);
	auto child_offset = ListVector::GetListSize(out_vec);
	ListVector::Reserve(out_vec, child_offset + element_count);
	list_entries[output_offset].offset = child_offset;
	list_entries[output_offset].length = element_count;
	auto &child_vector = ListVector::GetEntry(out_vec);
	auto child_data = FlatVector::GetData<double>(child_vector);
	for (idx_t i = 0; i < element_count; i++) {
		child_data[child_offset + i] = ReadDouble();
	}
	ListVector::SetListSize(out_vec, child_offset + element_count);
}

void PostgresBinaryReader::ReadArray(const LogicalType &type, const PostgresType &postgres_type, Vector &out_vec,
                                     idx_t output_offset, uint32_t current_count, uint32_t dimensions[],
                                     uint32_t ndim) {
	auto list_entries = FlatVector::GetData<list_entry_t>(out_vec);
	auto child_offset = ListVector::GetListSize(out_vec);
	auto child_dimension = dimensions[0];
	auto child_count = current_count * child_dimension;
	// set up the list entries for this dimension
	auto current_offset = child_offset;
	for (idx_t c = 0; c < current_count; c++) {
		auto &list_entry = list_entries[output_offset + c];
		list_entry.offset = current_offset;
		list_entry.length = child_dimension;
		current_offset += child_dimension;
	}
	ListVector::Reserve(out_vec, child_offset + child_count);
	auto &child_vec = ListVector::GetEntry(out_vec);
	auto &child_type = ListType::GetChildType(type);
	auto &child_pg_type = postgres_type.children[0];
	if (ndim > 1) {
		// there are more dimensions to read - recurse into child list
		ReadArray(child_type, child_pg_type, child_vec, child_offset, child_count, dimensions + 1, ndim - 1);
	} else {
		// this is the last level - read the actual values
		for (idx_t child_idx = 0; child_idx < child_count; child_idx++) {
			ReadValue(child_type, child_pg_type, child_vec, child_offset + child_idx);
		}
	}
	ListVector::SetListSize(out_vec, child_offset + child_count);
}

void PostgresBinaryReader::ReadValue(const LogicalType &type, const PostgresType &postgres_type, Vector &out_vec,
                                     idx_t output_offset) {
	auto value_len = ReadInteger<int32_t>();
	if (value_len == -1) { // NULL
		FlatVector::SetNull(out_vec, output_offset, true);
		return;
	}
	switch (type.id()) {
	case LogicalTypeId::SMALLINT:
		D_ASSERT(value_len == sizeof(int16_t));
		FlatVector::GetData<int16_t>(out_vec)[output_offset] = ReadInteger<int16_t>();
		break;
	case LogicalTypeId::INTEGER:
		D_ASSERT(value_len == sizeof(int32_t));
		FlatVector::GetData<int32_t>(out_vec)[output_offset] = ReadInteger<int32_t>();
		break;
	case LogicalTypeId::UINTEGER:
		D_ASSERT(value_len == sizeof(uint32_t));
		FlatVector::GetData<uint32_t>(out_vec)[output_offset] = ReadInteger<uint32_t>();
		break;
	case LogicalTypeId::BIGINT:
		if (postgres_type.info == PostgresTypeAnnotation::CTID) {
			D_ASSERT(value_len == 6);
			int64_t page_index = ReadInteger<int32_t>();
			int64_t row_in_page = ReadInteger<int16_t>();
			FlatVector::GetData<int64_t>(out_vec)[output_offset] = (page_index << 16LL) + row_in_page;
			return;
		}
		D_ASSERT(value_len == sizeof(int64_t));
		FlatVector::GetData<int64_t>(out_vec)[output_offset] = ReadInteger<int64_t>();
		break;
	case LogicalTypeId::FLOAT:
		D_ASSERT(value_len == sizeof(float));
		FlatVector::GetData<float>(out_vec)[output_offset] = ReadFloat();
		break;
	case LogicalTypeId::DOUBLE: {
		// this was an unbounded decimal, read params from value and cast to double
		if (postgres_type.info == PostgresTypeAnnotation::NUMERIC_AS_DOUBLE) {
			FlatVector::GetData<double>(out_vec)[output_offset] = ReadDecimal<double, DecimalConversionDouble>();
			break;
		}
		D_ASSERT(value_len == sizeof(double));
		FlatVector::GetData<double>(out_vec)[output_offset] = ReadDouble();
		break;
	}

	case LogicalTypeId::BLOB:
	case LogicalTypeId::VARCHAR: {
		if (postgres_type.info == PostgresTypeAnnotation::JSONB) {
			auto version = ReadInteger<uint8_t>();
			value_len--;
			if (version != 1) {
				throw NotImplementedException("JSONB version number mismatch, expected 1, got %d", version);
			}
		}
		auto str = ReadString(value_len);
		if (postgres_type.info == PostgresTypeAnnotation::FIXED_LENGTH_CHAR) {
			// CHAR column - remove trailing spaces
			while (value_len > 0 && str[value_len - 1] == ' ') {
				value_len--;
			}
		}
		FlatVector::GetData<string_t>(out_vec)[output_offset] = StringVector::AddStringOrBlob(out_vec, str, value_len);
		break;
	}
	case LogicalTypeId::GEOMETRY: {
		const auto str = ReadString(value_len);

		string_t res_val;
		if (!Geometry::FromBinary(string_t(str, value_len), res_val, out_vec, true)) {
			throw InvalidInputException("Failed to parse Postgres geometry data");
		}
		FlatVector::GetData<string_t>(out_vec)[output_offset] = res_val;
		break;
	}
	case LogicalTypeId::BOOLEAN:
		D_ASSERT(value_len == sizeof(bool));
		FlatVector::GetData<bool>(out_vec)[output_offset] = ReadBoolean();
		break;
	case LogicalTypeId::DECIMAL: {
		if (value_len < sizeof(uint16_t) * 4) {
			throw InvalidInputException("Need at least 8 bytes to read a Postgres decimal. Got %d", value_len);
		}
		switch (type.InternalType()) {
		case PhysicalType::INT16:
			FlatVector::GetData<int16_t>(out_vec)[output_offset] = ReadDecimal<int16_t>();
			break;
		case PhysicalType::INT32:
			FlatVector::GetData<int32_t>(out_vec)[output_offset] = ReadDecimal<int32_t>();
			break;
		case PhysicalType::INT64:
			FlatVector::GetData<int64_t>(out_vec)[output_offset] = ReadDecimal<int64_t>();
			break;
		case PhysicalType::INT128:
			FlatVector::GetData<hugeint_t>(out_vec)[output_offset] = ReadDecimal<hugeint_t, DecimalConversionHugeint>();
			break;
		default:
			throw InvalidInputException("Unsupported decimal storage type");
		}
		break;
	}

	case LogicalTypeId::DATE: {
		D_ASSERT(value_len == sizeof(int32_t));
		auto out_ptr = FlatVector::GetData<date_t>(out_vec);
		out_ptr[output_offset] = ReadDate();
		break;
	}
	case LogicalTypeId::TIME: {
		D_ASSERT(value_len == sizeof(int64_t));
		FlatVector::GetData<dtime_t>(out_vec)[output_offset] = ReadTime();
		break;
	}
	case LogicalTypeId::TIME_TZ: {
		D_ASSERT(value_len == sizeof(int64_t) + sizeof(int32_t));
		FlatVector::GetData<dtime_tz_t>(out_vec)[output_offset] = ReadTimeTZ();
		break;
	}
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP: {
		D_ASSERT(value_len == sizeof(int64_t));
		FlatVector::GetData<timestamp_t>(out_vec)[output_offset] = ReadTimestamp();
		break;
	}
	case LogicalTypeId::ENUM: {
		auto enum_val = string(ReadString(value_len), value_len);
		auto offset = EnumType::GetPos(type, enum_val);
		if (offset < 0) {
			throw IOException("Could not map ENUM value %s", enum_val);
		}
		switch (type.InternalType()) {
		case PhysicalType::UINT8:
			FlatVector::GetData<uint8_t>(out_vec)[output_offset] = (uint8_t)offset;
			break;
		case PhysicalType::UINT16:
			FlatVector::GetData<uint16_t>(out_vec)[output_offset] = (uint16_t)offset;
			break;

		case PhysicalType::UINT32:
			FlatVector::GetData<uint32_t>(out_vec)[output_offset] = (uint32_t)offset;
			break;

		default:
			throw InternalException("ENUM can only have unsigned integers (except "
			                        "UINT64) as physical types, got %s",
			                        TypeIdToString(type.InternalType()));
		}
		break;
	}
	case LogicalTypeId::INTERVAL: {
		FlatVector::GetData<interval_t>(out_vec)[output_offset] = ReadInterval();
		break;
	}
	case LogicalTypeId::UUID: {
		D_ASSERT(value_len == 2 * sizeof(int64_t));
		FlatVector::GetData<hugeint_t>(out_vec)[output_offset] = ReadUUID();
		break;
	}
	case LogicalTypeId::LIST: {
		auto &list_entry = FlatVector::GetData<list_entry_t>(out_vec)[output_offset];
		auto child_offset = ListVector::GetListSize(out_vec);

		if (value_len < 1) {
			list_entry.offset = child_offset;
			list_entry.length = 0;
			break;
		}
		switch (postgres_type.info) {
		case PostgresTypeAnnotation::GEOM_LINE:
		case PostgresTypeAnnotation::GEOM_LINE_SEGMENT:
		case PostgresTypeAnnotation::GEOM_BOX:
		case PostgresTypeAnnotation::GEOM_PATH:
		case PostgresTypeAnnotation::GEOM_POLYGON:
		case PostgresTypeAnnotation::GEOM_CIRCLE:
			ReadGeometry(type, postgres_type, out_vec, output_offset);
			return;
		default:
			break;
		}
		D_ASSERT(value_len >= 3 * sizeof(uint32_t));
		auto array_dim = ReadInteger<uint32_t>();
		auto array_has_null = ReadInteger<uint32_t>(); // whether or not the array has nulls - ignore
		auto value_oid = ReadInteger<uint32_t>();      // value_oid - not necessary
		if (array_dim == 0) {
			list_entry.offset = child_offset;
			list_entry.length = 0;
			return;
		}
		// verify the number of dimensions matches the expected number of dimensions
		idx_t expected_dimensions = 0;
		const_reference<LogicalType> current_type = type;
		while (current_type.get().id() == LogicalTypeId::LIST) {
			current_type = ListType::GetChildType(current_type.get());
			expected_dimensions++;
		}
		if (expected_dimensions != array_dim) {
			throw InvalidInputException(
			    "Expected an array with %llu dimensions, but this array has %llu dimensions. The array stored in "
			    "Postgres does not match the schema. Postgres does not enforce that arrays match the provided "
			    "schema but DuckDB requires this.\nSet pg_array_as_varchar=true to read the array as a varchar "
			    "instead.",
			    expected_dimensions, array_dim);
		}
		auto dimensions = unique_ptr<uint32_t[]>(new uint32_t[array_dim]);
		for (idx_t d = 0; d < array_dim; d++) {
			dimensions[d] = ReadInteger<uint32_t>();
			auto lb = ReadInteger<uint32_t>(); // index lower bounds for each dimension -- we don't need them
		}
		// read the arrays recursively
		ReadArray(type, postgres_type, out_vec, output_offset, 1, dimensions.get(), array_dim);
		break;
	}
	case LogicalTypeId::STRUCT: {
		auto &child_entries = StructVector::GetEntries(out_vec);
		if (postgres_type.info == PostgresTypeAnnotation::GEOM_POINT) {
			D_ASSERT(value_len == sizeof(double) * 2);
			FlatVector::GetData<double>(*child_entries[0])[output_offset] = ReadDouble();
			FlatVector::GetData<double>(*child_entries[1])[output_offset] = ReadDouble();
			break;
		}
		auto entry_count = ReadInteger<uint32_t>();
		if (entry_count != child_entries.size()) {
			throw InternalException("Mismatch in entry count: expected %d but got %d", child_entries.size(),
			                        entry_count);
		}
		for (idx_t c = 0; c < entry_count; c++) {
			auto &child = *child_entries[c];
			auto value_oid = ReadInteger<uint32_t>();
			ReadValue(child.GetType(), postgres_type.children[c], child, output_offset);
		}
		break;
	}
	default:
		throw InternalException("Unsupported Type %s", type.ToString());
	}
}

} // namespace duckdb
