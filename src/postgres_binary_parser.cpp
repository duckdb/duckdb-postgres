#include "postgres_binary_parser.hpp"
#include "duckdb/common/types/geometry.hpp"

#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"

namespace duckdb {

PostgresBinaryParser::PostgresBinaryParser(vector<LogicalType> types_p, vector<PostgresType> postgres_types_p,
                                           PostgresTypeConfig type_config_p)
    : types(std::move(types_p)), postgres_types(std::move(postgres_types_p)), type_config(type_config_p) {
}

void PostgresBinaryParser::SetBuffer(data_ptr_t buf, idx_t len) {
	buffer_ptr = buf;
	end = buf + len;
}

bool PostgresBinaryParser::ReadChunk(DataChunk &output, const vector<column_t> &column_ids) {
	while (output.size() < STANDARD_VECTOR_SIZE) {
		if (!Ready()) {
			return false;
		}

		auto field_count = ReadInteger<int16_t>();
		if (field_count <= 0) {
			// a field count of -1 signifies the file trailer (i.e. footer)
			// clear the buffer so Ready() returns false and the caller can free it
			buffer_ptr = nullptr;
			end = nullptr;
			continue;
		}

		if (field_count != column_ids.size()) {
			throw InvalidInputException("Postgres binary reader - expected %llu field(s) per row, got %d",
			                            column_ids.size(), field_count);
		}

		idx_t output_offset = output.size();
		for (idx_t output_idx = 0; output_idx < output.ColumnCount(); output_idx++) {
			auto col_idx = column_ids[output_idx];
			auto &out_vec = output.data[output_idx];
			if (col_idx == COLUMN_IDENTIFIER_ROW_ID) {
				PostgresType ctid_type;
				ctid_type.info = PostgresTypeAnnotation::CTID;
				ReadValue(LogicalType::BIGINT, ctid_type, out_vec, output_offset);
			} else {
				ReadValue(types[col_idx], postgres_types[col_idx], out_vec, output_offset);
			}
		}
		output.SetChildCardinality(output_offset + 1);
	}
	return true;
}

void PostgresBinaryParser::CheckHeader() {
	auto magic_len = PostgresConversion::COPY_HEADER_LENGTH;
	auto flags_len = 8;
	auto header_len = magic_len + flags_len;

	if (!buffer_ptr) {
		throw IOException("buffer_ptr not set in CheckHeader");
	}
	// a buffer holding exactly the header is valid - the reader hands one over when the first row does
	// not fit alongside it
	if (buffer_ptr + header_len > end) {
		throw IOException("Unable to read binary COPY data, invalid header");
	}
	if (memcmp(buffer_ptr, PostgresConversion::COPY_HEADER, magic_len) != 0) {
		throw IOException("Expected Postgres binary COPY header, got something else");
	}
	buffer_ptr += header_len;
	// as far as i can tell the "Flags field" and the "Header
	// extension area length" do not contain anything interesting
}

PostgresDecimalConfig PostgresBinaryParser::ReadDecimalConfig() {
	PostgresDecimalConfig config;
	config.ndigits = ReadInteger<uint16_t>();
	config.weight = ReadInteger<int16_t>();
	auto sign = ReadInteger<uint16_t>();

	if (!(sign == NUMERIC_POS || sign == NUMERIC_NAN || sign == NUMERIC_PINF || sign == NUMERIC_NINF ||
	      sign == NUMERIC_NEG)) {
		throw NotImplementedException("Postgres numeric NA/Inf");
	}
	config.sign = sign;
	config.is_negative = sign == NUMERIC_NEG;
	config.scale = ReadInteger<uint16_t>();

	return config;
}

void PostgresBinaryParser::ReadGeometry(const LogicalType &type, const PostgresType &postgres_type, Vector &out_vec,
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
		auto path_is_closed = ReadBoolean();
		element_count = 2 * ReadInteger<uint32_t>();
		break;
	}
	case PostgresTypeAnnotation::GEOM_POLYGON:
		element_count = 2 * ReadInteger<uint32_t>();
		break;
	default:
		throw InternalException("Unsupported type for ReadGeometry");
	}
	auto list_entries = FlatVector::GetDataMutable<list_entry_t>(out_vec);
	auto child_offset = ListVector::GetListSize(out_vec);
	ListVector::Reserve(out_vec, child_offset + element_count);
	list_entries[output_offset].offset = child_offset;
	list_entries[output_offset].length = element_count;
	auto &child_vector = ListVector::GetChildMutable(out_vec);
	auto child_data = FlatVector::GetDataMutable<double>(child_vector);
	for (idx_t i = 0; i < element_count; i++) {
		child_data[child_offset + i] = ReadDouble();
	}
	ListVector::SetListSize(out_vec, child_offset + element_count);
}

void PostgresBinaryParser::ReadArray(const LogicalType &type, const PostgresType &postgres_type, Vector &out_vec,
                                     idx_t output_offset, uint32_t current_count, uint32_t dimensions[],
                                     uint32_t ndim) {
	auto list_entries = FlatVector::GetDataMutable<list_entry_t>(out_vec);
	auto child_offset = ListVector::GetListSize(out_vec);
	auto child_dimension = dimensions[0];
	auto child_count = current_count * child_dimension;
	auto current_offset = child_offset;
	for (idx_t c = 0; c < current_count; c++) {
		auto &list_entry = list_entries[output_offset + c];
		list_entry.offset = current_offset;
		list_entry.length = child_dimension;
		current_offset += child_dimension;
	}
	ListVector::Reserve(out_vec, child_offset + child_count);
	auto &child_vec = ListVector::GetChildMutable(out_vec);
	auto &child_type = ListType::GetChildType(type);
	auto &child_pg_type = postgres_type.children[0];
	if (ndim > 1) {
		ReadArray(child_type, child_pg_type, child_vec, child_offset, child_count, dimensions + 1, ndim - 1);
	} else {
		for (idx_t child_idx = 0; child_idx < child_count; child_idx++) {
			ReadValue(child_type, child_pg_type, child_vec, child_offset + child_idx);
		}
	}
	ListVector::SetListSize(out_vec, child_offset + child_count);
}

// Check that the length of the value Postgres sent matches the length we expect. COPY format only
// tells us the length of the value, so this is the only check we have that the data matches the
// type we expect.
static void CheckValueLength(int32_t value_len, idx_t expected_len, const LogicalType &type) {
	if (value_len != expected_len) {
		throw InvalidInputException("Postgres binary reader - expected %llu bytes for a value of type %s, got %d",
		                            expected_len, type.ToString(), value_len);
	}
}

void PostgresBinaryParser::ReadValue(const LogicalType &type, const PostgresType &postgres_type, Vector &out_vec,
                                     idx_t output_offset) {
	auto value_len = ReadInteger<int32_t>();
	if (value_len == -1) { // NULL
		FlatVector::SetNull(out_vec, output_offset, true);
		return;
	}
	switch (type.id()) {
	case LogicalTypeId::SMALLINT:
		CheckValueLength(value_len, sizeof(int16_t), type);
		FlatVector::GetDataMutable<int16_t>(out_vec)[output_offset] = ReadInteger<int16_t>();
		break;
	case LogicalTypeId::INTEGER:
		CheckValueLength(value_len, sizeof(int32_t), type);
		FlatVector::GetDataMutable<int32_t>(out_vec)[output_offset] = ReadInteger<int32_t>();
		break;
	case LogicalTypeId::UINTEGER:
		CheckValueLength(value_len, sizeof(uint32_t), type);
		FlatVector::GetDataMutable<uint32_t>(out_vec)[output_offset] = ReadInteger<uint32_t>();
		break;
	case LogicalTypeId::BIGINT:
		if (postgres_type.info == PostgresTypeAnnotation::CTID) {
			CheckValueLength(value_len, sizeof(int32_t) + sizeof(int16_t), type);
			int64_t page_index = ReadInteger<int32_t>();
			int64_t row_in_page = ReadInteger<int16_t>();
			FlatVector::GetDataMutable<int64_t>(out_vec)[output_offset] = (page_index << 16LL) + row_in_page;
			return;
		}
		CheckValueLength(value_len, sizeof(int64_t), type);
		FlatVector::GetDataMutable<int64_t>(out_vec)[output_offset] = ReadInteger<int64_t>();
		break;
	case LogicalTypeId::FLOAT:
		CheckValueLength(value_len, sizeof(float), type);
		FlatVector::GetDataMutable<float>(out_vec)[output_offset] = ReadFloat();
		break;
	case LogicalTypeId::DOUBLE: {
		if (postgres_type.info == PostgresTypeAnnotation::NUMERIC_AS_DOUBLE) {
			PostgresDecimal<double> dec = ReadDecimal<double, DecimalConversionDouble>();
			double double_value = 0;
			switch (dec.kind) {
			case PostgresDecimalKind::ORDINARY:
				double_value = dec.value;
				break;
			case PostgresDecimalKind::NOT_A_NUMBER:
				double_value = std::numeric_limits<double>::quiet_NaN();
				break;
			case PostgresDecimalKind::POSITIVE_INFINITY:
				double_value = std::numeric_limits<double>::infinity();
				break;
			case PostgresDecimalKind::NEGATIVE_INFINITY:
				double_value = -std::numeric_limits<double>::infinity();
				break;
			default:
				throw InvalidInputException("Unsupported decimal kind");
			}
			FlatVector::GetDataMutable<double>(out_vec)[output_offset] = double_value;
			break;
		}
		CheckValueLength(value_len, sizeof(double), type);
		FlatVector::GetDataMutable<double>(out_vec)[output_offset] = ReadDouble();
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
			while (value_len > 0 && str[value_len - 1] == ' ') {
				value_len--;
			}
		}
		FlatVector::GetDataMutable<string_t>(out_vec)[output_offset] =
		    StringVector::AddStringOrBlob(out_vec, str, value_len);
		break;
	}
	case LogicalTypeId::GEOMETRY: {
		const auto str = ReadString(value_len);

		string_t res_val;
		auto &string_heap = StringVector::GetStringHeap(out_vec);
		if (!Geometry::FromBinary(string_t(str, value_len), res_val, string_heap, true)) {
			throw InvalidInputException("Failed to parse Postgres geometry data");
		}
		FlatVector::GetDataMutable<string_t>(out_vec)[output_offset] = res_val;
		break;
	}
	case LogicalTypeId::BOOLEAN:
		CheckValueLength(value_len, sizeof(bool), type);
		FlatVector::GetDataMutable<bool>(out_vec)[output_offset] = ReadBoolean();
		break;
	case LogicalTypeId::DECIMAL: {
		if (value_len < sizeof(uint16_t) * 4) {
			throw InvalidInputException("Need at least 8 bytes to read a Postgres decimal. Got %d", value_len);
		}
		switch (type.InternalType()) {
		case PhysicalType::INT16: {
			PostgresDecimal<int16_t> dec = ReadDecimal<int16_t>();
			if (!CheckDecimalKindSetNull(out_vec, output_offset, dec.kind)) {
				FlatVector::GetDataMutable<int16_t>(out_vec)[output_offset] = dec.value;
			}
			break;
		}
		case PhysicalType::INT32: {
			PostgresDecimal<int32_t> dec = ReadDecimal<int32_t>();
			if (!CheckDecimalKindSetNull(out_vec, output_offset, dec.kind)) {
				FlatVector::GetDataMutable<int32_t>(out_vec)[output_offset] = dec.value;
			}
			break;
		}
		case PhysicalType::INT64: {
			PostgresDecimal<int64_t> dec = ReadDecimal<int64_t>();
			if (!CheckDecimalKindSetNull(out_vec, output_offset, dec.kind)) {
				FlatVector::GetDataMutable<int64_t>(out_vec)[output_offset] = dec.value;
			}
			break;
		}
		case PhysicalType::INT128: {
			PostgresDecimal<hugeint_t> dec = ReadDecimal<hugeint_t, DecimalConversionHugeint>();
			if (!CheckDecimalKindSetNull(out_vec, output_offset, dec.kind)) {
				FlatVector::GetDataMutable<hugeint_t>(out_vec)[output_offset] = dec.value;
			}
			break;
		}
		default:
			throw InvalidInputException("Unsupported decimal storage type");
		}
		break;
	}

	case LogicalTypeId::DATE: {
		CheckValueLength(value_len, sizeof(int32_t), type);
		auto out_ptr = FlatVector::GetDataMutable<date_t>(out_vec);
		out_ptr[output_offset] = ReadDate();
		break;
	}
	case LogicalTypeId::TIME: {
		CheckValueLength(value_len, sizeof(int64_t), type);
		FlatVector::GetDataMutable<dtime_t>(out_vec)[output_offset] = ReadTime();
		break;
	}
	case LogicalTypeId::TIME_TZ: {
		CheckValueLength(value_len, sizeof(int64_t) + sizeof(int32_t), type);
		FlatVector::GetDataMutable<dtime_tz_t>(out_vec)[output_offset] = ReadTimeTZ();
		break;
	}
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP: {
		CheckValueLength(value_len, sizeof(int64_t), type);
		FlatVector::GetDataMutable<timestamp_t>(out_vec)[output_offset] = ReadTimestamp();
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
			FlatVector::GetDataMutable<uint8_t>(out_vec)[output_offset] = (uint8_t)offset;
			break;
		case PhysicalType::UINT16:
			FlatVector::GetDataMutable<uint16_t>(out_vec)[output_offset] = (uint16_t)offset;
			break;

		case PhysicalType::UINT32:
			FlatVector::GetDataMutable<uint32_t>(out_vec)[output_offset] = (uint32_t)offset;
			break;

		default:
			throw InternalException("ENUM can only have unsigned integers (except "
			                        "UINT64) as physical types, got %s",
			                        TypeIdToString(type.InternalType()));
		}
		break;
	}
	case LogicalTypeId::INTERVAL: {
		FlatVector::GetDataMutable<interval_t>(out_vec)[output_offset] = ReadInterval();
		break;
	}
	case LogicalTypeId::UUID: {
		CheckValueLength(value_len, 2 * sizeof(int64_t), type);
		FlatVector::GetDataMutable<hugeint_t>(out_vec)[output_offset] = ReadUUID();
		break;
	}
	case LogicalTypeId::LIST: {
		auto &list_entry = FlatVector::GetDataMutable<list_entry_t>(out_vec)[output_offset];
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
		if (value_len < 3 * sizeof(uint32_t)) {
			throw InvalidInputException(
			    "Postgres binary reader - expected at least %llu bytes for an array header of type %s, got %d",
			    3 * sizeof(uint32_t), type.ToString(), value_len);
		}
		auto array_dim = ReadInteger<uint32_t>();
		auto array_has_null = ReadInteger<uint32_t>(); // whether or not the array has nulls - ignore
		auto value_oid = ReadInteger<uint32_t>();      // value_oid - not necessary
		if (array_dim == 0) {
			list_entry.offset = child_offset;
			list_entry.length = 0;
			return;
		}
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
		ReadArray(type, postgres_type, out_vec, output_offset, 1, dimensions.get(), array_dim);
		break;
	}
	case LogicalTypeId::STRUCT: {
		auto &child_entries = StructVector::GetEntries(out_vec);
		if (postgres_type.info == PostgresTypeAnnotation::GEOM_POINT) {
			CheckValueLength(value_len, sizeof(double) * 2, type);
			FlatVector::GetDataMutable<double>(child_entries[0])[output_offset] = ReadDouble();
			FlatVector::GetDataMutable<double>(child_entries[1])[output_offset] = ReadDouble();
			break;
		}
		auto entry_count = ReadInteger<uint32_t>();
		if (entry_count != child_entries.size()) {
			throw InternalException("Mismatch in entry count: expected %d but got %d", child_entries.size(),
			                        entry_count);
		}
		for (idx_t c = 0; c < entry_count; c++) {
			auto &child = child_entries[c];
			auto value_oid = ReadInteger<uint32_t>();
			ReadValue(child.GetType(), postgres_type.children[c], child, output_offset);
		}
		break;
	}
	default:
		throw InternalException("Unsupported Type %s", type.ToString());
	}
}

PostgresDecimalKind PostgresBinaryParser::NonFiniteDecimalKindFromSign(uint16_t dec_sign) {
	if (dec_sign == NUMERIC_NAN) {
		return PostgresDecimalKind::NOT_A_NUMBER;
	}
	if (dec_sign == NUMERIC_PINF) {
		return PostgresDecimalKind::POSITIVE_INFINITY;
	}
	if (dec_sign == NUMERIC_NINF) {
		return PostgresDecimalKind::NEGATIVE_INFINITY;
	}
	throw InvalidInputException("Unsupported unbound NUMERIC sign: %u", static_cast<unsigned int>(dec_sign));
}

string PostgresBinaryParser::NonFiniteDecimalKindToString(PostgresDecimalKind kind) {
	switch (kind) {
	case PostgresDecimalKind::NOT_A_NUMBER:
		return "NaN";
	case PostgresDecimalKind::POSITIVE_INFINITY:
		return "Infinity";
	case PostgresDecimalKind::NEGATIVE_INFINITY:
		return "-Infinity";
	default:
		throw InvalidInputException("Unsupported unbound NUMERIC kind");
	}
}

bool PostgresBinaryParser::CheckDecimalKindSetNull(Vector &out_vec, idx_t output_offset, PostgresDecimalKind dec_kind) {
	if (dec_kind == PostgresDecimalKind::ORDINARY) {
		return false;
	}
	if (!type_config.numeric_nan_as_null) {
		string kind_str = NonFiniteDecimalKindToString(dec_kind);
		throw InvalidInputException("Unsupported NUMERIC value: %s", kind_str);
	}
	FlatVector::SetNull(out_vec, output_offset, true);
	return true;
}

} // namespace duckdb
