//===----------------------------------------------------------------------===//
//                         DuckDB
//
// postgres_binary_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "postgres_conversion.hpp"

namespace duckdb {

class PostgresBinaryWriter {
public:
	explicit PostgresBinaryWriter(PostgresCopyState &state) : state(state) {
	}

	template <class T>
	T GetInteger(T val) {
		if (sizeof(T) == sizeof(uint8_t)) {
			return val;
		} else if (sizeof(T) == sizeof(uint16_t)) {
			return htons(val);
		} else if (sizeof(T) == sizeof(uint32_t)) {
			return htonl(val);
		} else if (sizeof(T) == sizeof(uint64_t)) {
			return htonll(val);
		} else {
			D_ASSERT(0);
			return val;
		}
	}

	template <class T>
	void WriteRawInteger(T val) {
		stream.Write<T>(GetInteger(val));
	}

public:
	void WriteHeader() {
		stream.WriteData(const_data_ptr_cast(PostgresConversion::COPY_HEADER), PostgresConversion::COPY_HEADER_LENGTH);
		WriteRawInteger<int32_t>(0);
		WriteRawInteger<int32_t>(0);
	}

	void WriteFooter() {
		WriteRawInteger<int16_t>(-1);
	}

	void BeginRow(idx_t column_count) {
		// field count
		WriteRawInteger<int16_t>(int16_t(column_count));
	}

	void FinishRow() {
	}

	void WriteNull() {
		WriteRawInteger<int32_t>(-1);
	}

	template <class T>
	void WriteInteger(T value) {
		WriteRawInteger<int32_t>(sizeof(T));
		WriteRawInteger<T>(value);
	}

	void WriteBoolean(bool value) {
		WriteInteger<uint8_t>(value ? 1 : 0);
	}

	void WriteFloat(float value) {
		uint32_t i = *reinterpret_cast<uint32_t *>(&value);
		WriteInteger<uint32_t>(i);
	}

	void WriteDouble(double value) {
		uint64_t i = *reinterpret_cast<uint64_t *>(&value);
		WriteInteger<uint64_t>(i);
	}

	static uint32_t DuckDBDateToPostgres(date_t value) {
		if (value == date_t::infinity()) {
			return POSTGRES_DATE_INF;
		}
		if (value == date_t::ninfinity()) {
			return POSTGRES_DATE_NINF;
		}
		if (value.days <= POSTGRES_MIN_DATE || value.days >= POSTGRES_MAX_DATE) {
			throw InvalidInputException("DATE \"%s\" is out of range for Postgres' DATE field", Date::ToString(value));
		}
		return uint32_t(value.days + DUCKDB_EPOCH_DATE - POSTGRES_EPOCH_JDATE);
	}

	void WriteDate(date_t value) {
		WriteInteger<uint32_t>(DuckDBDateToPostgres(value));
	}

	void WriteTime(dtime_t value) {
		WriteInteger<uint64_t>(value.micros);
	}

	void WriteTimeTZ(dtime_tz_t value) {
		WriteRawInteger<int32_t>(sizeof(uint64_t) + sizeof(int32_t));
		WriteRawInteger<uint64_t>(value.time().micros);
		WriteRawInteger<int32_t>(-value.offset());
	}

	static uint64_t DuckDBTimestampToPostgres(timestamp_t value) {
		if (value == timestamp_t::infinity()) {
			return POSTGRES_INFINITY;
		}
		if (value == timestamp_t::ninfinity()) {
			return POSTGRES_NINFINITY;
		}
		return uint64_t(value.value - (POSTGRES_EPOCH_TS - DUCKDB_EPOCH_TS));
	}

	void WriteTimestamp(timestamp_t value) {
		WriteInteger<uint64_t>(DuckDBTimestampToPostgres(value));
	}

	void WriteInterval(interval_t value) {
		WriteRawInteger<int32_t>(sizeof(uint64_t) + sizeof(uint32_t) + sizeof(uint32_t));
		WriteRawInteger<uint64_t>(value.micros);
		WriteRawInteger<uint32_t>(value.days);
		WriteRawInteger<uint32_t>(value.months);
	}

	void WriteUUID(hugeint_t value) {
		WriteRawInteger<int32_t>(sizeof(uint64_t) * 2);
		WriteRawInteger<uint64_t>(value.upper ^ uint64_t(1) << 63);
		WriteRawInteger<uint64_t>(value.lower);
	}

	template <class T, class OP = DecimalConversionInteger>
	void WriteDecimal(T value, uint16_t scale) {
		constexpr idx_t MAX_DIGITS = sizeof(T) * 4;
		uint16_t sign;
		if (value < 0) {
			value = -value;
			sign = NUMERIC_NEG;
		} else {
			sign = NUMERIC_POS;
		}
		// divide the decimal into the integer part (before the decimal point) and fractional part (after the point)
		T integer_part;
		T fractional_part;
		if (scale == 0) {
			integer_part = value;
			fractional_part = 0;
		} else {
			integer_part = value / OP::GetPowerOfTen(scale);
			fractional_part = value % OP::GetPowerOfTen(scale);
		}
		uint16_t integral_digits[MAX_DIGITS];
		uint16_t fractional_digits[MAX_DIGITS];
		int32_t integral_ndigits = 0;
		// split the integral part into parts of up to NBASE (4 digits => 0..9999)
		while (integer_part > 0) {
			integral_digits[integral_ndigits++] = uint16_t(integer_part % T(NBASE));
			integer_part /= T(NBASE);
		}
		// split the fractional part into parts of up to NBASE (4 digits => 0..9999)
		// count the amount of digits required for the fractional part
		// note that while it is technically possible to leave out zeros here this adds even more complications
		// so we just always write digits for the full "scale", even if not strictly required
		int32_t fractional_ndigits = (scale + DEC_DIGITS - 1) / DEC_DIGITS;
		// fractional digits are LEFT aligned (for some unknown reason)
		// that means if we write ".12" with a scale of 2 we actually need to write "1200", instead of "12"
		// this means we need to "correct" the number 12 by multiplying by 100 in this case
		// this correction factor is the "number of digits to the next full number"
		int32_t correction = fractional_ndigits * DEC_DIGITS - scale;
		fractional_part *= OP::GetPowerOfTen(correction);
		for (idx_t i = 0; i < fractional_ndigits; i++) {
			fractional_digits[i] = uint16_t(fractional_part % NBASE);
			fractional_part /= NBASE;
		}

		int32_t ndigits = integral_ndigits + fractional_ndigits;
		int32_t weight = integral_ndigits - 1;
		// size
		WriteRawInteger<int32_t>(int32_t(sizeof(uint16_t)) * (4 + ndigits));
		// header
		WriteRawInteger<uint16_t>(ndigits);
		WriteRawInteger<int16_t>(weight);
		WriteRawInteger<uint16_t>(sign);
		WriteRawInteger<uint16_t>(scale);
		// wriet the integer and fractional values (in reverse order)
		for (idx_t i = integral_ndigits; i > 0; i--) {
			WriteRawInteger<uint16_t>(integral_digits[i - 1]);
		}
		for (idx_t i = fractional_ndigits; i > 0; i--) {
			WriteRawInteger<uint16_t>(fractional_digits[i - 1]);
		}
	}

	void WriteRawBlob(string_t value) {
		auto str_size = value.GetSize();
		auto str_data = value.GetData();
		WriteRawInteger<int32_t>(NumericCast<int32_t>(str_size));
		stream.WriteData(const_data_ptr_cast(str_data), str_size);
	}

	void WriteVarchar(string_t value) {
		auto str_size = value.GetSize();
		auto str_data = value.GetData();
		if (memchr(str_data, '\0', str_size) != nullptr) {
			if (!state.has_null_byte_replacement) {
				throw InvalidInputException("Attempting to write a VARCHAR value with a NULL-byte. Postgres does not "
				                            "support NULL-bytes in VARCHAR values.\n* SET pg_null_byte_replacement='' "
				                            "to remove NULL bytes or replace them with another character");
			}
			// we have a NULL byte replacement - construct a new string that has all null bytes replaced and write it
			// out
			string new_str;
			for (idx_t i = 0; i < str_size; i++) {
				if (str_data[i] == '\0') {
					new_str += state.null_byte_replacement;
				} else {
					new_str += str_data[i];
				}
			}
			WriteRawBlob(new_str);
			return;
		}
		WriteRawBlob(value);
	}

	void WriteArray(Vector &col, idx_t r, const vector<uint32_t> &dimensions, idx_t depth, uint32_t count) {
		auto list_data = FlatVector::GetData<list_entry_t>(col);
		auto &child_vector = ListVector::GetEntry(col);
		for (idx_t i = 0; i < count; i++) {
			auto list_entry = list_data[r + i];
			if (list_entry.length != dimensions[depth]) {
				throw InvalidInputException("Postgres multidimensional arrays must all have matching dimensions - "
				                            "found a length mismatch (found %llu entries, expected %llu)",
				                            list_entry.length, dimensions[depth]);
			}
			if (child_vector.GetType().id() == LogicalTypeId::LIST) {
				// multidimensional array - recurse
				WriteArray(child_vector, list_entry.offset, dimensions, depth + 1, list_entry.length);
			} else {
				// write the actual values
				for (idx_t child_idx = 0; child_idx < list_entry.length; child_idx++) {
					WriteValue(child_vector, list_entry.offset + child_idx);
				}
			}
		}
	}

	void WriteValue(Vector &col, idx_t r) {
		if (FlatVector::IsNull(col, r)) {
			WriteNull();
			return;
		}
		auto &type = col.GetType();
		switch (type.id()) {
		case LogicalTypeId::BOOLEAN: {
			auto data = FlatVector::GetData<bool>(col)[r];
			WriteBoolean(data);
			break;
		}
		case LogicalTypeId::SMALLINT: {
			auto data = FlatVector::GetData<int16_t>(col)[r];
			WriteInteger<int16_t>(data);
			break;
		}
		case LogicalTypeId::INTEGER: {
			auto data = FlatVector::GetData<int32_t>(col)[r];
			WriteInteger<int32_t>(data);
			break;
		}
		case LogicalTypeId::BIGINT: {
			auto data = FlatVector::GetData<int64_t>(col)[r];
			WriteInteger<int64_t>(data);
			break;
		}
		case LogicalTypeId::FLOAT: {
			auto data = FlatVector::GetData<float>(col)[r];
			WriteFloat(data);
			break;
		}
		case LogicalTypeId::DOUBLE: {
			auto data = FlatVector::GetData<double>(col)[r];
			WriteDouble(data);
			break;
		}
		case LogicalTypeId::DECIMAL: {
			auto scale = DecimalType::GetScale(type);
			switch (type.InternalType()) {
			case PhysicalType::INT16:
				WriteDecimal<int16_t>(FlatVector::GetData<int16_t>(col)[r], scale);
				break;
			case PhysicalType::INT32:
				WriteDecimal<int32_t>(FlatVector::GetData<int32_t>(col)[r], scale);
				break;
			case PhysicalType::INT64:
				WriteDecimal<int64_t>(FlatVector::GetData<int64_t>(col)[r], scale);
				break;
			case PhysicalType::INT128:
				WriteDecimal<hugeint_t, DecimalConversionHugeint>(FlatVector::GetData<hugeint_t>(col)[r], scale);
				break;
			default:
				throw InternalException("Unsupported type for decimal");
			}
			break;
		}
		case LogicalTypeId::DATE: {
			auto data = FlatVector::GetData<date_t>(col)[r];
			WriteDate(data);
			break;
		}
		case LogicalTypeId::TIME: {
			auto data = FlatVector::GetData<dtime_t>(col)[r];
			WriteTime(data);
			break;
		}
		case LogicalTypeId::TIME_TZ: {
			auto data = FlatVector::GetData<dtime_tz_t>(col)[r];
			WriteTimeTZ(data);
			break;
		}
		case LogicalTypeId::TIMESTAMP:
		case LogicalTypeId::TIMESTAMP_TZ: {
			auto data = FlatVector::GetData<timestamp_t>(col)[r];
			WriteTimestamp(data);
			break;
		}
		case LogicalTypeId::INTERVAL: {
			auto data = FlatVector::GetData<interval_t>(col)[r];
			WriteInterval(data);
			break;
		}
		case LogicalTypeId::UUID: {
			auto data = FlatVector::GetData<hugeint_t>(col)[r];
			WriteUUID(data);
			break;
		}
		case LogicalTypeId::VARCHAR: {
			auto data = FlatVector::GetData<string_t>(col)[r];
			WriteVarchar(data);
			break;
		}
		case LogicalTypeId::BLOB: {
			auto data = FlatVector::GetData<string_t>(col)[r];
			WriteRawBlob(data);
			break;
		}
		case LogicalTypeId::ENUM: {
			idx_t pos;
			switch (type.InternalType()) {
			case PhysicalType::UINT8:
				pos = FlatVector::GetData<uint8_t>(col)[r];
				break;
			case PhysicalType::UINT16:
				pos = FlatVector::GetData<uint16_t>(col)[r];
				break;
			case PhysicalType::UINT32:
				pos = FlatVector::GetData<uint32_t>(col)[r];
				break;
			default:
				throw InternalException("ENUM can only have unsigned integers (except "
				                        "UINT64) as physical types, got %s",
				                        TypeIdToString(type.InternalType()));
			}
			WriteVarchar(EnumType::GetString(type, pos));
			break;
		}
		case LogicalTypeId::LIST: {
			auto list_entry = FlatVector::GetData<list_entry_t>(col)[r];
			auto value_oid = PostgresUtils::ToPostgresOid(ListType::GetChildType(type));
			if (list_entry.length == 0) {
				// empty list
				WriteRawInteger<int32_t>(sizeof(uint32_t) * 3);
				WriteRawInteger<uint32_t>(0);
				WriteRawInteger<uint32_t>(0);
				WriteRawInteger<uint32_t>(value_oid);
				return;
			}
			// compute how many dimensions we will write
			vector<uint32_t> dimensions;
			const_reference<Vector> current_vector = col;
			idx_t current_position = r;
			while (current_vector.get().GetType().id() == LogicalTypeId::LIST) {
				auto current_entry = FlatVector::GetData<list_entry_t>(current_vector.get())[current_position];
				dimensions.push_back(current_entry.length);
				current_vector = ListVector::GetEntry(current_vector.get());
				current_position = current_entry.offset;
			}

			// list header
			// record the location of the field size in the stream
			auto start_position = stream.GetPosition();
			WriteRawInteger<int32_t>(0);                  // data size (nop for now)
			WriteRawInteger<uint32_t>(dimensions.size()); // ndim
			WriteRawInteger<uint32_t>(1);                 // has nulls
			WriteRawInteger<uint32_t>(value_oid);         // value_oid
			// write the dimensions of the arrays
			for (auto &dim : dimensions) {
				WriteRawInteger<uint32_t>(dim); // array length
				WriteRawInteger<uint32_t>(1);   // index lower bounds
			}
			// now recursively write the actual values
			WriteArray(col, r, dimensions, 0, 1);

			// after writing all list elements update the field size
			auto end_position = stream.GetPosition();
			auto field_size = int32_t(end_position - start_position - sizeof(int32_t));
			Store<int32_t>(GetInteger(field_size), stream.GetData() + start_position);
			break;
		}
		case LogicalTypeId::STRUCT: {
			auto &child_entries = StructVector::GetEntries(col);

			auto start_position = stream.GetPosition();
			WriteRawInteger<int32_t>(0);                     // data size (nop for now)
			WriteRawInteger<uint32_t>(child_entries.size()); // column count
			for (auto &child : child_entries) {
				auto value_oid = PostgresUtils::ToPostgresOid(child->GetType());
				WriteRawInteger<uint32_t>(value_oid); // value oid
				WriteValue(*child, r);
			}
			auto end_position = stream.GetPosition();
			// after writing all list elements update the field size
			auto field_size = int32_t(end_position - start_position - sizeof(int32_t));
			Store<int32_t>(GetInteger(field_size), stream.GetData() + start_position);
			break;
		}
		default:
			throw NotImplementedException("Type \"%s\" is not supported for Postgres binary copy", type);
		}
	}

public:
	MemoryStream stream;
	PostgresCopyState &state;
};

} // namespace duckdb
