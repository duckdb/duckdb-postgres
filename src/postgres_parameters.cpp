#include "postgres_parameters.hpp"

#include "duckdb.hpp"

#include <libpq-fe.h>

#include "postgres_conversion.hpp"

namespace duckdb {

static const int FORMAT_TEXT = 0;
static const int FORMAT_BINARY = 1;

struct Param {
	const char *ptr = nullptr;
	int length = 0;
	int format = FORMAT_TEXT;

	Param() {
	}

	Param(const char *ptr_in, int length_in, int format_in) : ptr(ptr_in), length(length_in), format(format_in) {
	}
};

static Param CreateVarcharParam(Value &value) {
	const string &str = StringValue::Get(value);
	return Param(str.c_str(), static_cast<int>(str.length()), FORMAT_TEXT);
}

template <typename INT_TYPE>
static Param CreateIntParam(INT_TYPE num, vector<char> &copy_holder) {
	copy_holder.resize(sizeof(INT_TYPE));
	memcpy(copy_holder.data(), &num, sizeof(INT_TYPE));
	return Param(copy_holder.data(), sizeof(INT_TYPE), FORMAT_BINARY);
}

static uint32_t FloatHtonl(float num) {
	std::array<char, sizeof(float)> arr;
	memcpy(arr.data(), &num, sizeof(float));
	uint32_t int_num = *reinterpret_cast<uint32_t *>(arr.data());
	return htonl(int_num);
}

static uint64_t DoubleHtonll(double num) {
	std::array<char, sizeof(double)> arr;
	memcpy(arr.data(), &num, sizeof(double));
	uint64_t int_num = *reinterpret_cast<uint64_t *>(arr.data());
	return htonll(int_num);
}

static Param CreateParam(Value &value, vector<char> &copy_holder) {
	if (value.IsNull()) {
		return Param(nullptr, 0, FORMAT_BINARY);
	}

	switch (value.type().id()) {
	case LogicalTypeId::VARCHAR:
		return CreateVarcharParam(value);
	case LogicalTypeId::TINYINT: {
		uint16_t num = static_cast<uint16_t>(TinyIntValue::Get(value));
		return CreateIntParam(htons(num), copy_holder);
	}
	case LogicalTypeId::UTINYINT: {
		uint16_t num = static_cast<uint16_t>(UTinyIntValue::Get(value));
		return CreateIntParam(htons(num), copy_holder);
	}
	case LogicalTypeId::SMALLINT: {
		uint16_t num = static_cast<uint16_t>(SmallIntValue::Get(value));
		return CreateIntParam(htons(num), copy_holder);
	}
	case LogicalTypeId::USMALLINT: {
		uint16_t num = static_cast<uint32_t>(USmallIntValue::Get(value));
		return CreateIntParam(htonl(num), copy_holder);
	}
	case LogicalTypeId::INTEGER: {
		uint32_t num = static_cast<uint32_t>(IntegerValue::Get(value));
		return CreateIntParam(htonl(num), copy_holder);
	}
	case LogicalTypeId::UINTEGER: {
		uint64_t num = static_cast<uint64_t>(UIntegerValue::Get(value));
		return CreateIntParam(htonll(num), copy_holder);
	}
	case LogicalTypeId::BIGINT: {
		uint64_t num = static_cast<uint64_t>(BigIntValue::Get(value));
		return CreateIntParam(htonll(num), copy_holder);
	}
	case LogicalTypeId::FLOAT: {
		float num = FloatValue::Get(value);
		return CreateIntParam(FloatHtonl(num), copy_holder);
	}
	case LogicalTypeId::DOUBLE: {
		double num = DoubleValue::Get(value);
		return CreateIntParam(DoubleHtonll(num), copy_holder);
	}
	default:
		throw BinderException("Unsupported parameter type: %s", value.type().ToString().c_str());
	}
}

PostgresParameters::PostgresParameters(vector<Oid> types_p, vector<Value> values_p)
    : types(std::move(types_p)), values(std::move(values_p)) {
	idx_t count = types.size();
	if (values.size() != count) {
		throw BinderException("Parameters count mismatch, types count: %zu, values count: %zu", count, values.size());
	}

	copied_values.resize(count);
	value_ptrs.resize(count);
	lengths.resize(count);
	formats.resize(count);

	for (idx_t i = 0; i < types.size(); i++) {
		Value &val = values[i];
		vector<char> &copy_holder = copied_values[i];

		Param param = CreateParam(val, copy_holder);

		value_ptrs[i] = param.ptr;
		lengths[i] = param.length;
		formats[i] = param.format;
	}
}

} // namespace duckdb
