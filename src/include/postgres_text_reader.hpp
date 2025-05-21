//===----------------------------------------------------------------------===//
//                         DuckDB
//
// postgres_text_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/common/types/interval.hpp"
#include "libpq-fe.h"
#include "postgres_conversion.hpp"
#include "postgres_connection.hpp"
#include <cstring>

namespace duckdb {

struct PostgresTextReader {
	explicit PostgresTextReader(PostgresConnection &con_p) : con(con_p), current_result(nullptr), 
                                                         current_row(0), current_field(0) {
	}
	
	~PostgresTextReader() {
		Reset();
	}
	
	PostgresConnection &GetConn() {
		return con;
	}

	bool ReadTextFrom(const string &query) {
		Reset();
		current_result = PQexec(con.GetConn(), query.c_str());
		if (!current_result) {
			throw IOException("Failed to execute query: %s", string(PQerrorMessage(con.GetConn())));
		}
		
		auto status = PQresultStatus(current_result);
		if (status != PGRES_TUPLES_OK && status != PGRES_SINGLE_TUPLE) {
			string error_msg = string(PQresultErrorMessage(current_result));
			Reset();
			throw IOException("Query failed: %s", error_msg);
		}
		
		rows = PQntuples(current_result);
		fields = PQnfields(current_result);
		current_row = 0;
		current_field = 0;
		
		return rows > 0;
	}

	bool NextRow() {
		if (!current_result || current_row >= rows) {
			return false;
		}
		current_row++;
		current_field = 0;
		return current_row < rows;
	}
	
	bool NextField() {
		if (!current_result || current_field >= fields) {
			return false;
		}
		current_field++;
		return current_field < fields;
	}
	
	void Reset() {
		if (current_result) {
			PQclear(current_result);
			current_result = nullptr;
		}
		current_row = 0;
		current_field = 0;
		rows = 0;
		fields = 0;
	}
	
	bool Ready() {
		return current_result != nullptr && current_row < rows && current_field < fields;
	}

	bool Done() {
		return !Ready();
	}
	
	// Check if the current field is NULL
	bool IsNull() {
		return PQgetisnull(current_result, current_row, current_field) != 0;
	}
	
	// Basic type readers
	template <class T>
	T ReadInteger(const char* str, idx_t len) {
		T result;
		return (T)std::stoll(string(str, len));
	}

	template <class T>
	T ReadUInteger(const char* str, idx_t len) {
		T result;
		return (T)std::stoull(string(str, len));
	}

	inline bool ReadBoolean(const char* str) {
		if (strcmp(str, "t") == 0) {
			return true;
		} else {
			return false;
		}
	}
	
	inline float ReadFloat(const char* str) {
		return std::stof(str);
	}


	inline double ReadDouble(const char* str) {
		return std::stod(str);
	}

	// Read current field as value for the specified type
	void ReadValue(const LogicalType &type, const PostgresType &postgres_type, Vector &out_vec, idx_t output_offset) {

		if (IsNull()) {
			FlatVector::SetNull(out_vec, output_offset, true);
			return;
		}
		
		char *value = PQgetvalue(current_result, current_row, current_field);
		int len = PQgetlength(current_result, current_row, current_field);
		
		switch (type.id()) {
		case LogicalTypeId::SMALLINT:
			FlatVector::GetData<int16_t>(out_vec)[output_offset] = ReadInteger<int16_t>(value, len);
			break;

		case LogicalTypeId::INTEGER:
			FlatVector::GetData<int32_t>(out_vec)[output_offset] = ReadInteger<int32_t>(value, len);
			break;

		case LogicalTypeId::BIGINT:
			FlatVector::GetData<int64_t>(out_vec)[output_offset] = ReadInteger<int64_t>(value, len);
			break;
			
		case LogicalTypeId::UINTEGER:
			FlatVector::GetData<uint32_t>(out_vec)[output_offset] = ReadUInteger<uint32_t>(value, len);
			break;



		case LogicalTypeId::FLOAT:
			FlatVector::GetData<bool>(out_vec)[output_offset] = ReadFloat(value);
			break;

		case LogicalTypeId::DOUBLE:
			FlatVector::GetData<bool>(out_vec)[output_offset] = ReadDouble(value);
			break;

		
		case LogicalTypeId::BLOB:
		case LogicalTypeId::VARCHAR:
			FlatVector::GetData<string_t>(out_vec)[output_offset] = StringVector::AddString(out_vec, value, len);
			break;
		
		case LogicalTypeId::BOOLEAN:
			FlatVector::GetData<bool>(out_vec)[output_offset] = ReadBoolean(value);
			break;

		case LogicalTypeId::DATE: {
			FlatVector::GetData<date_t>(out_vec)[output_offset] = Date::FromString(value);
			break;
		}
		case LogicalTypeId::TIMESTAMP: 
			FlatVector::GetData<timestamp_t>(out_vec)[output_offset] = Timestamp::FromString(value);
			break;
	

		default:
			throw NotImplementedException("Type %s not implemented in text protocol reader", 
										 LogicalTypeIdToString(type.id()));
		}
	}

private:
	PostgresConnection &con;
	PGresult *current_result;
	int current_row;
	int current_field;
	int rows;
	int fields;
};

} // namespace duckdb