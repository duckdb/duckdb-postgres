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

	int RowCount() {
		return rows;
	}

	int ColCount() {
		return fields;
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

	void MoveResCur(int row_idx, int col_idx) {
		// Move the result cursor to row and col
		current_row = row_idx;
		current_field =col_idx;
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
	
	// Check if the current field is NULL
	bool IsNull() {
		if (!Ready()) {
			throw IOException("No current field to check for NULL");
		}
		return PQgetisnull(current_result, current_row, current_field) != 0;
	}
	
	// Basic type readers
	template <class T>
	T ReadInteger(const char* str, idx_t len) {
		T result;
		// Use proper string-to-number conversion with bounds checking
		try {
			if (std::is_same<T, int8_t>::value || std::is_same<T, int16_t>::value ||
			    std::is_same<T, int32_t>::value || std::is_same<T, int64_t>::value) {
				return (T)std::stoll(string(str, len));
			} else {
				return (T)std::stoull(string(str, len));
			}
		} catch (std::exception &e) {
			throw IOException("Failed to convert string '%s' to integer: %s", string(str, len), e.what());
		}
	}
	
	// Read current field as value for the specified type
	void ReadValue(const LogicalType &type, const PostgresType &postgres_type, Vector &out_vec, idx_t output_offset) {
		if (!Ready()) {
			throw IOException("No current field to read");
		}
		
		if (IsNull()) {
			FlatVector::SetNull(out_vec, output_offset, true);
			return;
		}
		
		char *value = PQgetvalue(current_result, current_row, current_field);
		int len = PQgetlength(current_result, current_row, current_field);
		
		switch (type.id()) {
		case LogicalTypeId::INTEGER:
			FlatVector::GetData<int32_t>(out_vec)[output_offset] = ReadInteger<int32_t>(value, len);
			break;
			
		case LogicalTypeId::VARCHAR:
			FlatVector::GetData<string_t>(out_vec)[output_offset] = StringVector::AddString(out_vec, value, len);
			break;
		
		default:
			throw NotImplementedException("Type %s not implemented in text protocol reader", 
										 LogicalTypeIdToString(type.id()));
		}
	}
	
	// Helper function to map PostgreSQL OIDs to DuckDB types
	LogicalTypeId PostgresTypeToLogicalType(Oid pg_type) {
		switch (pg_type) {
		case 16:    return LogicalTypeId::BOOLEAN;    // bool
		case 21:    return LogicalTypeId::SMALLINT;   // int2
		case 23:    return LogicalTypeId::INTEGER;    // int4
		case 20:    return LogicalTypeId::BIGINT;     // int8
		case 700:   return LogicalTypeId::FLOAT;      // float4
		case 701:   return LogicalTypeId::DOUBLE;     // float8
		case 1700:  return LogicalTypeId::DECIMAL;    // numeric
		case 25:    return LogicalTypeId::VARCHAR;    // text
		case 1042:  return LogicalTypeId::VARCHAR;    // char(n)
		case 1043:  return LogicalTypeId::VARCHAR;    // varchar(n)
		case 1082:  return LogicalTypeId::DATE;       // date
		case 1083:  return LogicalTypeId::TIME;       // time
		case 1114:  return LogicalTypeId::TIMESTAMP;  // timestamp
		case 1184:  return LogicalTypeId::TIMESTAMP_TZ; // timestamptz
		case 1186:  return LogicalTypeId::INTERVAL;   // interval
		case 17:    return LogicalTypeId::BLOB;       // bytea
		case 2950:  return LogicalTypeId::UUID;       // uuid
		// Add more as needed
		default:    
			// Handle unknown types as VARCHAR
			return LogicalTypeId::VARCHAR;
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