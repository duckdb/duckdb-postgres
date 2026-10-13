#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/vector/map_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "postgres_binary_reader.hpp"
#include "postgres_scanner.hpp"

namespace duckdb {

PostgresBinaryReader::PostgresBinaryReader(PostgresConnection &con_p, const vector<column_t> &column_ids,
                                           const PostgresBindData &bind_data)
    : PostgresResultReader(con_p, column_ids, bind_data),
      parser(bind_data.types, bind_data.postgres_types, bind_data.type_config) {
	if (bind_data.query_mode == PostgresQueryMode::PLAIN_QUERY_BINARY) {
		this->mode = PostgresBinaryReaderMode::PLAIN_QUERY;
	} else {
		this->mode = PostgresBinaryReaderMode::COPY;
	}
}

PostgresBinaryReader::~PostgresBinaryReader() {
	FreeBuffer();
}

void PostgresBinaryReader::BeginCopy(ClientContext &context, const string &sql) {
	if (mode == PostgresBinaryReaderMode::PLAIN_QUERY) {
		this->result = con.Query(context, sql, bind_data.params, 1);
		this->row_offset = 0;
	} else {
		con.BeginCopyFrom(context, sql, PGRES_COPY_OUT);
		if (!FetchNextBuffer()) {
			throw IOException("Failed to fetch header for COPY \"%s\"", sql);
		}
		parser.CheckHeader();
	}
}

PostgresReadResult PostgresBinaryReader::Read(DataChunk &result) {
	if (mode == PostgresBinaryReaderMode::PLAIN_QUERY) {
		return ReadPlain(result);
	} else {
		return ReadCopy(result);
	}
}

PostgresReadResult PostgresBinaryReader::ReadCopy(DataChunk &output) {
	auto result = PostgresReadResult::HAVE_MORE_TUPLES;
	while (output.size() < STANDARD_VECTOR_SIZE) {
		if (parser.ReadChunk(output, column_ids)) {
			break;
		}
		FreeBuffer();
		if (!FetchNextBuffer()) {
			result = PostgresReadResult::FINISHED;
			break;
		}
	}
	output.SetChildCardinality(output.size());
	return result;
}

bool PostgresBinaryReader::FetchNextBuffer() {
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

	// take ownership of the buffer before validating it, so that a short read
	// does not leak it - PQgetCopyData allocates even when it returns a length
	// we cannot use
	buffer = new_buffer;

	// len -2 is error
	// we expect at least 2 bytes in each message for the tuple count
	if (!new_buffer || len < sizeof(int16_t)) {
		throw IOException("Unable to read binary COPY data from Postgres: %s", string(PQerrorMessage(con.GetConn())));
	}
	parser.SetBuffer(buffer, len);
	return true;
}

void PostgresBinaryReader::FreeBuffer() {
	if (mode == PostgresBinaryReaderMode::PLAIN_QUERY) {
		result.reset();
		row_offset = 0;
	} else {
		if (buffer) {
			PQfreemem(buffer);
		}
		buffer = nullptr;
	}
}

PostgresReadResult PostgresBinaryReader::ReadPlain(DataChunk &output) {
	if (!result) {
		return PostgresReadResult::FINISHED;
	}

	idx_t output_offset = output.size();
	for (; output_offset < STANDARD_VECTOR_SIZE && row_offset < result->Count(); row_offset++, output_offset++) {
		for (idx_t output_idx = 0; output_idx < output.ColumnCount(); output_idx++) {
			auto col_idx = column_ids[output_idx];
			auto &out_vec = output.data[output_idx];

			if (result->IsNull(row_offset, output_idx)) {
				FlatVector::SetNull(out_vec, output_offset, true);
				continue;
			}

			// Obtain the raw binary data from the result cell
			auto val = result->GetStringRef(row_offset, output_idx);
			parser.SetBuffer(data_ptr_cast(val.GetDataWriteable()), val.GetSize());

			if (col_idx == COLUMN_IDENTIFIER_ROW_ID) {
				PostgresType ctid_type;
				ctid_type.info = PostgresTypeAnnotation::CTID;
				parser.ParseValue(LogicalType::BIGINT, ctid_type, NumericCast<int32_t>(val.GetSize()), out_vec,
				                  output_offset);
			} else {
				parser.ParseValue(bind_data.types[col_idx], bind_data.postgres_types[col_idx],
				                  NumericCast<int32_t>(val.GetSize()), out_vec, output_offset);
			}
		}
	}
	output.SetChildCardinality(output_offset);

	bool finished = row_offset >= result->Count();
	if (finished) {
		FreeBuffer();
		return PostgresReadResult::FINISHED;
	}
	return PostgresReadResult::HAVE_MORE_TUPLES;
}

} // namespace duckdb
