#include "postgres_binary_reader.hpp"
#include "postgres_scanner.hpp"

namespace duckdb {

PostgresBinaryReader::PostgresBinaryReader(PostgresConnection &con_p, const vector<column_t> &column_ids,
                                           const PostgresBindData &bind_data)
    : PostgresResultReader(con_p, column_ids, bind_data), parser(bind_data.types, bind_data.postgres_types) {
}

PostgresBinaryReader::~PostgresBinaryReader() {
	FreeBuffer();
}

void PostgresBinaryReader::BeginCopy(ClientContext &context, const string &sql) {
	con.BeginCopyFrom(context, sql, PGRES_COPY_OUT);
	if (!FetchNextBuffer()) {
		throw IOException("Failed to fetch header for COPY \"%s\"", sql);
	}
	parser.CheckHeader();
}

PostgresReadResult PostgresBinaryReader::Read(DataChunk &output) {
	while (output.size() < STANDARD_VECTOR_SIZE) {
		if (parser.ReadChunk(output, column_ids)) {
			return PostgresReadResult::HAVE_MORE_TUPLES;
		}
		FreeBuffer();
		if (!FetchNextBuffer()) {
			break;
		}
	}
	if (output.size() > 0) {
		return PostgresReadResult::HAVE_MORE_TUPLES;
	}
	return PostgresReadResult::FINISHED;
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

	// len -2 is error
	// we expect at least 2 bytes in each message for the tuple count
	if (!new_buffer || len < sizeof(int16_t)) {
		throw IOException("Unable to read binary COPY data from Postgres: %s", string(PQerrorMessage(con.GetConn())));
	}
	buffer = new_buffer;
	parser.SetBuffer(buffer, len);
	return true;
}

void PostgresBinaryReader::FreeBuffer() {
	if (buffer) {
		PQfreemem(buffer);
	}
	buffer = nullptr;
}

} // namespace duckdb
