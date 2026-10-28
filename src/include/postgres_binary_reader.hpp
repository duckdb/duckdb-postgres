//===----------------------------------------------------------------------===//
//                         DuckDB
//
// postgres_binary_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "postgres_binary_parser.hpp"
#include "duckdb/main/client_context.hpp"
#include "postgres_result_reader.hpp"
#include "postgres_connection.hpp"

namespace duckdb {

enum class PostgresBinaryReaderMode { UNINITIALIZED, COPY, PLAIN_QUERY };

struct PostgresBinaryReader : public PostgresResultReader {
	explicit PostgresBinaryReader(PostgresConnection &con, const vector<column_t> &column_ids,
	                              const PostgresBindData &bind_data,
	                              optional_ptr<PostgresPreparedStatement> prepared_statement = nullptr);
	~PostgresBinaryReader() override;

public:
	void BeginCopy(ClientContext &context, const string &sql) override;
	PostgresReadResult Read(DataChunk &result) override;

private:
	bool FetchNextBuffer();
	void FreeBuffer();

	PostgresReadResult ReadCopy(DataChunk &result);
	PostgresReadResult ReadPlain(DataChunk &result);

private:
	PostgresBinaryReaderMode mode = PostgresBinaryReaderMode::UNINITIALIZED;
	PostgresBinaryParser parser;
	// copy mode
	data_ptr_t buffer = nullptr;
	// plain mode
	unique_ptr<PostgresResult> result;
	idx_t row_offset = 0;
};

} // namespace duckdb
