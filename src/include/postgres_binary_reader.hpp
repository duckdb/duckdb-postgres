//===----------------------------------------------------------------------===//
//                         DuckDB
//
// postgres_binary_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "postgres_binary_parser.hpp"
#include "postgres_result_reader.hpp"
#include "postgres_connection.hpp"

namespace duckdb {

struct PostgresBinaryReader : public PostgresResultReader {
	explicit PostgresBinaryReader(PostgresConnection &con, const vector<column_t> &column_ids,
	                              const PostgresBindData &bind_data);
	~PostgresBinaryReader() override;

public:
	void BeginCopy(ClientContext &context, const string &sql) override;
	PostgresReadResult Read(DataChunk &result) override;

private:
	bool FetchNextBuffer();
	void FreeBuffer();

private:
	PostgresBinaryParser parser;
	data_ptr_t buffer = nullptr;
};

} // namespace duckdb
