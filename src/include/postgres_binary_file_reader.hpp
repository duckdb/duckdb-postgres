//===----------------------------------------------------------------------===//
//                         DuckDB
//
// postgres_binary_file_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "postgres_binary_parser.hpp"
#include "duckdb/common/file_system.hpp"

namespace duckdb {

class PostgresBinaryFileReader {
public:
	static constexpr idx_t DEFAULT_BUFFER_SIZE = 32 * 1024 * 1024;
	//! Size of the binary COPY file header: the signature, followed by the flags field and the header
	//! extension area length (4 bytes each). Must match the header consumed by PostgresBinaryParser::CheckHeader.
	static constexpr idx_t COPY_FILE_HEADER_SIZE = PostgresConversion::COPY_HEADER_LENGTH + 8;

	PostgresBinaryFileReader(ClientContext &context, const string &file_path, vector<LogicalType> types,
	                         vector<PostgresType> postgres_types, idx_t buffer_size = DEFAULT_BUFFER_SIZE);

	bool ReadChunk(DataChunk &output);

private:
	bool FillBuffer();
	static idx_t FindLastCompleteRow(data_ptr_t buf, idx_t len);

private:
	vector<column_t> column_ids;
	PostgresBinaryParser parser;
	unique_ptr<FileHandle> file_handle;
	unique_ptr<data_t[]> read_buffer;
	idx_t buffer_size;
	idx_t file_size;
	idx_t file_offset;
	idx_t leftover;
	idx_t leftover_offset;
	bool finished;
	bool header_scanned;
};

} // namespace duckdb
