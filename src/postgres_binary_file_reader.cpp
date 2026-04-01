#include "postgres_binary_file_reader.hpp"

namespace duckdb {

static vector<column_t> MakeSequentialColumnIds(idx_t count) {
	vector<column_t> ids;
	for (idx_t i = 0; i < count; i++) {
		ids.push_back(i);
	}
	return ids;
}

PostgresBinaryFileReader::PostgresBinaryFileReader(ClientContext &context, const string &file_path,
                                                   vector<LogicalType> types_p, vector<PostgresType> postgres_types_p,
                                                   idx_t buffer_size_p)
    : column_ids(MakeSequentialColumnIds(types_p.size())), parser(std::move(types_p), std::move(postgres_types_p)),
      buffer_size(buffer_size_p), file_offset(0), leftover(0), leftover_offset(0), finished(false) {
	auto &fs = FileSystem::GetFileSystem(context);
	file_handle = fs.OpenFile(file_path, FileFlags::FILE_FLAGS_READ);
	file_size = file_handle->GetFileSize();
	if (file_size == 0) {
		throw IOException("Postgres binary file '%s' is empty", file_path);
	}
	read_buffer = make_uniq_array<data_t>(buffer_size);
	if (!FillBuffer()) {
		throw IOException("Failed to read postgres binary file '%s'", file_path);
	}
	parser.CheckHeader();
}

bool PostgresBinaryFileReader::ReadChunk(DataChunk &output) {
	while (output.size() < STANDARD_VECTOR_SIZE) {
		if (parser.ReadChunk(output, column_ids)) {
			return true;
		}
		if (finished) {
			return false;
		}
		if (!FillBuffer()) {
			return false;
		}
	}
	return true;
}

bool PostgresBinaryFileReader::FillBuffer() {
	if (file_offset >= file_size && leftover == 0) {
		finished = true;
		return false;
	}

	if (leftover > 0) {
		memmove(read_buffer.get(), read_buffer.get() + leftover_offset, leftover);
	}

	idx_t to_read = MinValue(buffer_size - leftover, file_size - file_offset);
	if (to_read > 0) {
		file_handle->Read(read_buffer.get() + leftover, to_read, file_offset);
		file_offset += to_read;
	}

	idx_t total = leftover + to_read;
	if (total == 0) {
		finished = true;
		return false;
	}

	idx_t valid = FindLastCompleteRow(read_buffer.get(), total);
	if (valid == 0) {
		if (file_offset >= file_size) {
			valid = total;
		} else {
			throw IOException("Postgres binary file contains a row larger than the read buffer (%llu bytes). "
			                  "Increase buffer_size.",
			                  buffer_size);
		}
	}

	parser.SetBuffer(read_buffer.get(), valid);
	leftover = total - valid;
	leftover_offset = valid;
	return true;
}

idx_t PostgresBinaryFileReader::FindLastCompleteRow(data_ptr_t buf, idx_t len) {
	data_ptr_t ptr = buf;
	data_ptr_t end = buf + len;
	idx_t last_safe = 0;

	while (ptr + sizeof(int16_t) <= end) {
		int16_t tuple_count =
		    static_cast<int16_t>((static_cast<uint16_t>(ptr[0]) << 8) | static_cast<uint16_t>(ptr[1]));
		ptr += sizeof(int16_t);

		if (tuple_count <= 0) {
			last_safe = ptr - buf;
			break;
		}

		bool row_complete = true;
		for (int16_t c = 0; c < tuple_count; c++) {
			if (ptr + sizeof(int32_t) > end) {
				row_complete = false;
				break;
			}
			int32_t value_len =
			    static_cast<int32_t>((static_cast<uint32_t>(ptr[0]) << 24) | (static_cast<uint32_t>(ptr[1]) << 16) |
			                         (static_cast<uint32_t>(ptr[2]) << 8) | static_cast<uint32_t>(ptr[3]));
			ptr += sizeof(int32_t);

			if (value_len > 0) {
				if (ptr + value_len > end) {
					row_complete = false;
					break;
				}
				ptr += value_len;
			}
		}

		if (!row_complete) {
			break;
		}
		last_safe = ptr - buf;
	}

	return last_safe;
}

} // namespace duckdb
