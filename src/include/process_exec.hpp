#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

struct ProcessResult {
	int exit_code;
	string stdout_str;
	string stderr_str;
};

// Executes argv[0] (resolved via PATH) with args argv[1..]. No shell interpretation.
// Throws IOException if the process cannot be spawned or exceeds the 30-second timeout.
ProcessResult RunProcess(const vector<string> &argv);

} // namespace duckdb
