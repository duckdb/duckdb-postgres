#include "duckdb/main/attached_database.hpp"
#include "duckdb/logging/file_system_logger.hpp"
#include "duckdb/logging/log_type.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/http_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "postgres_logging.hpp"

namespace duckdb {

constexpr LogLevel PostgresQueryLogType::LEVEL;

//===--------------------------------------------------------------------===//
// PostgresQueryLogType
//===--------------------------------------------------------------------===//
string PostgresQueryLogType::ConstructLogMessage(const string &str, int64_t duration) {
	child_list_t<Value> child_list = {
	    {"query", str},
	    {"duration_ms", duration},
	};

	return Value::STRUCT(std::move(child_list)).ToString();
}

LogicalType PostgresQueryLogType::GetLogType() {
	child_list_t<LogicalType> child_list = {
	    {"query", LogicalType::VARCHAR},
	    {"duration_ms", LogicalType::BIGINT},
	};
	return LogicalType::STRUCT(child_list);
}

} // namespace duckdb
