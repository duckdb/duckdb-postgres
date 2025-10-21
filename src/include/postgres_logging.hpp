
#pragma once

#include "duckdb/logging/logging.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

class PostgresQueryLogType : public LogType {
public:
	static constexpr const char *NAME = "PostgresQueryLog";
	static constexpr LogLevel LEVEL = LogLevel::LOG_DEBUG;

	PostgresQueryLogType() : LogType(NAME, LEVEL, GetLogType()) {};

	static string ConstructLogMessage(const string &str, int64_t duration);
	static LogicalType GetLogType();
};

} // namespace duckdb
