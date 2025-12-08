//===----------------------------------------------------------------------===//
//                         DuckDB
//
// postgres_utils.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include <libpq-fe.h>
#include "postgres_version.hpp"

namespace duckdb {
class PostgresSchemaEntry;
class PostgresTransaction;

struct PostgresTypeData {
	int64_t type_modifier = 0;
	string type_name;
	idx_t array_dimensions = 0;
};

enum class PostgresTypeAnnotation {
	STANDARD,
	CAST_TO_VARCHAR,
	NUMERIC_AS_DOUBLE,
	CTID,
	JSONB,
	FIXED_LENGTH_CHAR,
	GEOM_POINT,
	GEOM_LINE,
	GEOM_LINE_SEGMENT,
	GEOM_BOX,
	GEOM_PATH,
	GEOM_POLYGON,
	GEOM_CIRCLE
};

struct PostgresType {
	idx_t oid = 0;
	PostgresTypeAnnotation info = PostgresTypeAnnotation::STANDARD;
	vector<PostgresType> children;
};

enum class PostgresCopyFormat { AUTO = 0, BINARY = 1, TEXT = 2 };

struct PostgresCopyState {
	PostgresCopyFormat format = PostgresCopyFormat::AUTO;
	bool has_null_byte_replacement = false;
	string null_byte_replacement;

	void Initialize(ClientContext &context);
};

enum class PostgresIsolationLevel { READ_COMMITTED, REPEATABLE_READ, SERIALIZABLE };

class PostgresUtils {
public:
	static PGconn *PGConnect(const string &dsn, const string &attach_path);

	static LogicalType ToPostgresType(const LogicalType &input);
	static LogicalType TypeToLogicalType(optional_ptr<PostgresTransaction> transaction,
	                                     optional_ptr<PostgresSchemaEntry> schema, const PostgresTypeData &input,
	                                     PostgresType &postgres_type);
	static string TypeToString(const LogicalType &input);
	static string PostgresOidToName(uint32_t oid);
	static uint32_t ToPostgresOid(const LogicalType &input);
	static bool SupportedPostgresOid(const LogicalType &input);
	static LogicalType RemoveAlias(const LogicalType &type);
	static PostgresType CreateEmptyPostgresType(const LogicalType &type);
	static string QuotePostgresIdentifier(const string &text);

	static PostgresVersion ExtractPostgresVersion(const string &version);
};

} // namespace duckdb
