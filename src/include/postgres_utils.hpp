//===----------------------------------------------------------------------===//
//                         DuckDB
//
// postgres_utils.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <mutex>

#include "duckdb.hpp"
#include "duckdb/main/client_context.hpp"
#include <libpq-fe.h>
#include "postgres_version.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

namespace duckdb {
class PostgresCatalog;
class PostgresSchemaEntry;
class PostgresTransaction;

enum class PostgresQueryMode { AUTO, COPY_BINARY, PLAIN_QUERY_BINARY, PLAIN_QUERY_TEXT };

struct PostgresTypeData {
	int64_t type_modifier = 0;
	string type_name;
	string type_schema;
	idx_t array_dimensions = 0;
};

struct PostgresTypeConfig {
	bool array_as_varchar = false;
	bool numeric_as_varchar = false;
	bool numeric_nan_as_null = true;

	static PostgresTypeConfig FromContext(optional_ptr<ClientContext> context);
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

enum class PostgresConnectCheck { THROW_ON_CONNECTION_BAD, RETURN_ON_CONNECTION_BAD };

class PostgresUtils {
public:
	static PGconn *PGConnect(const string &dsn, const string &attach_path);

	static bool UseInformationSchemaIntrospection(optional_ptr<ClientContext> context);
	static bool StalenessQueryEnabled(ClientContext &context);
	static string GetCustomStalenessQuery(ClientContext &context);
	static string DataTypeToTypeName(const string &data_type);
	static LogicalType ToPostgresType(const LogicalType &input);
	static LogicalType TypeToLogicalType(optional_ptr<PostgresTransaction> transaction,
	                                     optional_ptr<PostgresSchemaEntry> schema,
	                                     const PostgresTypeConfig &type_config, const PostgresTypeData &input,
	                                     PostgresType &postgres_type);
	static string TypeToString(const LogicalType &input);
	static string PostgresOidToName(uint32_t oid);
	static uint32_t ToPostgresOid(const LogicalType &input);
	static uint32_t TypeNameToPostgresOid(const string &type_name);
	static bool SupportedPostgresOid(const LogicalType &input);
	static LogicalType RemoveAlias(const LogicalType &type);
	static PostgresType CreateEmptyPostgresType(const LogicalType &type);
	static string QuotePostgresIdentifier(const string &text);

	static PostgresVersion ExtractPostgresVersion(const string &version);

	static string EscapeConnectionString(const string &input);
	static string ExtractConnectionOption(const KeyValueSecret &kv_secret, const string &name);
	static string WriteLiteral(const string &identifier);
	static string WriteLiteralsCommaSeparated(const vector<string> &literals);
	static string WriteIdentifier(const string &identifier);

	static PostgresQueryMode ExtractQueryMode(ClientContext &ctx);
	static PostgresQueryMode ExtractQueryMode(Value &value);
	static PostgresCatalog &GetCatalogByName(ClientContext &context, const std::string &name);

private:
	static mutex libpq_init_lock;
	static bool libpq_connect_succeeded_at_least_once;

	static PGconn *PGConnectInternal(const string &dsn, const string &attach_path, PostgresConnectCheck check);

	static string EscapeQuotes(const string &text, char quote);
	static string WriteQuoted(const string &text, char quote);
};

} // namespace duckdb
