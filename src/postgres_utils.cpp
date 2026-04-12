#include "postgres_utils.hpp"
#include "storage/postgres_schema_entry.hpp"
#include "storage/postgres_transaction.hpp"
#include "postgres_type_oids.hpp"

namespace duckdb {

static void PGNoticeProcessor(void *arg, const char *message) {
}

PGconn *PostgresUtils::PGConnect(const string &dsn, const string &attach_path) {
	PGconn *conn = PQconnectdb(dsn.c_str());

	// both PQStatus and PQerrorMessage check for nullptr
	if (PQstatus(conn) == CONNECTION_BAD) {
		throw IOException("Unable to connect to Postgres at \"%s\": %s", attach_path, string(PQerrorMessage(conn)));
	}
	PQsetNoticeProcessor(conn, PGNoticeProcessor, nullptr);
	return conn;
}

string PostgresUtils::TypeToString(const LogicalType &input) {
	if (input.HasAlias()) {
		return input.GetAlias();
	}
	switch (input.id()) {
	case LogicalTypeId::GEOMETRY:
		return "GEOMETRY";
	case LogicalTypeId::FLOAT:
		return "REAL";
	case LogicalTypeId::DOUBLE:
		return "FLOAT";
	case LogicalTypeId::BLOB:
		return "BYTEA";
	case LogicalTypeId::LIST:
		return PostgresUtils::TypeToString(ListType::GetChildType(input)) + "[]";
	case LogicalTypeId::ENUM:
		throw NotImplementedException("Enums in Postgres must be named - unnamed enums are not supported. Use CREATE "
		                              "TYPE to create a named enum.");
	case LogicalTypeId::STRUCT:
		throw NotImplementedException("Composite types in Postgres must be named - unnamed composite types are not "
		                              "supported. Use CREATE TYPE to create a named composite type.");
	case LogicalTypeId::MAP:
		throw NotImplementedException("MAP type not supported in Postgres");
	case LogicalTypeId::UNION:
		throw NotImplementedException("UNION type not supported in Postgres");
	default:
		return input.ToString();
	}
}

LogicalType PostgresUtils::RemoveAlias(const LogicalType &type) {
	if (!type.HasAlias()) {
		return type;
	}
	if (StringUtil::CIEquals(type.GetAlias(), "json")) {
		return type;
	}
	switch (type.id()) {
	case LogicalTypeId::STRUCT: {
		auto child_types = StructType::GetChildTypes(type);
		return LogicalType::STRUCT(std::move(child_types));
	}
	case LogicalTypeId::ENUM: {
		auto &enum_vector = EnumType::GetValuesInsertOrder(type);
		Vector new_vector(LogicalType::VARCHAR);
		new_vector.Reference(enum_vector);
		return LogicalType::ENUM(new_vector, EnumType::GetSize(type));
	}
	default:
		throw InternalException("Unsupported logical type for RemoveAlias");
	}
}

LogicalType PostgresUtils::TypeToLogicalType(optional_ptr<PostgresTransaction> transaction,
                                             optional_ptr<PostgresSchemaEntry> schema,
                                             const PostgresTypeData &type_info, PostgresType &postgres_type) {
	auto &pgtypename = type_info.type_name;

	// postgres array types start with an _
	if (StringUtil::StartsWith(pgtypename, "_")) {
		if (transaction) {
			auto context = transaction->GetContext();
			if (!context) {
				throw InternalException("Context is destroyed!?");
			}
			Value array_as_varchar;
			if (context->TryGetCurrentSetting("pg_array_as_varchar", array_as_varchar)) {
				if (BooleanValue::Get(array_as_varchar)) {
					postgres_type.info = PostgresTypeAnnotation::CAST_TO_VARCHAR;
					return LogicalType::VARCHAR;
				}
			}
		}
		// get the array dimension information
		idx_t dimensions = type_info.array_dimensions;
		if (dimensions == 0) {
			dimensions = 1;
		}
		// fetch the child type of the array
		PostgresTypeData child_type_info;
		child_type_info.type_name = pgtypename.substr(1);
		child_type_info.type_modifier = type_info.type_modifier;
		PostgresType child_pg_type;
		auto child_type = PostgresUtils::TypeToLogicalType(transaction, schema, child_type_info, child_pg_type);
		// construct the child type based on the number of dimensions
		for (idx_t i = 1; i < dimensions; i++) {
			PostgresType new_pg_type;
			new_pg_type.children.push_back(std::move(child_pg_type));
			child_pg_type = std::move(new_pg_type);
			child_type = LogicalType::LIST(child_type);
		}
		auto result = LogicalType::LIST(child_type);
		postgres_type.children.push_back(std::move(child_pg_type));
		return result;
	}

	if (pgtypename == "bool") {
		return LogicalType::BOOLEAN;
	} else if (pgtypename == "int2") {
		return LogicalType::SMALLINT;
	} else if (pgtypename == "int4") {
		return LogicalType::INTEGER;
	} else if (pgtypename == "int8") {
		return LogicalType::BIGINT;
	} else if (pgtypename == "oid") { // "The oid type is currently implemented as an unsigned four-byte integer."
		return LogicalType::UINTEGER;
	} else if (pgtypename == "float4") {
		return LogicalType::FLOAT;
	} else if (pgtypename == "float8") {
		return LogicalType::DOUBLE;
	} else if (pgtypename == "numeric") {
		auto width = ((type_info.type_modifier - sizeof(int32_t)) >> 16) & 0xffff;
		auto scale = (((type_info.type_modifier - sizeof(int32_t)) & 0x7ff) ^ 1024) - 1024;
		if (type_info.type_modifier == -1 || width < 0 || scale < 0 || width > 38) {
			// fallback to double
			postgres_type.info = PostgresTypeAnnotation::NUMERIC_AS_DOUBLE;
			return LogicalType::DOUBLE;
		}
		return LogicalType::DECIMAL(width, scale);
	} else if (pgtypename == "char" || pgtypename == "bpchar") {
		postgres_type.info = PostgresTypeAnnotation::FIXED_LENGTH_CHAR;
		return LogicalType::VARCHAR;
	} else if (pgtypename == "varchar" || pgtypename == "text" || pgtypename == "json") {
		return LogicalType::VARCHAR;
	} else if (pgtypename == "jsonb") {
		postgres_type.info = PostgresTypeAnnotation::JSONB;
		return LogicalType::VARCHAR;
	} else if (pgtypename == "geometry") {
		return LogicalType::GEOMETRY();
	} else if (pgtypename == "date") {
		return LogicalType::DATE;
	} else if (pgtypename == "bytea") {
		return LogicalType::BLOB;
	} else if (pgtypename == "time") {
		return LogicalType::TIME;
	} else if (pgtypename == "timetz") {
		return LogicalType::TIME_TZ;
	} else if (pgtypename == "timestamp") {
		return LogicalType::TIMESTAMP;
	} else if (pgtypename == "timestamptz") {
		return LogicalType::TIMESTAMP_TZ;
	} else if (pgtypename == "interval") {
		return LogicalType::INTERVAL;
	} else if (pgtypename == "uuid") {
		return LogicalType::UUID;
	} else if (pgtypename == "point") {
		postgres_type.info = PostgresTypeAnnotation::GEOM_POINT;
		child_list_t<LogicalType> point_struct;
		point_struct.emplace_back(make_pair("x", LogicalType::DOUBLE));
		point_struct.emplace_back(make_pair("y", LogicalType::DOUBLE));
		return LogicalType::STRUCT(point_struct);
	} else if (pgtypename == "line") {
		postgres_type.info = PostgresTypeAnnotation::GEOM_LINE;
		return LogicalType::LIST(LogicalType::DOUBLE);
	} else if (pgtypename == "lseg") {
		postgres_type.info = PostgresTypeAnnotation::GEOM_LINE_SEGMENT;
		return LogicalType::LIST(LogicalType::DOUBLE);
	} else if (pgtypename == "box") {
		postgres_type.info = PostgresTypeAnnotation::GEOM_BOX;
		return LogicalType::LIST(LogicalType::DOUBLE);
	} else if (pgtypename == "path") {
		postgres_type.info = PostgresTypeAnnotation::GEOM_PATH;
		return LogicalType::LIST(LogicalType::DOUBLE);
	} else if (pgtypename == "polygon") {
		postgres_type.info = PostgresTypeAnnotation::GEOM_POLYGON;
		return LogicalType::LIST(LogicalType::DOUBLE);
	} else if (pgtypename == "circle") {
		postgres_type.info = PostgresTypeAnnotation::GEOM_CIRCLE;
		return LogicalType::LIST(LogicalType::DOUBLE);
	} else {
		if (!transaction) {
			// unsupported so fallback to varchar
			postgres_type.info = PostgresTypeAnnotation::CAST_TO_VARCHAR;
			return LogicalType::VARCHAR;
		}
		auto context = transaction->GetContext();
		if (!context) {
			throw InternalException("Context is destroyed!?");
		}
		auto entry = schema->GetEntry(CatalogTransaction(schema->ParentCatalog(), *context), CatalogType::TYPE_ENTRY,
		                              pgtypename);
		if (!entry) {
			// unsupported so fallback to varchar
			postgres_type.info = PostgresTypeAnnotation::CAST_TO_VARCHAR;
			return LogicalType::VARCHAR;
		}
		// custom type (e.g. composite or enum)
		auto &type_entry = entry->Cast<PostgresTypeEntry>();
		auto result_type = RemoveAlias(type_entry.user_type);
		postgres_type = type_entry.postgres_type;
		return result_type;
	}
}

LogicalType PostgresUtils::ToPostgresType(const LogicalType &input) {
	switch (input.id()) {
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::ENUM:
	case LogicalTypeId::BLOB:
	case LogicalTypeId::DATE:
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::INTERVAL:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::UUID:
	case LogicalTypeId::VARCHAR:
		return input;
	case LogicalTypeId::LIST:
		return LogicalType::LIST(ToPostgresType(ListType::GetChildType(input)));
	case LogicalTypeId::STRUCT: {
		child_list_t<LogicalType> new_types;
		for (idx_t c = 0; c < StructType::GetChildCount(input); c++) {
			auto &name = StructType::GetChildName(input, c);
			auto &type = StructType::GetChildType(input, c);
			new_types.push_back(make_pair(name, ToPostgresType(type)));
		}
		auto result = LogicalType::STRUCT(std::move(new_types));
		result.SetAlias(input.GetAlias());
		return result;
	}
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
		return LogicalType::TIMESTAMP;
	case LogicalTypeId::TINYINT:
		return LogicalType::SMALLINT;
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
		return LogicalType::BIGINT;
	case LogicalTypeId::UBIGINT:
		return LogicalType::DECIMAL(20, 0);
	case LogicalTypeId::HUGEINT:
		return LogicalType::DOUBLE;
	case LogicalTypeId::GEOMETRY:
		return LogicalType::GEOMETRY();
	default:
		return LogicalType::VARCHAR;
	}
}

PostgresType PostgresUtils::CreateEmptyPostgresType(const LogicalType &type) {
	PostgresType result;
	switch (type.id()) {
	case LogicalTypeId::STRUCT:
		for (auto &child_type : StructType::GetChildTypes(type)) {
			result.children.push_back(CreateEmptyPostgresType(child_type.second));
		}
		break;
	case LogicalTypeId::LIST:
		result.children.push_back(CreateEmptyPostgresType(ListType::GetChildType(type)));
		break;
	default:
		break;
	}
	return result;
}

bool PostgresUtils::SupportedPostgresOid(const LogicalType &input) {
	switch (input.id()) {
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BLOB:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::INTERVAL:
	case LogicalTypeId::TIME_TZ:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::BIT:
	case LogicalTypeId::UUID:
		return true;
	default:
		return false;
	}
}

string PostgresUtils::PostgresOidToName(uint32_t oid) {
	switch (oid) {
	case BOOLOID:
		return "bool";
	case INT2OID:
		return "int2";
	case INT4OID:
		return "int4";
	case INT8OID:
		return "int8";
	case FLOAT4OID:
		return "float4";
	case FLOAT8OID:
		return "float8";
	case CHAROID:
	case BPCHAROID:
		return "char";
	case TEXTOID:
	case VARCHAROID:
		return "varchar";
	case JSONOID:
		return "json";
	case BYTEAOID:
		return "bytea";
	case DATEOID:
		return "date";
	case TIMEOID:
		return "time";
	case TIMESTAMPOID:
		return "timestamp";
	case INTERVALOID:
		return "interval";
	case TIMETZOID:
		return "timetz";
	case TIMESTAMPTZOID:
		return "timestamptz";
	case BITOID:
		return "bit";
	case UUIDOID:
		return "uuid";
	case NUMERICOID:
		return "numeric";
	case JSONBOID:
		return "jsonb";
	case BOOLARRAYOID:
		return "_bool";
	case CHARARRAYOID:
	case BPCHARARRAYOID:
		return "_char";
	case INT8ARRAYOID:
		return "_int8";
	case INT2ARRAYOID:
		return "_int2";
	case INT4ARRAYOID:
		return "_int4";
	case FLOAT4ARRAYOID:
		return "_float4";
	case FLOAT8ARRAYOID:
		return "_float8";
	case TEXTARRAYOID:
	case VARCHARARRAYOID:
		return "_varchar";
	case JSONARRAYOID:
		return "_json";
	case JSONBARRAYOID:
		return "_jsonb";
	case NUMERICARRAYOID:
		return "_numeric";
	case UUIDARRAYOID:
		return "_uuid";
	case DATEARRAYOID:
		return "_date";
	case TIMEARRAYOID:
		return "_time";
	case TIMESTAMPARRAYOID:
		return "_timestamp";
	case TIMESTAMPTZARRAYOID:
		return "_timestamptz";
	case INTERVALARRAYOID:
		return "_interval";
	case TIMETZARRAYOID:
		return "_timetz";
	case BITARRAYOID:
		return "_bit";
	default:
		return "unsupported_type";
	}
}

uint32_t PostgresUtils::ToPostgresOid(const LogicalType &input) {
	switch (input.id()) {
	case LogicalTypeId::BOOLEAN:
		return BOOLOID;
	case LogicalTypeId::SMALLINT:
		return INT2OID;
	case LogicalTypeId::INTEGER:
		return INT4OID;
	case LogicalTypeId::BIGINT:
		return INT8OID;
	case LogicalTypeId::FLOAT:
		return FLOAT4OID;
	case LogicalTypeId::DOUBLE:
		return FLOAT8OID;
	case LogicalTypeId::VARCHAR:
		return VARCHAROID;
	case LogicalTypeId::BLOB:
		return BYTEAOID;
	case LogicalTypeId::DATE:
		return DATEOID;
	case LogicalTypeId::TIME:
		return TIMEOID;
	case LogicalTypeId::TIMESTAMP:
		return TIMESTAMPOID;
	case LogicalTypeId::INTERVAL:
		return INTERVALOID;
	case LogicalTypeId::TIME_TZ:
		return TIMETZOID;
	case LogicalTypeId::TIMESTAMP_TZ:
		return TIMESTAMPTZOID;
	case LogicalTypeId::BIT:
		return BITOID;
	case LogicalTypeId::UUID:
		return UUIDOID;
	case LogicalTypeId::LIST:
		return PostgresUtils::ToPostgresOid(ListType::GetChildType(input));
	default:
		throw NotImplementedException("Unsupported type for Postgres array copy: %s", input.ToString());
	}
}

PostgresVersion PostgresUtils::ExtractPostgresVersion(const string &version_str) {
	PostgresVersion result;
	idx_t pos = 0;
	if (!StringUtil::Contains(version_str, "PostgreSQL")) {
		result.type_v = PostgresInstanceType::UNKNOWN;
	}
	// scan for the first digit
	while (pos < version_str.size() && !StringUtil::CharacterIsDigit(version_str[pos])) {
		pos++;
	}
	for (idx_t version_idx = 0; version_idx < 3; version_idx++) {
		idx_t digit_start = pos;
		while (pos < version_str.size() && StringUtil::CharacterIsDigit(version_str[pos])) {
			pos++;
		}
		if (digit_start == pos) {
			// no digits
			break;
		}
		// our version is at [digit_start..pos)
		auto digit_str = version_str.substr(digit_start, pos - digit_start);
		auto digit = std::strtoll(digit_str.c_str(), 0, 10);
		switch (version_idx) {
		case 0:
			result.major_v = digit;
			break;
		case 1:
			result.minor_v = digit;
			break;
		default:
			result.patch_v = digit;
			break;
		}

		// check if the next character is a dot, if not we stop
		if (pos >= version_str.size() || version_str[pos] != '.') {
			break;
		}
		pos++;
	}
	return result;
}

string PostgresUtils::QuotePostgresIdentifier(const string &text) {
	return KeywordHelper::WriteOptionallyQuoted(text, '"', false);
}

} // namespace duckdb
