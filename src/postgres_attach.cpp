#include "duckdb.hpp"

#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "postgres_filter_pushdown.hpp"
#include "postgres_scanner.hpp"
#include "postgres_result.hpp"

namespace duckdb {

struct AttachFunctionData : public TableFunctionData {
	bool finished = false;
	string source_schema = "public";
	string sink_schema = "main";
	string suffix = "";
	bool overwrite = false;
	bool filter_pushdown = false;

	string dsn = "";
};

static unique_ptr<FunctionData> AttachBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {

	auto result = make_uniq<AttachFunctionData>();
	result->dsn = input.inputs[0].GetValue<string>();

	for (auto &kv : input.named_parameters) {
		if (kv.first == "source_schema") {
			result->source_schema = StringValue::Get(kv.second);
		} else if (kv.first == "sink_schema") {
			result->sink_schema = StringValue::Get(kv.second);
		} else if (kv.first == "overwrite") {
			result->overwrite = BooleanValue::Get(kv.second);
		} else if (kv.first == "filter_pushdown") {
			result->filter_pushdown = BooleanValue::Get(kv.second);
		}
	}

	return_types.push_back(LogicalType::BOOLEAN);
	names.emplace_back("Success");
	return std::move(result);
}

static void AttachFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = (AttachFunctionData &)*data_p.bind_data;
	if (data.finished) {
		return;
	}

	auto conn = PostgresConnection::Open(data.dsn, data.dsn);
	auto dconn = Connection(context.db->GetDatabase(context));
	auto fetch_table_query = StringUtil::Format(
	    R"(
SELECT relname
FROM pg_class JOIN pg_namespace ON pg_class.relnamespace = pg_namespace.oid
JOIN pg_attribute ON pg_class.oid = pg_attribute.attrelid
WHERE relkind = 'r' AND attnum > 0 AND nspname = %s
GROUP BY relname
ORDER BY relname;
)",
	    KeywordHelper::WriteQuoted(data.source_schema));
	auto res = conn.Query(fetch_table_query);
	for (idx_t row = 0; row < PQntuples(res->res); row++) {
		auto table_name = res->GetString(row, 0);
		string query;
		if (data.overwrite) {
			query = "CREATE OR REPLACE VIEW ";
		} else {
			query = "CREATE VIEW IF NOT EXISTS ";
		}
		if (!data.sink_schema.empty()) {
			query += KeywordHelper::WriteQuoted(data.sink_schema, '"') + ".";
		}
		query += KeywordHelper::WriteQuoted(table_name, '"');
		query += " AS SELECT * FROM ";
		if (data.filter_pushdown) {
			query += "postgres_scan_pushdown";
		} else {
			query += "postgres_scan";
		}
		query += "(";
		query += KeywordHelper::WriteQuoted(data.dsn);
		query += ", ";
		query += KeywordHelper::WriteQuoted(data.source_schema);
		query += ", ";
		query += KeywordHelper::WriteQuoted(table_name);
		query += ");";
		dconn.Query(query);
	}
	res.reset();

	data.finished = true;
}

PostgresAttachFunction::PostgresAttachFunction()
    : TableFunction("postgres_attach", {LogicalType::VARCHAR}, AttachFunction, AttachBind) {
	named_parameters["overwrite"] = LogicalType::BOOLEAN;
	named_parameters["filter_pushdown"] = LogicalType::BOOLEAN;

	named_parameters["source_schema"] = LogicalType::VARCHAR;
	named_parameters["sink_schema"] = LogicalType::VARCHAR;
	named_parameters["suffix"] = LogicalType::VARCHAR;
}

} // namespace duckdb
