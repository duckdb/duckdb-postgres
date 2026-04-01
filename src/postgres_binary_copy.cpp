#include "postgres_binary_copy.hpp"
#include "postgres_binary_writer.hpp"
#include "postgres_binary_file_reader.hpp"
#include "duckdb/common/serializer/buffered_file_writer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/file_system.hpp"

namespace duckdb {

struct PostgresBinaryCopyGlobalState : public GlobalFunctionData {
	explicit PostgresBinaryCopyGlobalState(ClientContext &context) {
		copy_state.Initialize(context);
	}

	void Flush(PostgresBinaryWriter &writer) {
		file_writer->WriteData(writer.stream.GetData(), writer.stream.GetPosition());
	}

	void WriteHeader() {
		PostgresBinaryWriter writer(copy_state);
		writer.WriteHeader();
		Flush(writer);
	}

	void WriteChunk(DataChunk &chunk) {
		chunk.Flatten();
		PostgresBinaryWriter writer(copy_state);
		for (idx_t r = 0; r < chunk.size(); r++) {
			writer.BeginRow(chunk.ColumnCount());
			for (idx_t c = 0; c < chunk.ColumnCount(); c++) {
				auto &col = chunk.data[c];
				writer.WriteValue(col, r);
			}
			writer.FinishRow();
		}
		Flush(writer);
	}

	void Flush() {
		// write the footer
		PostgresBinaryWriter writer(copy_state);
		writer.WriteFooter();
		Flush(writer);
		// flush and close the file
		file_writer->Flush();
		file_writer.reset();
	}

public:
	unique_ptr<BufferedFileWriter> file_writer;
	PostgresCopyState copy_state;
};

struct PostgresBinaryWriteBindData : public TableFunctionData {};

unique_ptr<FunctionData> PostgresBinaryCopyFunction::PostgresBinaryWriteBind(ClientContext &context,
                                                                             CopyFunctionBindInput &input,
                                                                             const vector<string> &names,
                                                                             const vector<LogicalType> &sql_types) {
	return make_uniq<PostgresBinaryWriteBindData>();
}

unique_ptr<GlobalFunctionData>
PostgresBinaryCopyFunction::PostgresBinaryWriteInitializeGlobal(ClientContext &context, FunctionData &bind_data,
                                                                const string &file_path) {
	auto result = make_uniq<PostgresBinaryCopyGlobalState>(context);
	auto &fs = FileSystem::GetFileSystem(context);
	result->file_writer = make_uniq<BufferedFileWriter>(fs, file_path);
	// write the header
	result->WriteHeader();
	return std::move(result);
}

unique_ptr<LocalFunctionData>
PostgresBinaryCopyFunction::PostgresBinaryWriteInitializeLocal(ExecutionContext &context, FunctionData &bind_data_p) {
	return make_uniq<LocalFunctionData>();
}

void PostgresBinaryCopyFunction::PostgresBinaryWriteSink(ExecutionContext &context, FunctionData &bind_data_p,
                                                         GlobalFunctionData &gstate_p, LocalFunctionData &lstate,
                                                         DataChunk &input) {
	auto &gstate = gstate_p.Cast<PostgresBinaryCopyGlobalState>();
	gstate.WriteChunk(input);
}

void PostgresBinaryCopyFunction::PostgresBinaryWriteCombine(ExecutionContext &context, FunctionData &bind_data,
                                                            GlobalFunctionData &gstate, LocalFunctionData &lstate) {
}

void PostgresBinaryCopyFunction::PostgresBinaryWriteFinalize(ClientContext &context, FunctionData &bind_data,
                                                             GlobalFunctionData &gstate_p) {
	auto &gstate = gstate_p.Cast<PostgresBinaryCopyGlobalState>();
	// write the footer and close the file
	gstate.Flush();
}

struct PostgresBinaryReadBindData : public TableFunctionData {
	string file_path;
	vector<string> names;
	vector<LogicalType> types;
	vector<PostgresType> postgres_types;
	idx_t buffer_size = PostgresBinaryFileReader::DEFAULT_BUFFER_SIZE;

	unique_ptr<FunctionData> Copy() const override {
		auto copy = make_uniq<PostgresBinaryReadBindData>();
		copy->file_path = file_path;
		copy->names = names;
		copy->types = types;
		copy->postgres_types = postgres_types;
		copy->buffer_size = buffer_size;
		return std::move(copy);
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<PostgresBinaryReadBindData>();
		return file_path == other.file_path && names == other.names;
	}
};

struct PostgresBinaryReadGlobalState : public GlobalTableFunctionState {
	unique_ptr<PostgresBinaryFileReader> reader;
	bool finished = false;
};

unique_ptr<FunctionData> PostgresBinaryCopyFunction::PostgresBinaryReadBind(ClientContext &context,
                                                                            CopyFromFunctionBindInput &info,
                                                                            vector<string> &expected_names,
                                                                            vector<LogicalType> &expected_types) {
	auto result = make_uniq<PostgresBinaryReadBindData>();
	result->file_path = info.info.file_path;
	result->names = expected_names;
	result->types = expected_types;
	for (auto &type : expected_types) {
		result->postgres_types.push_back(PostgresUtils::CreateEmptyPostgresType(type));
	}
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> PostgresBinaryReadInitGlobal(ClientContext &context,
                                                                         TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<PostgresBinaryReadBindData>();
	auto result = make_uniq<PostgresBinaryReadGlobalState>();
	result->reader = make_uniq<PostgresBinaryFileReader>(context, bind_data.file_path, bind_data.types,
	                                                     bind_data.postgres_types, bind_data.buffer_size);
	return std::move(result);
}

static void PostgresBinaryReadScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &gstate = data.global_state->Cast<PostgresBinaryReadGlobalState>();
	if (gstate.finished) {
		return;
	}
	if (!gstate.reader->ReadChunk(output)) {
		gstate.finished = true;
	}
}

static unique_ptr<FunctionData> ReadPostgresBinaryBind(ClientContext &context, TableFunctionBindInput &input,
                                                       vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<PostgresBinaryReadBindData>();
	result->file_path = input.inputs[0].GetValue<string>();

	if (!input.named_parameters.count("columns")) {
		throw BinderException("read_postgres_binary requires a 'columns' parameter, "
		                      "e.g. columns={col1: 'INTEGER', col2: 'VARCHAR'}");
	}
	auto &columns = input.named_parameters.at("columns");
	auto &column_map = StructValue::GetChildren(columns);
	auto &struct_type = columns.type();
	for (idx_t i = 0; i < column_map.size(); i++) {
		auto &col_name = StructType::GetChildName(struct_type, i);
		auto col_type_str = column_map[i].GetValue<string>();
		auto col_type = TransformStringToLogicalType(col_type_str, context);

		names.push_back(col_name);
		return_types.push_back(col_type);
		result->postgres_types.push_back(PostgresUtils::CreateEmptyPostgresType(col_type));
	}

	result->names = names;
	result->types = return_types;

	if (input.named_parameters.count("buffer_size")) {
		result->buffer_size = input.named_parameters.at("buffer_size").GetValue<uint64_t>();
	}

	return std::move(result);
}

static void PostgresBinaryReadSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
                                        const TableFunction &function) {
	auto &bind_data = bind_data_p->Cast<PostgresBinaryReadBindData>();
	serializer.WriteProperty(100, "file_path", bind_data.file_path);
	serializer.WriteProperty(101, "names", bind_data.names);
	serializer.WriteProperty(102, "types", bind_data.types);
	serializer.WriteProperty(103, "buffer_size", bind_data.buffer_size);
}

static unique_ptr<FunctionData> PostgresBinaryReadDeserialize(Deserializer &deserializer, TableFunction &function) {
	auto result = make_uniq<PostgresBinaryReadBindData>();
	deserializer.ReadProperty(100, "file_path", result->file_path);
	deserializer.ReadProperty(101, "names", result->names);
	deserializer.ReadProperty(102, "types", result->types);
	deserializer.ReadProperty(103, "buffer_size", result->buffer_size);
	for (auto &type : result->types) {
		result->postgres_types.push_back(PostgresUtils::CreateEmptyPostgresType(type));
	}
	return std::move(result);
}

PostgresBinaryCopyFunction::PostgresBinaryCopyFunction() : CopyFunction("postgres_binary") {
	copy_to_bind = PostgresBinaryWriteBind;
	copy_to_initialize_global = PostgresBinaryWriteInitializeGlobal;
	copy_to_initialize_local = PostgresBinaryWriteInitializeLocal;
	copy_to_sink = PostgresBinaryWriteSink;
	copy_to_combine = PostgresBinaryWriteCombine;
	copy_to_finalize = PostgresBinaryWriteFinalize;

	copy_from_bind = PostgresBinaryReadBind;
	copy_from_function = PostgresReadBinaryFunction();
}

PostgresReadBinaryFunction::PostgresReadBinaryFunction()
    : TableFunction("read_postgres_binary", {LogicalType::VARCHAR}, PostgresBinaryReadScan, ReadPostgresBinaryBind,
                    PostgresBinaryReadInitGlobal) {
	named_parameters["columns"] = LogicalType::ANY;
	named_parameters["buffer_size"] = LogicalType::UBIGINT;
	serialize = PostgresBinaryReadSerialize;
	deserialize = PostgresBinaryReadDeserialize;
}

} // namespace duckdb
