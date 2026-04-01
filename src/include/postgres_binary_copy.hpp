//===----------------------------------------------------------------------===//
//                         DuckDB
//
// postgres_binary_copy.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "postgres_utils.hpp"
#include "postgres_result.hpp"
#include "duckdb/function/copy_function.hpp"

namespace duckdb {

class PostgresBinaryCopyFunction : public CopyFunction {
public:
	PostgresBinaryCopyFunction();

	static unique_ptr<FunctionData> PostgresBinaryWriteBind(ClientContext &context, CopyFunctionBindInput &input,
	                                                        const vector<string> &names,
	                                                        const vector<LogicalType> &sql_types);

	static unique_ptr<GlobalFunctionData>
	PostgresBinaryWriteInitializeGlobal(ClientContext &context, FunctionData &bind_data, const string &file_path);
	static unique_ptr<LocalFunctionData> PostgresBinaryWriteInitializeLocal(ExecutionContext &context,
	                                                                        FunctionData &bind_data_p);
	static void PostgresBinaryWriteSink(ExecutionContext &context, FunctionData &bind_data_p,
	                                    GlobalFunctionData &gstate, LocalFunctionData &lstate, DataChunk &input);
	static void PostgresBinaryWriteCombine(ExecutionContext &context, FunctionData &bind_data,
	                                       GlobalFunctionData &gstate, LocalFunctionData &lstate);
	static void PostgresBinaryWriteFinalize(ClientContext &context, FunctionData &bind_data,
	                                        GlobalFunctionData &gstate);

	static unique_ptr<FunctionData> PostgresBinaryReadBind(ClientContext &context, CopyFromFunctionBindInput &info,
	                                                       vector<string> &expected_names,
	                                                       vector<LogicalType> &expected_types);
};

class PostgresReadBinaryFunction : public TableFunction {
public:
	PostgresReadBinaryFunction();
};

} // namespace duckdb
