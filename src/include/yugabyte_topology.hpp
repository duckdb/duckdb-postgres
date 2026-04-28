//===----------------------------------------------------------------------===//
//                         DuckDB
//
// yugabyte_topology.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

struct YugabyteTserver {
	string host;
	int32_t port = 5433;
	string cloud;
	string region;
	string zone;
	string ip_address;
	bool reachable = false;
};

struct YugabyteTopology {
	vector<YugabyteTserver> tservers;
	bool direct_connect_available = false;
};

} // namespace duckdb
