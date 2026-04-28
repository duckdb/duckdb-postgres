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

	bool HasTopology() const {
		return !tservers.empty();
	}

	idx_t ReachableCount() const {
		idx_t count = 0;
		for (auto &ts : tservers) {
			if (ts.reachable) {
				count++;
			}
		}
		return count;
	}
};

} // namespace duckdb
