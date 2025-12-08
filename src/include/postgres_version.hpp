//===----------------------------------------------------------------------===//
//                         DuckDB
//
// postgres_version.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

enum class PostgresInstanceType { UNKNOWN, POSTGRES, AURORA, REDSHIFT };

struct PostgresVersion {
	PostgresVersion() {
	}
	PostgresVersion(idx_t major_v, idx_t minor_v, idx_t patch_v = 0)
	    : major_v(major_v), minor_v(minor_v), patch_v(patch_v) {
	}

	idx_t major_v = 0;
	idx_t minor_v = 0;
	idx_t patch_v = 0;
	PostgresInstanceType type_v = PostgresInstanceType::POSTGRES;

	inline bool operator<(const PostgresVersion &rhs) const {
		if (major_v < rhs.major_v) {
			return true;
		}
		if (major_v > rhs.major_v) {
			return false;
		}
		if (minor_v < rhs.minor_v) {
			return true;
		}
		if (minor_v > rhs.minor_v) {
			return false;
		}
		return patch_v < rhs.patch_v;
	};
	inline bool operator<=(const PostgresVersion &rhs) const {
		return !(rhs < *this);
	};
	inline bool operator>(const PostgresVersion &rhs) const {
		return rhs < *this;
	};
	inline bool operator>=(const PostgresVersion &rhs) const {
		return !(*this < rhs);
	};
};

} // namespace duckdb
