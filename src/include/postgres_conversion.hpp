//===----------------------------------------------------------------------===//
//                         DuckDB
//
// postgres_conversion.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/common/types/interval.hpp"

#include <cmath>
#include <arpa/inet.h>
// htonll is not available on Linux it seems
#ifndef ntohll
#define ntohll(x) ((((uint64_t)ntohl(x & 0xFFFFFFFF)) << 32) + ntohl(x >> 32))
#endif
#ifndef htonll
#define htonll(x) ((((uint64_t)htonl(x)) << 32) + htonl((x) >> 32))
#endif

#define POSTGRES_EPOCH_JDATE   2451545 /* == date2j(2000, 1, 1) */
#define DUCKDB_EPOCH_DATE      2440588
#define POSTGRES_MIN_DATE      -2440589
#define POSTGRES_MAX_DATE      2145042906
#define POSTGRES_DATE_INF      2147483647U
#define POSTGRES_DATE_NINF     2147483648U
#define POSTGRES_EPOCH_TS      211813488000000000ULL
#define DUCKDB_EPOCH_TS        210866803200000000ULL
/* Values from PostgreSQL's datatype/timestamp.h. The end is exclusive. */
#define POSTGRES_MIN_TIMESTAMP (-211813488000000000LL)
#define POSTGRES_END_TIMESTAMP 9223371331200000000LL
#define POSTGRES_INFINITY      9223372036854775807ULL
#define POSTGRES_NINFINITY     9223372036854775808ULL

#define NBASE      10000
#define DEC_DIGITS 4 /* decimal digits per NBASE digit */

/*
 * Interpretation of high bits.
 */

#define NUMERIC_SIGN_MASK 0xC000
#define NUMERIC_POS       0x0000
#define NUMERIC_NEG       0x4000
#define NUMERIC_SHORT     0x8000
#define NUMERIC_SPECIAL   0xC000

/*
 * Definitions for special values (NaN, positive infinity, negative infinity).
 *
 * The two bits after the NUMERIC_SPECIAL bits are 00 for NaN, 01 for positive
 * infinity, 11 for negative infinity.  (This makes the sign bit match where
 * it is in a short-format value, though we make no use of that at present.)
 * We could mask off the remaining bits before testing the active bits, but
 * currently those bits must be zeroes, so masking would just add cycles.
 */
#define NUMERIC_EXT_SIGN_MASK 0xF000 /* high bits plus NaN/Inf flag bits */
#define NUMERIC_NAN           0xC000
#define NUMERIC_PINF          0xD000
#define NUMERIC_NINF          0xF000
#define NUMERIC_INF_SIGN_MASK 0x2000

#define NUMERIC_EXT_FLAGBITS(n) ((n)->choice.n_header & NUMERIC_EXT_SIGN_MASK)
#define NUMERIC_IS_NAN(n)       ((n)->choice.n_header == NUMERIC_NAN)
#define NUMERIC_IS_PINF(n)      ((n)->choice.n_header == NUMERIC_PINF)
#define NUMERIC_IS_NINF(n)      ((n)->choice.n_header == NUMERIC_NINF)
#define NUMERIC_IS_INF(n)       (((n)->choice.n_header & ~NUMERIC_INF_SIGN_MASK) == NUMERIC_PINF)

/*
 * Short format definitions.
 */

#define NUMERIC_DSCALE_MASK            0x3FFF
#define NUMERIC_SHORT_SIGN_MASK        0x2000
#define NUMERIC_SHORT_DSCALE_MASK      0x1F80
#define NUMERIC_SHORT_DSCALE_SHIFT     7
#define NUMERIC_SHORT_DSCALE_MAX       (NUMERIC_SHORT_DSCALE_MASK >> NUMERIC_SHORT_DSCALE_SHIFT)
#define NUMERIC_SHORT_WEIGHT_SIGN_MASK 0x0040
#define NUMERIC_SHORT_WEIGHT_MASK      0x003F
#define NUMERIC_SHORT_WEIGHT_MAX       NUMERIC_SHORT_WEIGHT_MASK
#define NUMERIC_SHORT_WEIGHT_MIN       (-(NUMERIC_SHORT_WEIGHT_MASK + 1))

#define NUMERIC_SIGN(is_short, header1)                                                                                \
	(is_short ? ((header1 & NUMERIC_SHORT_SIGN_MASK) ? NUMERIC_NEG : NUMERIC_POS) : (header1 & NUMERIC_SIGN_MASK))
#define NUMERIC_DSCALE(is_short, header1)                                                                              \
	(is_short ? (header1 & NUMERIC_SHORT_DSCALE_MASK) >> NUMERIC_SHORT_DSCALE_SHIFT : (header1 & NUMERIC_DSCALE_MASK))
#define NUMERIC_WEIGHT(is_short, header1, header2)                                                                     \
	(is_short ? ((header1 & NUMERIC_SHORT_WEIGHT_SIGN_MASK ? ~NUMERIC_SHORT_WEIGHT_MASK : 0) |                         \
	             (header1 & NUMERIC_SHORT_WEIGHT_MASK))                                                                \
	          : (header2))

namespace duckdb {

struct PostgresDecimalConfig {
	uint16_t scale;
	uint16_t ndigits;
	int16_t weight;
	bool is_negative;
};

struct PostgresConversion {
	static constexpr const char *COPY_HEADER = "PGCOPY\n\377\r\n\0";
	static constexpr const idx_t COPY_HEADER_LENGTH = 11;
};

// copied from cast_helpers.cpp because windows linking issues
struct DecimalConversionInteger {
	static int64_t GetPowerOfTen(idx_t index) {
		static const int64_t POWERS_OF_TEN[] {1,
		                                      10,
		                                      100,
		                                      1000,
		                                      10000,
		                                      100000,
		                                      1000000,
		                                      10000000,
		                                      100000000,
		                                      1000000000,
		                                      10000000000,
		                                      100000000000,
		                                      1000000000000,
		                                      10000000000000,
		                                      100000000000000,
		                                      1000000000000000,
		                                      10000000000000000,
		                                      100000000000000000,
		                                      1000000000000000000};
		if (index >= 19) {
			throw InternalException("DecimalConversionInteger::GetPowerOfTen - Out of range");
		}
		return POWERS_OF_TEN[index];
	}

	template <class T>
	static T Finalize(const PostgresDecimalConfig &config, T result) {
		return result;
	}
};

struct DecimalConversionHugeint {
	static hugeint_t GetPowerOfTen(idx_t index) {
		static const hugeint_t POWERS_OF_TEN[] {
		    hugeint_t(1),
		    hugeint_t(10),
		    hugeint_t(100),
		    hugeint_t(1000),
		    hugeint_t(10000),
		    hugeint_t(100000),
		    hugeint_t(1000000),
		    hugeint_t(10000000),
		    hugeint_t(100000000),
		    hugeint_t(1000000000),
		    hugeint_t(10000000000),
		    hugeint_t(100000000000),
		    hugeint_t(1000000000000),
		    hugeint_t(10000000000000),
		    hugeint_t(100000000000000),
		    hugeint_t(1000000000000000),
		    hugeint_t(10000000000000000),
		    hugeint_t(100000000000000000),
		    hugeint_t(1000000000000000000),
		    hugeint_t(1000000000000000000) * hugeint_t(10),
		    hugeint_t(1000000000000000000) * hugeint_t(100),
		    hugeint_t(1000000000000000000) * hugeint_t(1000),
		    hugeint_t(1000000000000000000) * hugeint_t(10000),
		    hugeint_t(1000000000000000000) * hugeint_t(100000),
		    hugeint_t(1000000000000000000) * hugeint_t(1000000),
		    hugeint_t(1000000000000000000) * hugeint_t(10000000),
		    hugeint_t(1000000000000000000) * hugeint_t(100000000),
		    hugeint_t(1000000000000000000) * hugeint_t(1000000000),
		    hugeint_t(1000000000000000000) * hugeint_t(10000000000),
		    hugeint_t(1000000000000000000) * hugeint_t(100000000000),
		    hugeint_t(1000000000000000000) * hugeint_t(1000000000000),
		    hugeint_t(1000000000000000000) * hugeint_t(10000000000000),
		    hugeint_t(1000000000000000000) * hugeint_t(100000000000000),
		    hugeint_t(1000000000000000000) * hugeint_t(1000000000000000),
		    hugeint_t(1000000000000000000) * hugeint_t(10000000000000000),
		    hugeint_t(1000000000000000000) * hugeint_t(100000000000000000),
		    hugeint_t(1000000000000000000) * hugeint_t(1000000000000000000),
		    hugeint_t(1000000000000000000) * hugeint_t(1000000000000000000) * hugeint_t(10),
		    hugeint_t(1000000000000000000) * hugeint_t(1000000000000000000) * hugeint_t(100)};
		if (index >= 39) {
			throw InternalException("DecimalConversionHugeint::GetPowerOfTen - Out of range");
		}
		return POWERS_OF_TEN[index];
	}

	static hugeint_t Finalize(const PostgresDecimalConfig &config, hugeint_t result) {
		return result;
	}
};

struct DecimalConversionDouble {
	static double GetPowerOfTen(idx_t index) {
		return pow(10, double(index));
	}

	static double Finalize(const PostgresDecimalConfig &config, double result) {
		return result / GetPowerOfTen(config.scale);
	}
};

} // namespace duckdb
