//===----------------------------------------------------------------------===//
//                         DuckDB
//
// postgres_oauth.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {

struct OAuthTokenHolder {
	~OAuthTokenHolder();
};

//! Initializes the PQsetAuthDataHook for OAuth bearer token injection.
//! The hook provides tokens from either the PGOAUTHTOKEN environment variable
//! or the pg_oauth_token DuckDB setting.
void PostgresInitOAuthHook();

//! Gets the 'oauth_token' value from the 'pg_oauth_token option',
//! sets it to a thread-local var to be available to PG auth hook;
//! should be called at all call sites that are about to open the new connection.
OAuthTokenHolder SetThreadLocalOAuthTokenFromSessionOption(ClientContext &ctx);

} // namespace duckdb
