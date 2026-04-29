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

//! Initializes the PQsetAuthDataHook for OAuth bearer token injection.
//! The hook provides tokens from either the PGOAUTHTOKEN environment variable
//! or the pg_oauth_token DuckDB setting.
void PostgresInitOAuthHook();

//! Sets the in-memory OAuth token (used by the pg_oauth_token setting)
void PostgresSetOAuthToken(const string &token);

//! Clears the in-memory OAuth token
void PostgresClearOAuthToken();

} // namespace duckdb
