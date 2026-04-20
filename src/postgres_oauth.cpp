#include "postgres_oauth.hpp"

#include <cstdlib>
#include <cstring>
#include <mutex>
#include <string>

#ifdef _WIN32
#include <windows.h>
#define SecureZero(ptr, len) SecureZeroMemory(ptr, len)
#else
#define SecureZero(ptr, len) explicit_bzero(ptr, len)
#endif

extern "C" {
#include "libpq-fe.h"
}

namespace duckdb {

//! Global mutex protecting the token string
static std::mutex oauth_token_mutex;
//! The in-memory token set via the DuckDB setting
static std::string oauth_token_value;

//! Previous hook in the chain (if any)
static PQauthDataHook_type prev_hook = nullptr;

struct OAuthTokenState {
	char *token_copy;
};

static void OAuthTokenCleanup(PGconn *conn, PGoauthBearerRequest *request) {
	auto *ts = static_cast<OAuthTokenState *>(request->user);
	if (ts) {
		if (ts->token_copy) {
			SecureZero(ts->token_copy, strlen(ts->token_copy));
			free(ts->token_copy);
		}
		free(ts);
		request->user = nullptr;
	}
	request->token = nullptr;
}

static int OAuthBearerTokenHook(PGauthData type, PGconn *conn, void *data) {
	if (type == PQAUTHDATA_OAUTH_BEARER_TOKEN) {
		const char *token = nullptr;

		// Priority 1: DuckDB setting (more secure, in-process memory only)
		std::string token_str;
		{
			std::lock_guard<std::mutex> lock(oauth_token_mutex);
			token_str = oauth_token_value;
		}

		// Priority 2: Environment variable PGOAUTHTOKEN
		if (token_str.empty()) {
			const char *env_token = std::getenv("PGOAUTHTOKEN");
			if (env_token && env_token[0] != '\0') {
				token_str = env_token;
			}
		}

		if (!token_str.empty()) {
			auto *request = static_cast<PGoauthBearerRequest *>(data);

			auto *ts = static_cast<OAuthTokenState *>(malloc(sizeof(OAuthTokenState)));
			if (!ts) {
				return -1;
			}

			ts->token_copy = strdup(token_str.c_str());
			if (!ts->token_copy) {
				free(ts);
				return -1;
			}

			request->token = ts->token_copy;
			request->cleanup = OAuthTokenCleanup;
			request->user = ts;
			request->async = nullptr;
			return 1;
		}
	}

	// Fall through to previous hook
	if (prev_hook) {
		return prev_hook(type, conn, data);
	}
	return 0;
}

void PostgresInitOAuthHook() {
	prev_hook = PQgetAuthDataHook();
	PQsetAuthDataHook(OAuthBearerTokenHook);
}

void PostgresSetOAuthToken(const string &token) {
	std::lock_guard<std::mutex> lock(oauth_token_mutex);
	if (!oauth_token_value.empty()) {
		SecureZero(&oauth_token_value[0], oauth_token_value.size());
	}
	oauth_token_value = token;
}

void PostgresClearOAuthToken() {
	std::lock_guard<std::mutex> lock(oauth_token_mutex);
	if (!oauth_token_value.empty()) {
		SecureZero(&oauth_token_value[0], oauth_token_value.size());
	}
	oauth_token_value.clear();
}

} // namespace duckdb
