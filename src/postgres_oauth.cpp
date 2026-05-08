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

//! Previous hook in the chain (if any)
static PQauthDataHook_type prev_hook = nullptr;

struct OAuthTokenState {
	char *token_copy;
};

//! Managed by SetThreadLocalOAuthTokenFromSessionOption
static thread_local std::string oauth_token;

OAuthTokenHolder::~OAuthTokenHolder() {
	if (!oauth_token.empty()) {
		SecureZero(&oauth_token[0], oauth_token.length());
		oauth_token.clear();
	}
}

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

		// Priority 1: thread-local token set by the calling thread from the 'pg_oauth_token' option
		std::string token_str = oauth_token;

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

OAuthTokenHolder SetThreadLocalOAuthTokenFromSessionOption(ClientContext &ctx) {
	Value val;
	if (ctx.TryGetCurrentSetting("pg_oauth_token", val) && !val.IsNull()) {
		std::string token = StringValue::Get(val);
		oauth_token = std::string(token.data(), token.length());
	}
	return OAuthTokenHolder();
}

} // namespace duckdb
