#include "duckdb.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/config.hpp"

#include "duckdb/main/settings.hpp"
#include "postgres_storage.hpp"
#include "storage/postgres_catalog.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "storage/postgres_transaction_manager.hpp"
#include "utf8proc_wrapper.hpp"

namespace duckdb {

static vector<string> ExtractSchemas(Value &value) {
	if (value.IsNull()) {
		throw BinderException("Value for \"SCHEMA\" option must not be null");
	}
	switch (value.type().id()) {
	case LogicalTypeId::VARCHAR: {
		vector<string> res;
		const string &name = StringValue::Get(value);
		if (name.empty()) {
			throw BinderException("Value \"SCHEMA\" option must be not empty");
		}
		res.push_back(name);
		return res;
	}
	case LogicalTypeId::LIST: {
		if (ListType::GetChildType(value.type()).id() != LogicalTypeId::VARCHAR) {
			throw BinderException(
			    "Value for \"SCHEMA\" option must be either \"VARCHAR\" or \"VARCHAR[]\", was: \"%s\"",
			    value.type().ToString());
		}
		vector<string> res;
		for (const Value &en : ListValue::GetChildren(value)) {
			if (en.IsNull()) {
				throw BinderException("Values for \"SCHEMA\" option must not be null");
			}
			const string &name = StringValue::Get(en);
			if (name.empty()) {
				throw BinderException("Values \"SCHEMA\" option must be not empty");
			}
			res.push_back(name);
		}
		return res;
	}
	default:
		throw BinderException("Value for `SCHEMA` option must be either \"VARCHAR\" or \"VARCHAR[]\", was: \"%s\"",
		                      value.type().ToString());
	}
}

//! The connect display is rendered verbatim into the shell prompt, so reject anything that could take over the
//! terminal or trip up the code that renders it
static string ExtractConnectDisplay(Value &value) {
	if (value.IsNull()) {
		throw BinderException("Value for \"CONNECT_DISPLAY\" option must not be null");
	}
	// generous upper bound - this is only here to keep pathological input from making the prompt unusable,
	// every realistic endpoint is far below it
	const idx_t max_width = 1024;
	auto display = value.ToString();
	// this has to happen before anything below inspects the string - RenderWidth walks UTF-8 sequences and
	// relies on them being well formed
	if (!Utf8Proc::IsValid(display.c_str(), display.size())) {
		throw BinderException("Value for \"CONNECT_DISPLAY\" option must be valid UTF-8");
	}
	for (idx_t i = 0; i < display.size(); i++) {
		auto c = static_cast<unsigned char>(display[i]);
		// C0 control characters and DEL
		bool is_control = c < 0x20 || c == 0x7f;
		// C1 control characters - these are encoded as 0xC2 0x80 through 0xC2 0x9F in UTF-8
		if (c == 0xc2 && i + 1 < display.size()) {
			auto next = static_cast<unsigned char>(display[i + 1]);
			is_control = is_control || (next >= 0x80 && next <= 0x9f);
		}
		if (is_control) {
			throw BinderException("Value for \"CONNECT_DISPLAY\" option must not contain control characters - the "
			                      "value is rendered directly into the shell prompt");
		}
	}
	auto width = Utf8Proc::RenderWidth(display);
	if (width > max_width) {
		throw BinderException("Value for \"CONNECT_DISPLAY\" option must be at most %d columns wide, was %d", max_width,
		                      idx_t(width));
	}
	return display;
}

static PostgresTextProtocolMode ExtractTextProtocolMode(Value &value) {
	if (value.IsNull()) {
		throw BinderException("Value for \"USE_TEXT_PROTOCOL\" option must not be null");
	}
	if (value.type().id() == LogicalTypeId::BOOLEAN) {
		return BooleanValue::Get(value) ? PostgresTextProtocolMode::TEXT : PostgresTextProtocolMode::BINARY;
	}
	auto param = value.ToString();
	auto lparam = StringUtil::Lower(param);
	if (lparam == "auto") {
		return PostgresTextProtocolMode::AUTO;
	}
	if (lparam == "true") {
		return PostgresTextProtocolMode::TEXT;
	}
	if (lparam == "false") {
		return PostgresTextProtocolMode::BINARY;
	}
	throw BinderException("Invalid value \"%s\" for use_text_protocol, expected TRUE, FALSE or AUTO", param);
}

static unique_ptr<Catalog> PostgresAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                          AttachedDatabase &db, const string &name, AttachInfo &info,
                                          AttachOptions &attach_options) {
	auto &config = DBConfig::GetConfig(context);
	if (!Settings::Get<EnableExternalAccessSetting>(context)) {
		throw PermissionException("Attaching Postgres databases is disabled through configuration");
	}
	string attach_path = info.path;

	string secret_name;
	vector<string> schemas_to_load;
	PostgresIsolationLevel isolation_level = PostgresIsolationLevel::REPEATABLE_READ;
	PostgresTextProtocolMode text_protocol_mode = PostgresTextProtocolMode::AUTO;
	string secret_storage_table_name;
	bool secret_storage_table_specified_explicitly = false;
	string connect_display;
	for (auto &entry : attach_options.options) {
		auto lower_name = StringUtil::Lower(entry.first);
		if (lower_name == "secret") {
			secret_name = entry.second.ToString();
		} else if (lower_name == "schema") {
			schemas_to_load = ExtractSchemas(entry.second);
		} else if (lower_name == "isolation_level") {
			auto param = entry.second.ToString();
			auto lparam = StringUtil::Lower(param);
			if (lparam == "read committed") {
				isolation_level = PostgresIsolationLevel::READ_COMMITTED;
			} else if (lparam == "repeatable read") {
				isolation_level = PostgresIsolationLevel::REPEATABLE_READ;
			} else if (lparam == "serializable") {
				isolation_level = PostgresIsolationLevel::SERIALIZABLE;
			} else {
				throw BinderException("Invalid value \"%s\" for isolation_level, expected READ COMMITTED, "
				                      "REPEATABLE READ or SERIALIZABLE",
				                      param);
			}
		} else if (lower_name == "use_text_protocol") {
			text_protocol_mode = ExtractTextProtocolMode(entry.second);
		} else if (lower_name == "secret_storage_table") {
			secret_storage_table_name = entry.second.ToString();
			secret_storage_table_specified_explicitly = true;
		} else if (lower_name == "connect_display") {
			connect_display = ExtractConnectDisplay(entry.second);
		} else {
			throw BinderException("Unrecognized option for Postgres attach: %s", entry.first);
		}
	}
	SecretStorageTable secret_storage_table(std::move(secret_storage_table_name),
	                                        secret_storage_table_specified_explicitly);
	return make_uniq<PostgresCatalog>(context, db, std::move(attach_path), attach_options.access_mode,
	                                  std::move(schemas_to_load), isolation_level, secret_name,
	                                  std::move(secret_storage_table), text_protocol_mode, std::move(connect_display));
}

static unique_ptr<TransactionManager> PostgresCreateTransactionManager(optional_ptr<StorageExtensionInfo> storage_info,
                                                                       AttachedDatabase &db, Catalog &catalog) {
	auto &postgres_catalog = catalog.Cast<PostgresCatalog>();
	return make_uniq<PostgresTransactionManager>(db, postgres_catalog);
}

PostgresStorageExtension::PostgresStorageExtension() {
	attach = PostgresAttach;
	create_transaction_manager = PostgresCreateTransactionManager;
}

} // namespace duckdb
